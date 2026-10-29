/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "file_sql_jsonl.h"

#include <algorithm>
#include <memory>
#include <set>
#include <unordered_set>

namespace oceanbase {
namespace sql {
namespace filesql {

namespace {

struct JsonMember {
  std::string key;
  InferredToken token;
};

bool is_json_ws(char c) { return c == ' ' || c == '\t' || c == '\r' || c == '\n'; }

bool is_digit(char c) { return c >= '0' && c <= '9'; }

std::string trim_json_ws(const std::string &text) {
  size_t begin = 0;
  size_t end = text.size();
  while (begin < end && is_json_ws(text[begin])) {
    ++begin;
  }
  while (end > begin && is_json_ws(text[end - 1])) {
    --end;
  }
  return text.substr(begin, end - begin);
}

std::string line_error(int64_t line, const std::string &detail) {
  return "line " + std::to_string(line) + ": " + detail;
}

void append_utf8(uint32_t code_point, std::string &out) {
  if (code_point <= 0x7F) {
    out.push_back(static_cast<char>(code_point));
  } else if (code_point <= 0x7FF) {
    out.push_back(static_cast<char>(0xC0 | (code_point >> 6)));
    out.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
  } else if (code_point <= 0xFFFF) {
    out.push_back(static_cast<char>(0xE0 | (code_point >> 12)));
    out.push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
    out.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
  } else {
    out.push_back(static_cast<char>(0xF0 | (code_point >> 18)));
    out.push_back(static_cast<char>(0x80 | ((code_point >> 12) & 0x3F)));
    out.push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
    out.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
  }
}

// Recursive-descent parser for one flat JSON object.  Only the subset needed by
// JSONL is implemented; anything the SQL layer cannot represent (nested
// objects/arrays) is a hard error so that schema inference never silently
// flattens or drops data.
class FlatJsonParser {
public:
  FlatJsonParser(const std::string &text, int64_t line) : text_(text), line_(line) {}

  bool parse(std::vector<JsonMember> &members, std::string &err) {
    skip_ws();
    if (pos_ >= text_.size() || text_[pos_] != '{') {
      return fail("expected a flat JSON object at the start of the line", err);
    }
    ++pos_;
    skip_ws();
    std::set<std::string> seen_keys;
    if (pos_ < text_.size() && text_[pos_] == '}') {
      ++pos_;
    } else {
      for (;;) {
        skip_ws();
        if (pos_ >= text_.size() || text_[pos_] != '"') {
          return fail("expected a quoted object key", err);
        }
        std::string key;
        if (!parse_string(key, err)) {
          return false;
        }
        if (!seen_keys.insert(key).second) {
          return fail("duplicate key \"" + key + "\"", err);
        }
        skip_ws();
        if (pos_ >= text_.size() || text_[pos_] != ':') {
          return fail("expected ':' after key \"" + key + "\"", err);
        }
        ++pos_;
        skip_ws();
        InferredToken token;
        if (!parse_value(key, token, err)) {
          return false;
        }
        JsonMember member;
        member.key = key;
        member.token = token;
        members.push_back(member);
        skip_ws();
        if (pos_ >= text_.size()) {
          return fail("unexpected end of line inside the JSON object", err);
        }
        if (text_[pos_] == ',') {
          ++pos_;
          continue;
        }
        if (text_[pos_] == '}') {
          ++pos_;
          break;
        }
        return fail("expected ',' or '}' after the value of column \"" + key + "\"", err);
      }
    }
    skip_ws();
    if (pos_ != text_.size()) {
      return fail("trailing characters after the JSON object", err);
    }
    return true;
  }

private:
  bool fail(const std::string &detail, std::string &err) {
    err = line_error(line_, detail);
    return false;
  }

  void skip_ws() {
    while (pos_ < text_.size() && is_json_ws(text_[pos_])) {
      ++pos_;
    }
  }

  bool parse_hex4(uint32_t &value, std::string &err) {
    if (pos_ + 4 > text_.size()) {
      return fail("truncated \\u escape in JSON string", err);
    }
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
      const char h = text_[pos_ + i];
      int d = 0;
      if (h >= '0' && h <= '9') {
        d = h - '0';
      } else if (h >= 'a' && h <= 'f') {
        d = h - 'a' + 10;
      } else if (h >= 'A' && h <= 'F') {
        d = h - 'A' + 10;
      } else {
        return fail("invalid \\u escape in JSON string", err);
      }
      v = v * 16 + static_cast<uint32_t>(d);
    }
    pos_ += 4;
    value = v;
    return true;
  }

  bool parse_unicode_escape(uint32_t &code_point, std::string &err) {
    uint32_t high = 0;
    if (!parse_hex4(high, err)) {
      return false;
    }
    if (high >= 0xD800 && high <= 0xDBFF) {
      // UTF-16 surrogate pair: the low surrogate must follow immediately.
      if (pos_ + 2 > text_.size() || text_[pos_] != '\\' || text_[pos_ + 1] != 'u') {
        return fail("lone UTF-16 high surrogate in JSON string", err);
      }
      pos_ += 2;
      uint32_t low = 0;
      if (!parse_hex4(low, err)) {
        return false;
      }
      if (low < 0xDC00 || low > 0xDFFF) {
        return fail("invalid UTF-16 surrogate pair in JSON string", err);
      }
      code_point = 0x10000u + ((high - 0xD800u) << 10) + (low - 0xDC00u);
      return true;
    }
    if (high >= 0xDC00 && high <= 0xDFFF) {
      return fail("lone UTF-16 low surrogate in JSON string", err);
    }
    code_point = high;
    return true;
  }

  bool parse_string(std::string &out, std::string &err) {
    out.clear();
    if (pos_ >= text_.size() || text_[pos_] != '"') {
      return fail("expected a JSON string", err);
    }
    ++pos_;
    while (pos_ < text_.size()) {
      const unsigned char c = static_cast<unsigned char>(text_[pos_]);
      if (c == '"') {
        ++pos_;
        return true;
      }
      if (c == '\\') {
        ++pos_;
        if (pos_ >= text_.size()) {
          return fail("unterminated escape sequence in JSON string", err);
        }
        const char escape = text_[pos_++];
        switch (escape) {
          case '"':
            out.push_back('"');
            break;
          case '\\':
            out.push_back('\\');
            break;
          case '/':
            out.push_back('/');
            break;
          case 'b':
            out.push_back('\b');
            break;
          case 'f':
            out.push_back('\f');
            break;
          case 'n':
            out.push_back('\n');
            break;
          case 'r':
            out.push_back('\r');
            break;
          case 't':
            out.push_back('\t');
            break;
          case 'u': {
            uint32_t code_point = 0;
            if (!parse_unicode_escape(code_point, err)) {
              return false;
            }
            append_utf8(code_point, out);
            break;
          }
          default:
            return fail(std::string("invalid escape sequence \\") + escape + " in JSON string",
                        err);
        }
        continue;
      }
      if (c < 0x20) {
        return fail("raw control character in JSON string (use a \\u escape)", err);
      }
      out.push_back(text_[pos_++]);
    }
    return fail("unterminated JSON string", err);
  }

  bool expect_literal(const char *literal, size_t length, std::string &err) {
    if (pos_ + length > text_.size() || text_.compare(pos_, length, literal) != 0) {
      return fail(std::string("invalid literal, expected \"") + literal + "\"", err);
    }
    pos_ += length;
    return true;
  }

  bool parse_number(InferredToken &token, std::string &err) {
    const size_t start = pos_;
    if (pos_ < text_.size() && text_[pos_] == '-') {
      ++pos_;
    }
    if (pos_ >= text_.size()) {
      return fail("truncated number", err);
    }
    if (text_[pos_] == '0') {
      ++pos_;
    } else if (text_[pos_] >= '1' && text_[pos_] <= '9') {
      while (pos_ < text_.size() && is_digit(text_[pos_])) {
        ++pos_;
      }
    } else {
      return fail("invalid JSON number", err);
    }
    bool is_float = false;
    if (pos_ < text_.size() && text_[pos_] == '.') {
      is_float = true;
      ++pos_;
      if (pos_ >= text_.size() || !is_digit(text_[pos_])) {
        return fail("invalid JSON number: missing fraction digits", err);
      }
      while (pos_ < text_.size() && is_digit(text_[pos_])) {
        ++pos_;
      }
    }
    if (pos_ < text_.size() && (text_[pos_] == 'e' || text_[pos_] == 'E')) {
      is_float = true;
      ++pos_;
      if (pos_ < text_.size() && (text_[pos_] == '+' || text_[pos_] == '-')) {
        ++pos_;
      }
      if (pos_ >= text_.size() || !is_digit(text_[pos_])) {
        return fail("invalid JSON number: missing exponent digits", err);
      }
      while (pos_ < text_.size() && is_digit(text_[pos_])) {
        ++pos_;
      }
    }
    token = InferredToken();
    token.raw = text_.substr(start, pos_ - start);
    token.kind = TokenKind::TEXT;
    if (!is_float) {
      int64_t int_value = 0;
      if (filesql_is_integer_text(token.raw, int_value)) {
        token.kind = TokenKind::INTEGER;
        token.i = int_value;
        return true;
      }
      // int64 overflow: fall through to DOUBLE (numeric boundary handling).
    }
    double double_value = 0.0;
    if (filesql_is_double_text(token.raw, double_value)) {
      token.kind = TokenKind::DOUBLE;
      token.d = double_value;
      return true;
    }
    // Syntactically valid JSON number outside the finite double range (1e999):
    // keep the literal text and let the column widen to VARCHAR.
    return true;
  }

  bool parse_value(const std::string &key, InferredToken &token, std::string &err) {
    if (pos_ >= text_.size()) {
      return fail("missing value for column \"" + key + "\"", err);
    }
    const char c = text_[pos_];
    if (c == '{' || c == '[') {
      return fail("nested object/array value is not supported (column \"" + key + "\")", err);
    }
    if (c == '"') {
      std::string value;
      if (!parse_string(value, err)) {
        return false;
      }
      token = classify_json_string(value);
      return true;
    }
    if (c == 't') {
      if (!expect_literal("true", 4, err)) {
        return false;
      }
      token = InferredToken();
      token.kind = TokenKind::BOOLEAN;
      token.i = 1;
      token.raw = "true";
      return true;
    }
    if (c == 'f') {
      if (!expect_literal("false", 5, err)) {
        return false;
      }
      token = InferredToken();
      token.kind = TokenKind::BOOLEAN;
      token.i = 0;
      token.raw = "false";
      return true;
    }
    if (c == 'n') {
      if (!expect_literal("null", 4, err)) {
        return false;
      }
      token = InferredToken();  // NULL_VALUE
      token.raw = "null";
      return true;
    }
    if (c == '-' || is_digit(c)) {
      return parse_number(token, err);
    }
    return fail(std::string("unexpected character '") + c + "' as the value of column \"" + key +
                    "\"",
                err);
  }

  const std::string &text_;
  size_t pos_ = 0;
  int64_t line_ = 1;
};

bool parse_flat_object_line(const std::string &line, int64_t line_no,
                            std::vector<JsonMember> &members, std::string &err) {
  FlatJsonParser parser(line, line_no);
  return parser.parse(members, err);
}

}  // namespace

// ---------------------------------------------------------------------------
// JsonlFileReader
// ---------------------------------------------------------------------------

bool JsonlFileReader::validate_options(const FileOptions &opts, std::string &err) {
  if (opts.sample_size != -1) {
    err = "sample_size other than -1 is not supported";
    return false;
  }
  if (!opts.newline_delimited) {
    // The struct only carries a boolean today; anything but newline-delimited
    // (array / auto) is outside the task specification.
    err = "only format='newline_delimited' is supported";
    return false;
  }
  return true;
}

ReadStatus JsonlFileReader::scan_schema(std::string &err) {
  schema_.clear();
  column_index_.clear();
  empty_ = true;

  struct ColumnState {
    std::string name;
    int64_t first_line = 0;
    int64_t first_pos = 0;
    TypeInferencer infer;
  };
  std::vector<ColumnState> columns;
  std::unordered_map<std::string, size_t> index;
  int64_t line_no = 0;
  int64_t data_line_no = 0;
  std::string line;
  std::vector<JsonMember> members;

  while (read_physical_line(in_, line) == 1) {
    ++line_no;
    if (trim_json_ws(line).empty()) {
      continue;
    }
    members.clear();
    if (!parse_flat_object_line(line, line_no, members, err)) {
      // Malformed line: keep the schema derived from the earlier lines.  The
      // streaming pass re-parses the file and reports the same error, with the
      // line number, from next_row().
      break;
    }
    ++data_line_no;
    std::unordered_set<std::string> present;
    present.reserve(members.size() * 2 + 1);
    for (size_t i = 0; i < members.size(); ++i) {
      present.insert(members[i].key);
    }
    // Columns known from earlier lines that are absent here are NULL.
    for (size_t i = 0; i < columns.size(); ++i) {
      if (present.find(columns[i].name) == present.end()) {
        columns[i].infer.add_null();
      }
    }
    for (size_t pos = 0; pos < members.size(); ++pos) {
      std::unordered_map<std::string, size_t>::iterator it = index.find(members[pos].key);
      if (it == index.end()) {
        ColumnState state;
        state.name = members[pos].key;
        state.first_line = line_no;
        state.first_pos = static_cast<int64_t>(pos);
        if (data_line_no > 1) {
          // The key did not exist in any earlier record, so the earlier rows
          // contribute NULL values for this column.
          state.infer.add_null();
        }
        index[state.name] = columns.size();
        columns.push_back(state);
        it = index.find(members[pos].key);
      }
      columns[it->second].infer.add(members[pos].token);
    }
  }
  empty_ = (data_line_no == 0);

  // Deterministic column order: first appearance (line, then position), with
  // the key name only as a final (never needed) tie-break.
  std::sort(columns.begin(), columns.end(), [](const ColumnState &left, const ColumnState &right) {
    if (left.first_line != right.first_line) {
      return left.first_line < right.first_line;
    }
    if (left.first_pos != right.first_pos) {
      return left.first_pos < right.first_pos;
    }
    return left.name < right.name;
  });

  for (size_t i = 0; i < columns.size(); ++i) {
    ColumnSchema column;
    column.name = columns[i].name;
    column.type = columns[i].infer.type();
    column.nullable = columns[i].infer.nullable();
    schema_.push_back(column);
    column_index_[column.name] = i;
  }
  return ReadStatus::OK;
}

ReadStatus JsonlFileReader::reopen_for_reading(std::string &err) {
  in_.close();
  if (!in_.open(path_, err)) {
    return ReadStatus::ERROR;
  }
  line_no_ = 0;
  return ReadStatus::OK;
}

ReadStatus JsonlFileReader::open(const std::string &path, const FileOptions &opts) {
  close();
  path_ = path;
  opts_ = opts;
  std::string err;
  if (!validate_options(opts_, err)) {
    last_error_ = err;
    return ReadStatus::ERROR;
  }
  if (!in_.open(path_, err)) {
    last_error_ = err;
    return ReadStatus::ERROR;
  }
  if (scan_schema(err) != ReadStatus::OK) {
    last_error_ = err;
    in_.close();
    return ReadStatus::ERROR;
  }
  if (reopen_for_reading(err) != ReadStatus::OK) {
    last_error_ = err;
    in_.close();
    return ReadStatus::ERROR;
  }
  opened_ = true;
  return ReadStatus::OK;
}

ReadStatus JsonlFileReader::next_row(std::vector<Cell> &row, std::string &err) {
  err.clear();
  if (!opened_) {
    err = "JSONL reader is not open";
    return ReadStatus::ERROR;
  }
  for (;;) {
    std::string line;
    if (read_physical_line(in_, line) == 0) {
      row.clear();
      return ReadStatus::END;
    }
    ++line_no_;
    if (trim_json_ws(line).empty()) {
      continue;
    }
    std::vector<JsonMember> members;
    if (!parse_flat_object_line(line, line_no_, members, err)) {
      return ReadStatus::ERROR;
    }
    row.assign(schema_.size(), Cell());
    for (size_t i = 0; i < members.size(); ++i) {
      std::unordered_map<std::string, size_t>::const_iterator it =
          column_index_.find(members[i].key);
      if (it == column_index_.end()) {
        err = "line " + std::to_string(line_no_) + ": internal error: column \"" +
              members[i].key + "\" is missing from the inferred schema";
        return ReadStatus::ERROR;
      }
      std::string cell_err;
      if (!token_to_cell(members[i].token, schema_[it->second].type, row[it->second], cell_err)) {
        err = "line " + std::to_string(line_no_) + ": column \"" + members[i].key + "\": " +
              cell_err;
        return ReadStatus::ERROR;
      }
    }
    return ReadStatus::OK;
  }
}

void JsonlFileReader::close() {
  in_.close();
  schema_.clear();
  column_index_.clear();
  opened_ = false;
  empty_ = true;
  line_no_ = 0;
}

std::unique_ptr<IFileReader> create_jsonl_reader() {
  return std::unique_ptr<IFileReader>(new JsonlFileReader());
}

}  // namespace filesql
}  // namespace sql
}  // namespace oceanbase
