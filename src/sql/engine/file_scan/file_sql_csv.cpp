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

#include "file_sql_csv.h"

#include <memory>
#include <set>

namespace oceanbase {
namespace sql {
namespace filesql {

namespace {

bool is_blank_char(char c) { return c == ' ' || c == '\t'; }

std::string trim_blank(const std::string &text) {
  size_t begin = 0;
  size_t end = text.size();
  while (begin < end && is_blank_char(text[begin])) {
    ++begin;
  }
  while (end > begin && is_blank_char(text[end - 1])) {
    --end;
  }
  return text.substr(begin, end - begin);
}

std::string char_snippet(char c) {
  if (c >= 0x20 && c < 0x7F) {
    return std::string(1, c);
  }
  // Non printable: show it as a small integer so the message stays ASCII.
  return "<" + std::to_string(static_cast<int>(static_cast<unsigned char>(c))) + ">";
}

}  // namespace

// ---------------------------------------------------------------------------
// CsvRecordParser
// ---------------------------------------------------------------------------

bool CsvRecordParser::is_blank_record(const std::vector<std::string> &fields,
                                      const std::vector<char> &quoted) {
  return fields.size() == 1 && !quoted.empty() && quoted[0] == 0 && fields[0].empty();
}

CsvRecordParser::RecordStatus CsvRecordParser::next_significant_record(
    std::vector<std::string> &fields, std::vector<char> &quoted, int64_t &start_line,
    std::string &err) {
  for (;;) {
    const RecordStatus status = next_record(fields, quoted, start_line, err);
    if (status != RECORD_OK || !is_blank_record(fields, quoted)) {
      return status;
    }
    // Blank line: skip it (DuckDB compatible).  A record that merely has an
    // empty first field, e.g. ",x", is not blank and is returned normally.
  }
}

CsvRecordParser::RecordStatus CsvRecordParser::next_record(std::vector<std::string> &fields,
                                                           std::vector<char> &quoted,
                                                           int64_t &start_line,
                                                           std::string &err) {
  fields.clear();
  quoted.clear();
  if (in_ == nullptr) {
    err = "CSV parser is not bound to an input stream";
    return RECORD_ERROR;
  }
  start_line = line_;
  std::string field;
  bool field_quoted = false;
  bool got_char = false;

  enum State {
    FIELD_START = 0,
    UNQUOTED,
    QUOTED,
    AFTER_QUOTE,
  };
  State state = FIELD_START;

  for (;;) {
    const int raw = in_->get();
    if (raw == -1) {
      if (state == QUOTED) {
        err = "line " + std::to_string(start_line) + ": unterminated quoted field";
        return RECORD_ERROR;
      }
      if (!got_char) {
        return RECORD_EOF;  // nothing left at all
      }
      // Last record of a file that does not end with a newline.
      fields.push_back(field);
      quoted.push_back(field_quoted ? 1 : 0);
      return RECORD_OK;
    }
    const char c = static_cast<char>(raw);
    got_char = true;

    if (c == '\n') {
      ++line_;
      if (state == QUOTED) {
        // Embedded newline inside a quoted field is data (normalized to LF).
        field.push_back('\n');
        continue;
      }
      fields.push_back(field);
      quoted.push_back(field_quoted ? 1 : 0);
      return RECORD_OK;
    }

    switch (state) {
      case FIELD_START: {
        if (c == quote_) {
          field_quoted = true;
          state = QUOTED;
        } else if (c == delimiter_) {
          fields.push_back(std::string());
          quoted.push_back(0);
        } else {
          field.push_back(c);
          state = UNQUOTED;
        }
        break;
      }
      case UNQUOTED: {
        if (c == delimiter_) {
          fields.push_back(field);
          quoted.push_back(0);
          field.clear();
          state = FIELD_START;
        } else if (c == quote_) {
          err = "line " + std::to_string(start_line) +
                ": unexpected quote character in unquoted field";
          return RECORD_ERROR;
        } else {
          field.push_back(c);
        }
        break;
      }
      case QUOTED: {
        if (escape_ != quote_) {
          // Explicit escape character (e.g. backslash): escapes the next byte.
          if (c == escape_) {
            const int next = in_->get();
            if (next == -1) {
              err = "line " + std::to_string(start_line) + ": unterminated quoted field";
              return RECORD_ERROR;
            }
            if (next == '\n') {
              ++line_;
            }
            field.push_back(static_cast<char>(next));
          } else if (c == quote_) {
            state = AFTER_QUOTE;
          } else {
            field.push_back(c);
          }
        } else if (c == quote_) {
          // Standard RFC 4180 doubling: "" inside a quoted field is one quote.
          if (in_->peek() == quote_) {
            in_->get();
            field.push_back(quote_);
          } else {
            state = AFTER_QUOTE;
          }
        } else {
          field.push_back(c);
        }
        break;
      }
      case AFTER_QUOTE: {
        if (c == delimiter_) {
          fields.push_back(field);
          quoted.push_back(1);
          field.clear();
          field_quoted = false;
          state = FIELD_START;
        } else if (c == ' ' || c == '\t') {
          // Tolerate padding after the closing quote.
        } else {
          err = "line " + std::to_string(start_line) + ": unexpected character " +
                char_snippet(c) + " after closing quote";
          return RECORD_ERROR;
        }
        break;
      }
      default: {
        err = "line " + std::to_string(start_line) + ": internal CSV parser state";
        return RECORD_ERROR;
      }
    }
  }
}

// ---------------------------------------------------------------------------
// CsvFileReader
// ---------------------------------------------------------------------------

bool CsvFileReader::validate_options(const FileOptions &opts, std::string &err) {
  if (opts.sample_size != -1) {
    // The task specification only allows a full-file scan; report the extra
    // option clearly so the SQL layer can map it to "unsupported feature".
    err = "sample_size other than -1 is not supported";
    return false;
  }
  if (opts.delimiter == '\0' || opts.delimiter == '\n' || opts.delimiter == '\r') {
    err = "invalid delimiter: must be a printable single byte";
    return false;
  }
  if (opts.quote == '\0' || opts.quote == '\n' || opts.quote == '\r') {
    err = "invalid quote: must be a printable single byte";
    return false;
  }
  if (opts.escape == '\0' || opts.escape == '\n' || opts.escape == '\r') {
    err = "invalid escape: must be a printable single byte";
    return false;
  }
  if (opts.delimiter == opts.quote) {
    err = "invalid options: delimiter and quote must differ";
    return false;
  }
  if (opts.escape == opts.delimiter) {
    err = "invalid options: escape and delimiter must differ";
    return false;
  }
  return true;
}

std::vector<std::string> CsvFileReader::build_column_names(
    const std::vector<std::string> &raw_names) {
  std::vector<std::string> names;
  names.reserve(raw_names.size());
  std::set<std::string> used;
  for (size_t i = 0; i < raw_names.size(); ++i) {
    std::string base = trim_blank(raw_names[i]);
    if (base.empty()) {
      base = "column" + std::to_string(i);
    }
    std::string name = base;
    int suffix = 2;
    while (used.find(name) != used.end()) {
      name = base + "__" + std::to_string(suffix);
      ++suffix;
    }
    used.insert(name);
    names.push_back(name);
  }
  return names;
}

ReadStatus CsvFileReader::infer_schema(const std::string &path, std::string &err) {
  (void)path;  // the stream is already positioned at the beginning
  schema_.clear();
  empty_ = true;
  parser_.bind(&in_);
  parser_.set_options(opts_.delimiter, opts_.quote, opts_.escape);

  std::vector<std::string> fields;
  std::vector<char> quoted;
  int64_t start_line = 0;
  std::vector<std::string> header_names;

  if (opts_.header) {
    const CsvRecordParser::RecordStatus status =
        parser_.next_significant_record(fields, quoted, start_line, err);
    if (status == CsvRecordParser::RECORD_ERROR) {
      return ReadStatus::ERROR;
    }
    if (status == CsvRecordParser::RECORD_EOF) {
      // Empty file (or only blank lines): no schema at all.
      return ReadStatus::OK;
    }
    header_names = build_column_names(fields);
  }

  std::vector<TypeInferencer> infer(header_names.size());
  size_t column_count = header_names.size();
  for (;;) {
    const CsvRecordParser::RecordStatus status =
        parser_.next_significant_record(fields, quoted, start_line, err);
    if (status == CsvRecordParser::RECORD_EOF) {
      break;
    }
    if (status == CsvRecordParser::RECORD_ERROR) {
      // Malformed record: stop inference and keep the schema derived from the
      // records before it.  next_row() re-reads the file and reports the same
      // error at the same line, so no diagnostics are lost.
      break;
    }
    if (column_count == 0) {
      // header = false: the first data record defines the column count.
      column_count = fields.size();
      infer.resize(column_count);
    }
    if (fields.size() != column_count) {
      break;  // resurfaces from next_row() as a line-numbered error
    }
    for (size_t j = 0; j < column_count; ++j) {
      infer[j].add(classify_csv_field(fields[j], quoted[j] != 0));
    }
    empty_ = false;
  }

  if (column_count == 0) {
    return ReadStatus::OK;  // empty file without header
  }
  for (size_t j = 0; j < column_count; ++j) {
    ColumnSchema column;
    column.name = header_names.empty()
                      ? "column" + std::to_string(j)
                      : header_names[j];
    column.type = infer[j].type();
    column.nullable = infer[j].nullable();
    schema_.push_back(column);
  }
  return ReadStatus::OK;
}

ReadStatus CsvFileReader::restart_for_reading(const std::string &path, std::string &err) {
  in_.close();
  if (!in_.open(path, err)) {
    return ReadStatus::ERROR;
  }
  parser_.bind(&in_);
  parser_.set_options(opts_.delimiter, opts_.quote, opts_.escape);
  if (opts_.header) {
    std::vector<std::string> fields;
    std::vector<char> quoted;
    int64_t start_line = 0;
    const CsvRecordParser::RecordStatus status =
        parser_.next_significant_record(fields, quoted, start_line, err);
    if (status == CsvRecordParser::RECORD_ERROR) {
      return ReadStatus::ERROR;
    }
    // RECORD_EOF means header-only file: next_row() will immediately report END.
  }
  return ReadStatus::OK;
}

ReadStatus CsvFileReader::open(const std::string &path, const FileOptions &opts) {
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
  ReadStatus status = infer_schema(path_, err);
  if (status != ReadStatus::OK) {
    last_error_ = err;
    in_.close();
    return ReadStatus::ERROR;
  }
  status = restart_for_reading(path_, err);
  if (status != ReadStatus::OK) {
    last_error_ = err;
    in_.close();
    return ReadStatus::ERROR;
  }
  opened_ = true;
  return ReadStatus::OK;
}

ReadStatus CsvFileReader::next_row(std::vector<Cell> &row, std::string &err) {
  err.clear();
  if (!opened_) {
    err = "CSV reader is not open";
    return ReadStatus::ERROR;
  }
  if (schema_.empty()) {
    row.clear();
    return ReadStatus::END;
  }
  for (;;) {
    std::vector<std::string> fields;
    std::vector<char> quoted;
    int64_t start_line = 0;
    const CsvRecordParser::RecordStatus status =
        parser_.next_significant_record(fields, quoted, start_line, err);
    if (status == CsvRecordParser::RECORD_EOF) {
      row.clear();
      return ReadStatus::END;
    }
    if (status == CsvRecordParser::RECORD_ERROR) {
      return ReadStatus::ERROR;  // `err` already carries "line N: ..."
    }
    if (fields.size() != schema_.size()) {
      err = "line " + std::to_string(start_line) + ": expected " +
            std::to_string(schema_.size()) + " fields but found " +
            std::to_string(fields.size());
      return ReadStatus::ERROR;
    }
    row.assign(schema_.size(), Cell());
    for (size_t j = 0; j < schema_.size(); ++j) {
      const bool was_quoted = j < quoted.size() && quoted[j] != 0;
      const InferredToken token = classify_csv_field(fields[j], was_quoted);
      std::string cell_err;
      if (!token_to_cell(token, schema_[j].type, row[j], cell_err)) {
        err = "line " + std::to_string(start_line) + ": column \"" + schema_[j].name +
              "\": " + cell_err;
        return ReadStatus::ERROR;
      }
    }
    return ReadStatus::OK;
  }
}

void CsvFileReader::close() {
  in_.close();
  schema_.clear();
  opened_ = false;
  empty_ = true;
}

std::unique_ptr<IFileReader> create_csv_reader() {
  return std::unique_ptr<IFileReader>(new CsvFileReader());
}

}  // namespace filesql
}  // namespace sql
}  // namespace oceanbase
