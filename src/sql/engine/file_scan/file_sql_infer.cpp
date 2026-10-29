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

#include "file_sql_infer.h"

#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <cstring>

#include "file_sql_csv.h"
#include "file_sql_jsonl.h"

namespace oceanbase {
namespace sql {
namespace filesql {

namespace {

inline bool is_digit(char c) { return c >= '0' && c <= '9'; }

inline char to_lower(char c) {
  return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
}

// Reads exactly `len` digits starting at `pos`; leading zeros are allowed.
bool read_fixed_digits(const std::string &text, size_t pos, size_t len, int &value) {
  if (pos + len > text.size()) {
    return false;
  }
  int v = 0;
  for (size_t i = 0; i < len; ++i) {
    const char c = text[pos + i];
    if (!is_digit(c)) {
      return false;
    }
    v = v * 10 + (c - '0');
  }
  value = v;
  return true;
}

bool is_leap_year(int year) {
  return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

const int kDaysInMonth[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

}  // namespace

// ---------------------------------------------------------------------------
// Strict recognizers
// ---------------------------------------------------------------------------

bool filesql_is_integer_text(const std::string &text, int64_t &value) {
  if (text.empty()) {
    return false;
  }
  size_t pos = 0;
  bool negative = false;
  if (text[0] == '+' || text[0] == '-') {
    negative = (text[0] == '-');
    pos = 1;
  }
  if (pos >= text.size()) {
    return false;
  }
  // Accumulate in uint64 with an explicit limit check so that INT64_MIN works
  // and no signed overflow happens.
  const uint64_t limit =
      negative ? static_cast<uint64_t>(INT64_MAX) + 1ULL : static_cast<uint64_t>(INT64_MAX);
  uint64_t acc = 0;
  for (; pos < text.size(); ++pos) {
    const char c = text[pos];
    if (!is_digit(c)) {
      return false;
    }
    const uint64_t digit = static_cast<uint64_t>(c - '0');
    if (acc > (limit - digit) / 10ULL) {
      return false;  // does not fit int64
    }
    acc = acc * 10ULL + digit;
  }
  if (negative) {
    value = (acc == static_cast<uint64_t>(INT64_MAX) + 1ULL)
                ? INT64_MIN
                : -static_cast<int64_t>(acc);
  } else {
    value = static_cast<int64_t>(acc);
  }
  return true;
}

bool filesql_is_double_text(const std::string &text, double &value) {
  if (text.empty()) {
    return false;
  }
  const size_t n = text.size();
  size_t pos = 0;
  if (text[pos] == '+' || text[pos] == '-') {
    ++pos;
  }
  size_t int_digits = 0;
  while (pos < n && is_digit(text[pos])) {
    ++pos;
    ++int_digits;
  }
  size_t frac_digits = 0;
  if (pos < n && text[pos] == '.') {
    ++pos;
    while (pos < n && is_digit(text[pos])) {
      ++pos;
      ++frac_digits;
    }
  }
  if (int_digits == 0 && frac_digits == 0) {
    return false;
  }
  if (pos < n && (text[pos] == 'e' || text[pos] == 'E')) {
    ++pos;
    if (pos < n && (text[pos] == '+' || text[pos] == '-')) {
      ++pos;
    }
    size_t exp_digits = 0;
    while (pos < n && is_digit(text[pos])) {
      ++pos;
      ++exp_digits;
    }
    if (exp_digits == 0) {
      return false;
    }
  }
  if (pos != n) {
    return false;  // trailing garbage, hex, "nan", "inf", ...
  }
  // Grammar is valid; let strtod() produce the value and reject overflow to
  // infinity (underflow to 0 is fine).
  errno = 0;
  char *end = nullptr;
  const double parsed = std::strtod(text.c_str(), &end);
  if (end != text.c_str() + n || !std::isfinite(parsed)) {
    return false;
  }
  value = parsed;
  return true;
}

bool filesql_is_boolean_text(const std::string &text, bool &value) {
  if (text.size() == 4) {
    if (to_lower(text[0]) == 't' && to_lower(text[1]) == 'r' && to_lower(text[2]) == 'u' &&
        to_lower(text[3]) == 'e') {
      value = true;
      return true;
    }
  } else if (text.size() == 5) {
    if (to_lower(text[0]) == 'f' && to_lower(text[1]) == 'a' && to_lower(text[2]) == 'l' &&
        to_lower(text[3]) == 's' && to_lower(text[4]) == 'e') {
      value = false;
      return true;
    }
  }
  return false;
}

bool filesql_is_valid_date_ymd(int year, int month, int day) {
  if (year < 1 || year > 9999 || month < 1 || month > 12 || day < 1) {
    return false;
  }
  int max_day = kDaysInMonth[month - 1];
  if (month == 2 && is_leap_year(year)) {
    max_day = 29;
  }
  return day <= max_day;
}

bool filesql_is_date_text(const std::string &text) {
  if (text.size() != 10 || text[4] != '-' || text[7] != '-') {
    return false;
  }
  int year = 0;
  int month = 0;
  int day = 0;
  if (!read_fixed_digits(text, 0, 4, year) || !read_fixed_digits(text, 5, 2, month) ||
      !read_fixed_digits(text, 8, 2, day)) {
    return false;
  }
  return filesql_is_valid_date_ymd(year, month, day);
}

bool filesql_is_datetime_text(const std::string &text, std::string &normalized) {
  if (text.size() != 19) {
    return false;
  }
  const char sep = text[10];
  if (sep != ' ' && sep != 'T') {
    return false;
  }
  if (text[13] != ':' || text[16] != ':') {
    return false;
  }
  int year = 0;
  int month = 0;
  int day = 0;
  int hour = 0;
  int minute = 0;
  int second = 0;
  if (!read_fixed_digits(text, 0, 4, year) || !read_fixed_digits(text, 5, 2, month) ||
      !read_fixed_digits(text, 8, 2, day) || !read_fixed_digits(text, 11, 2, hour) ||
      !read_fixed_digits(text, 14, 2, minute) || !read_fixed_digits(text, 17, 2, second)) {
    return false;
  }
  if (!filesql_is_valid_date_ymd(year, month, day) || hour > 23 || minute > 59 || second > 59) {
    return false;
  }
  normalized = text;
  normalized[10] = ' ';
  return true;
}

// ---------------------------------------------------------------------------
// Token classification
// ---------------------------------------------------------------------------

InferredToken classify_csv_field(const std::string &text, bool quoted) {
  InferredToken token;
  token.raw = text;
  if (!quoted && text.empty()) {
    token.kind = TokenKind::NULL_VALUE;  // empty unquoted field == NULL
    return token;
  }
  int64_t int_value = 0;
  if (filesql_is_integer_text(text, int_value)) {
    token.kind = TokenKind::INTEGER;
    token.i = int_value;
    return token;
  }
  double double_value = 0.0;
  if (filesql_is_double_text(text, double_value)) {
    token.kind = TokenKind::DOUBLE;
    token.d = double_value;
    return token;
  }
  bool bool_value = false;
  if (filesql_is_boolean_text(text, bool_value)) {
    token.kind = TokenKind::BOOLEAN;
    token.i = bool_value ? 1 : 0;
    return token;
  }
  if (filesql_is_date_text(text)) {
    token.kind = TokenKind::DATE;
    token.text = text;
    return token;
  }
  std::string normalized;
  if (filesql_is_datetime_text(text, normalized)) {
    token.kind = TokenKind::DATETIME;
    token.text = normalized;
    return token;
  }
  token.kind = TokenKind::TEXT;
  token.text = text;  // quoted empty string lands here as an empty VARCHAR
  return token;
}

InferredToken classify_json_string(const std::string &text) {
  // A JSON string is always a "quoted" value: "" is an empty VARCHAR.
  return classify_csv_field(text, true);
}

LogicalType token_kind_type(TokenKind kind) {
  switch (kind) {
    case TokenKind::BOOLEAN:
      return LogicalType::BOOLEAN;
    case TokenKind::INTEGER:
      return LogicalType::BIGINT;
    case TokenKind::DOUBLE:
      return LogicalType::DOUBLE;
    case TokenKind::DATE:
      return LogicalType::DATE;
    case TokenKind::DATETIME:
      return LogicalType::DATETIME;
    case TokenKind::TEXT:
      return LogicalType::VARCHAR;
    case TokenKind::NULL_VALUE:
    default:
      return LogicalType::NULLTYPE;
  }
}

// ---------------------------------------------------------------------------
// Column inference
// ---------------------------------------------------------------------------

LogicalType widen_type(LogicalType current, LogicalType next) {
  if (current == LogicalType::NULLTYPE) {
    return next;
  }
  if (next == LogicalType::NULLTYPE || current == next) {
    return current;
  }
  if ((current == LogicalType::BIGINT && next == LogicalType::DOUBLE) ||
      (current == LogicalType::DOUBLE && next == LogicalType::BIGINT)) {
    return LogicalType::DOUBLE;
  }
  if ((current == LogicalType::DATE && next == LogicalType::DATETIME) ||
      (current == LogicalType::DATETIME && next == LogicalType::DATE)) {
    return LogicalType::DATETIME;
  }
  // Any other mixture (e.g. BOOLEAN + BIGINT, BIGINT + VARCHAR) degrades to a
  // textual column so that no value is lost.
  return LogicalType::VARCHAR;
}

void TypeInferencer::add(const InferredToken &token) {
  if (token.kind == TokenKind::NULL_VALUE) {
    nullable_ = true;
    return;
  }
  type_ = widen_type(type_, token_kind_type(token.kind));
  has_value_ = true;
}

void TypeInferencer::add_null() { nullable_ = true; }

LogicalType TypeInferencer::type() const {
  return type_ == LogicalType::NULLTYPE ? LogicalType::VARCHAR : type_;
}

// ---------------------------------------------------------------------------
// Cell conversion
// ---------------------------------------------------------------------------

bool token_to_cell(const InferredToken &token, LogicalType target, Cell &cell, std::string &err) {
  cell = Cell();
  if (token.kind == TokenKind::NULL_VALUE) {
    cell.is_null = true;
    return true;
  }
  switch (target) {
    case LogicalType::BIGINT: {
      if (token.kind == TokenKind::INTEGER) {
        cell = make_int(token.i);
        return true;
      }
      int64_t value = 0;
      if (filesql_is_integer_text(token.raw, value)) {
        cell = make_int(value);
        return true;
      }
      err = "value \"" + token.raw + "\" is not a valid BIGINT";
      return false;
    }
    case LogicalType::DOUBLE: {
      if (token.kind == TokenKind::INTEGER) {
        cell = make_double(static_cast<double>(token.i));
        return true;
      }
      if (token.kind == TokenKind::DOUBLE) {
        cell = make_double(token.d);
        return true;
      }
      double value = 0.0;
      if (filesql_is_double_text(token.raw, value)) {
        cell = make_double(value);
        return true;
      }
      err = "value \"" + token.raw + "\" is not a valid DOUBLE";
      return false;
    }
    case LogicalType::BOOLEAN: {
      if (token.kind == TokenKind::BOOLEAN) {
        cell = make_bool(token.i != 0);
        return true;
      }
      bool value = false;
      if (filesql_is_boolean_text(token.raw, value)) {
        cell = make_bool(value);
        return true;
      }
      err = "value \"" + token.raw + "\" is not a valid BOOLEAN";
      return false;
    }
    case LogicalType::DATE: {
      if (token.kind == TokenKind::DATE) {
        cell = make_string(token.text);
        return true;
      }
      err = "value \"" + token.raw + "\" is not a valid DATE";
      return false;
    }
    case LogicalType::DATETIME: {
      if (token.kind == TokenKind::DATETIME) {
        cell = make_string(token.text);
        return true;
      }
      if (token.kind == TokenKind::DATE) {
        // DATE widened into DATETIME: midnight of that day.
        cell = make_string(token.text + " 00:00:00");
        return true;
      }
      err = "value \"" + token.raw + "\" is not a valid DATETIME";
      return false;
    }
    case LogicalType::VARCHAR: {
      // Every value has a textual representation; this is the widening target
      // that guarantees no information is lost.
      cell = make_string(token.raw);
      return true;
    }
    case LogicalType::NULLTYPE:
    default: {
      // No type information (all-NULL column kept as NULLTYPE by a forced
      // schema): the value is reported as NULL rather than fabricated.
      cell = Cell();
      return true;
    }
  }
}

const char *filesql_type_name(LogicalType type) {
  switch (type) {
    case LogicalType::BIGINT:
      return "BIGINT";
    case LogicalType::DOUBLE:
      return "DOUBLE";
    case LogicalType::BOOLEAN:
      return "BOOLEAN";
    case LogicalType::VARCHAR:
      return "VARCHAR";
    case LogicalType::DATE:
      return "DATE";
    case LogicalType::DATETIME:
      return "DATETIME";
    case LogicalType::NULLTYPE:
    default:
      return "NULL";
  }
}

// The contract header declares this utility; the shared inference module owns
// its definition so that DESCRIBE output and reader diagnostics cannot drift
// apart.  (The integration layer must not define it a second time.)
const char *logical_type_name(LogicalType type) { return filesql_type_name(type); }

const std::string *filesql_last_error(IFileReader *reader, FileFormat format) {
  if (reader == nullptr) {
    return nullptr;
  }
  switch (format) {
    case FileFormat::CSV:
      return &static_cast<CsvFileReader *>(reader)->last_error();
    case FileFormat::JSONL:
      return &static_cast<JsonlFileReader *>(reader)->last_error();
    default:
      // Parquet (or any future format) is implemented elsewhere and does not
      // provide this accessor.
      return nullptr;
  }
}

}  // namespace filesql
}  // namespace sql
}  // namespace oceanbase
