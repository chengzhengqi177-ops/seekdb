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

#pragma once
/**
 * Deterministic schema/type inference shared by the CSV and JSONL readers.
 *
 * The model is intentionally simple and reproducible: every non-NULL value of
 * a column is classified into exactly one "token kind", and the column type is
 * the least upper bound of all token kinds under the widening rules below.
 * There is no sampling: the readers always scan the whole file (sample_size is
 * fixed to -1 by the task specification), so the inferred schema does not
 * depend on row order or file size.
 *
 * Token -> type mapping
 *   BOOLEAN  -> BOOLEAN
 *   INTEGER  -> BIGINT
 *   DOUBLE   -> DOUBLE
 *   DATE     -> DATE
 *   DATETIME -> DATETIME
 *   TEXT     -> VARCHAR
 *   NULL     -> contributes nothing to the type, only to nullability
 *
 * Widening rules (commutative, documented because the SQL layer exposes them):
 *   NULLTYPE  + X            = X
 *   X         + X            = X
 *   BIGINT    + DOUBLE       = DOUBLE
 *   DATE      + DATETIME     = DATETIME   (date values get " 00:00:00")
 *   anything else that differs = VARCHAR  (a textual fallback, never NULL)
 *
 * Nullability: a column is nullable iff it contains at least one NULL value
 * (CSV: empty unquoted field; JSONL: JSON null or a missing key) or at least
 * one value that does not conform to the inferred type.  Under pure inference
 * every non-NULL value conforms by construction, so "non-conforming" can only
 * be observed when a value is coerced with an explicit target type.
 * An all-NULL column becomes a nullable VARCHAR column.
 *
 * Strictness: numeric / boolean / date / datetime recognition is exact.  A
 * value is only accepted when the whole string is a legal representation:
 * "12abc" is TEXT, " 1" is TEXT, "2026-02-30" is TEXT, "2026-13-01" is TEXT,
 * "nan" / "inf" / "0x10" / "1e" are TEXT.  An integer that does not fit int64
 * is classified as DOUBLE when it still fits a finite double, otherwise TEXT.
 * DATETIME accepts both "YYYY-MM-DD HH:MM:SS" and the ISO-8601 "T" separator
 * and is normalized to a space separator, matching the result file
 * (2026-08-03T12:13:14 -> 2026-08-03 12:13:14).  Fractional seconds are not
 * accepted (they would be TEXT); the task's value contract stores DATETIME at
 * second granularity.
 */

#include <cstdint>
#include <string>

#include "file_sql_types.h"

namespace oceanbase {
namespace sql {
namespace filesql {

// ---------------------------------------------------------------------------
// Strict text recognizers.  They never throw and never allocate the result
// beyond the returned bool; they only accept fully conforming strings.
// ---------------------------------------------------------------------------

// [+-]?[0-9]+ that fits in int64.
bool filesql_is_integer_text(const std::string &text, int64_t &value);

// [+-]? ( digits [ '.' digits? ] | '.' digits ) ( [eE][+-]?digits )?  that
// yields a finite double.  "nan", "inf", hex floats and partial parses fail.
bool filesql_is_double_text(const std::string &text, double &value);

// Case-insensitive "true" / "false".
bool filesql_is_boolean_text(const std::string &text, bool &value);

// Calendar validity: year 1..9999, month 1..12, day 1..days-in-month (leap).
bool filesql_is_valid_date_ymd(int year, int month, int day);

// Exactly "YYYY-MM-DD" and a valid calendar date.
bool filesql_is_date_text(const std::string &text);

// "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DDTHH:MM:SS", valid calendar date/time,
// normalized to a space separator in `normalized`.
bool filesql_is_datetime_text(const std::string &text, std::string &normalized);

// ---------------------------------------------------------------------------
// Token classification.
// ---------------------------------------------------------------------------

enum class TokenKind {
  NULL_VALUE = 0,
  BOOLEAN,
  INTEGER,
  DOUBLE,
  DATE,
  DATETIME,
  TEXT,
};

struct InferredToken {
  TokenKind kind = TokenKind::NULL_VALUE;
  int64_t i = 0;      // BOOLEAN (0/1) or INTEGER
  double d = 0;       // DOUBLE
  std::string raw;    // original textual form (VARCHAR fallback / diagnostics)
  std::string text;   // DATE "YYYY-MM-DD" / DATETIME "YYYY-MM-DD HH:MM:SS"
};

// Classifies one CSV field.  `quoted` distinguishes "empty unquoted field =
// NULL" from "quoted empty string = empty VARCHAR".
InferredToken classify_csv_field(const std::string &text, bool quoted);

// Classifies a decoded JSON string value.  Identical to a quoted CSV field:
// an empty JSON string is an empty VARCHAR, never NULL.
InferredToken classify_json_string(const std::string &text);

// Token kind -> logical type (NULL_VALUE maps to NULLTYPE).
LogicalType token_kind_type(TokenKind kind);

// ---------------------------------------------------------------------------
// Column-level inference.
// ---------------------------------------------------------------------------

LogicalType widen_type(LogicalType current, LogicalType next);

class TypeInferencer {
public:
  void add(const InferredToken &token);
  void add_null();

  bool has_value() const { return has_value_; }
  // Final type: an all-NULL column becomes VARCHAR.
  LogicalType type() const;
  // A column is nullable when it saw a NULL, or when it saw nothing at all.
  bool nullable() const { return nullable_ || !has_value_; }

private:
  LogicalType type_ = LogicalType::NULLTYPE;
  bool has_value_ = false;
  bool nullable_ = false;
};

// Converts an inferred token into the cell representation of `target`.
// Returns false and fills `err` when the token cannot be represented by the
// target type (only possible when a schema is forced, e.g. by the SQL layer).
bool token_to_cell(const InferredToken &token, LogicalType target, Cell &cell,
                   std::string &err);

// Human readable type name used in error messages and DESCRIBE output.
const char *filesql_type_name(LogicalType type);

// Returns the message of the last open()/next_row() failure for a reader
// created by this module, or nullptr when the reader does not track one
// (e.g. the Parquet reader).  IFileReader::open() has no err out parameter in
// the contract, so the SQL layer uses this bridge to report a useful message
// ("cannot open file: ...", "sample_size other than -1 is not supported", ...).
// The caller passes the format it used to create the reader, so no RTTI is
// required.  The returned pointer stays valid until the reader is closed or
// destroyed.
const std::string *filesql_last_error(IFileReader *reader, FileFormat format);

}  // namespace filesql
}  // namespace sql
}  // namespace oceanbase
