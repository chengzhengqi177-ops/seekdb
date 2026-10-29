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
 * Streaming JSONL / NDJSON reader for the file-as-database feature
 * (read_json(path, format = 'newline_delimited')).
 *
 * Input model: one flat JSON object per physical line.  Empty/whitespace-only
 * lines are skipped, LF and CRLF are both accepted, a leading UTF-8 BOM is
 * stripped.  Nested objects and arrays are rejected with a line-numbered error
 * that also names the offending column, because the SQL layer only exposes
 * scalar columns (use read_csv/Parquet or flatten upstream for nested data).
 *
 * Schema: the union of the keys of *all* lines is computed upfront (one full
 * streaming pass; no rows are buffered).  Column order is deterministic: keys
 * are ordered by first appearance, i.e. by (first line number, position inside
 * that line), with the key name as a final tie-break.  For basic.jsonl this
 * yields id, name, score, active, day, note, which is exactly the order used
 * by tools/deploy/mysql_test/test_suite/file_sql/r/mysql/file_sql_basic.result.
 *
 * A key that is missing from a line, or explicitly null, produces a NULL cell,
 * and makes the column nullable.
 *
 * Type conflict handling (shared with the CSV reader, documented here because
 * it is observable from SQL):
 *   - integer + float            -> DOUBLE
 *   - number + numeric string    -> DOUBLE  (all values are numeric)
 *   - number/boolean + non numeric string -> VARCHAR (no data is lost; the
 *     original textual form is preserved)
 *   - boolean + integer          -> VARCHAR (true/false and 1 are different
 *     domains; widening to text avoids a silent semantic change)
 *   - date string + datetime string -> DATETIME (dates become midnight)
 *   - JSON null never changes the type, it only marks the column nullable.
 *   A numeric literal that does not fit int64 is classified as DOUBLE; one that
 *   does not fit a finite double either (e.g. 1e999) becomes VARCHAR text.
 *
 * Error reporting mirrors the CSV reader: open() failures (missing file,
 * invalid options) are reported through ReadStatus::ERROR plus last_error();
 * malformed lines are reported from next_row() through `err` with the 1-based
 * physical line number, e.g. "line 2: nested object/array value is not
 * supported (column \"a\")".
 */

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "file_sql_infer.h"
#include "file_sql_io.h"
#include "file_sql_types.h"

namespace oceanbase {
namespace sql {
namespace filesql {

class JsonlFileReader : public IFileReader {
public:
  JsonlFileReader() {}
  ~JsonlFileReader() override { close(); }

  ReadStatus open(const std::string &path, const FileOptions &opts) override;
  const std::vector<ColumnSchema> &schema() const override { return schema_; }
  ReadStatus next_row(std::vector<Cell> &row, std::string &err) override;
  void close() override;

  // Reason of the last open() failure (open() has no err out parameter).
  const std::string &last_error() const { return last_error_; }
  // True when the file contained no JSON object at all.
  bool empty() const { return empty_; }

private:
  ReadStatus scan_schema(std::string &err);
  ReadStatus reopen_for_reading(std::string &err);
  static bool validate_options(const FileOptions &opts, std::string &err);

  std::string path_;
  FileOptions opts_;
  std::vector<ColumnSchema> schema_;
  std::unordered_map<std::string, size_t> column_index_;
  BufferedReader in_;
  bool opened_ = false;
  bool empty_ = true;
  std::string last_error_;
  int64_t line_no_ = 0;
};

}  // namespace filesql
}  // namespace sql
}  // namespace oceanbase
