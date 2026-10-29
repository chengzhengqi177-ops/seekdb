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
 * Streaming CSV reader for the file-as-database feature (read_csv()).
 *
 * Grammar (RFC 4180 with the configurable bits that DuckDB exposes):
 *   - records are separated by LF or CRLF (both are normalized by
 *     BufferedReader; an embedded CRLF inside a quoted field becomes LF);
 *   - fields are separated by `delimiter`;
 *   - a field may be quoted with `quote`; inside a quoted field the delimiter
 *     and newlines are literal data;
 *   - a doubled quote inside a quoted field yields one literal quote
 *     ("say ""hi""" -> say "hi"); when `escape` is set to a character other
 *     than `quote` it escapes the next character instead ("a\"b" -> a"b);
 *   - an empty unquoted field is NULL, while "" is an empty VARCHAR;
 *   - whitespace after the closing quote is ignored, any other trailing
 *     character is a malformed record;
 *   - blank lines are skipped (DuckDB-compatible, see kSkipBlankLines in the
 *     .cpp for the exact rule);
 *   - a leading UTF-8 BOM is stripped.
 *
 * Reading model: open() makes exactly one full streaming pass over the file to
 * infer the column schema (sample_size is fixed to -1 by the task spec), then
 * seeks back to the first data record and next_row() parses one record at a
 * time.  No rows are buffered, so a file of any size can be scanned with
 * bounded memory.  The only per-row allocations are the cells that are
 * returned to the caller.
 *
 * Errors: open() reports failure through ReadStatus::ERROR and the message
 * stored in last_error() (the contract's open() signature has no err out
 * parameter).  next_row() reports malformed records through `err` with the
 * 1-based physical line number of the record, e.g.
 *   "line 4: expected 8 fields but found 9"
 */

#include <cstdint>
#include <string>
#include <vector>

#include "file_sql_infer.h"
#include "file_sql_io.h"
#include "file_sql_types.h"

namespace oceanbase {
namespace sql {
namespace filesql {

// Parses one CSV record at a time from a BufferedReader.  Reusable on its own
// (the JSONL reader does not need it, but the unit test exercises it directly).
class CsvRecordParser {
public:
  enum RecordStatus {
    RECORD_OK = 0,    // a record was produced
    RECORD_EOF = 1,   // no more records
    RECORD_ERROR = 2, // malformed input, see err
  };

  CsvRecordParser() {}
  ~CsvRecordParser() {}

  void bind(BufferedReader *in) {
    in_ = in;
    line_ = 1;
  }
  void set_options(char delimiter, char quote, char escape) {
    delimiter_ = delimiter;
    quote_ = quote;
    escape_ = escape;
  }
  int64_t line() const { return line_; }

  // Fills `fields` and a parallel `quoted` vector (1 = the field was quoted),
  // sets `start_line` to the 1-based physical line where the record started.
  RecordStatus next_record(std::vector<std::string> &fields, std::vector<char> &quoted,
                           int64_t &start_line, std::string &err);
  // Same, but silently skips blank lines (a blank line is a record whose only
  // field is an unquoted empty string).
  RecordStatus next_significant_record(std::vector<std::string> &fields,
                                       std::vector<char> &quoted, int64_t &start_line,
                                       std::string &err);

  static bool is_blank_record(const std::vector<std::string> &fields,
                              const std::vector<char> &quoted);

private:
  BufferedReader *in_ = nullptr;
  char delimiter_ = ',';
  char quote_ = '"';
  char escape_ = '"';
  int64_t line_ = 1;
};

// IFileReader implementation for CSV files.
class CsvFileReader : public IFileReader {
public:
  CsvFileReader() {}
  ~CsvFileReader() override { close(); }

  ReadStatus open(const std::string &path, const FileOptions &opts) override;
  const std::vector<ColumnSchema> &schema() const override { return schema_; }
  ReadStatus next_row(std::vector<Cell> &row, std::string &err) override;
  void close() override;

  // Message of the last open()/next_row() failure.  open() has no err out
  // parameter in the IFileReader contract, so the SQL layer reads the reason
  // from here (non-empty and stable until the next call).
  const std::string &last_error() const { return last_error_; }
  // True when the file contained no data records at all (empty file or only a
  // header): schema() may then be empty or all-VARCHAR.
  bool empty() const { return empty_; }

private:
  // One full streaming pass that builds schema_.
  ReadStatus infer_schema(const std::string &path, std::string &err);
  // Re-opens the file and skips the header again for the streaming pass.
  ReadStatus restart_for_reading(const std::string &path, std::string &err);
  static std::vector<std::string> build_column_names(const std::vector<std::string> &raw_names);
  static bool validate_options(const FileOptions &opts, std::string &err);

  std::string path_;
  FileOptions opts_;
  std::vector<ColumnSchema> schema_;
  BufferedReader in_;
  CsvRecordParser parser_;
  bool opened_ = false;
  bool empty_ = true;
  std::string last_error_;
};

}  // namespace filesql
}  // namespace sql
}  // namespace oceanbase
