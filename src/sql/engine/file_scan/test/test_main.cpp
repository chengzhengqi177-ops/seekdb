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

/**
 * Standalone unit test for the file-as-database CSV / JSONL readers.
 *
 * Build & run (see test/run_tests.ps1):
 *   cl /std:c++17 /EHsc /O2 file_sql_csv.cpp file_sql_jsonl.cpp
 *      file_sql_infer.cpp test\test_main.cpp /Fe:test\test_file_sql.exe
 *   test\test_file_sql.exe <repo>/tools/deploy/mysql_test/test_suite/file_sql/data
 *
 * It verifies:
 *   - the shipped basic.csv / basic.jsonl fixtures match the expected rows and
 *     schema from tools/deploy/mysql_test/test_suite/file_sql/r/mysql/
 *     file_sql_basic.result (including the multiline quoted note and the
 *     duplicated id column);
 *   - CSRF/CRLF, BOM, quoted/unquoted empties, duplicate header names,
 *     delimiter/quote/escape options, embedded delimiter and newline, missing
 *     and extra fields, int64 overflow, strict date/datetime validation,
 *     header=false, huge single fields and UTF-8 payloads;
 *   - JSONL union schemas, deterministic column order, missing -> NULL,
 *     nested/array rejection with line number and column name, type conflicts,
 *     unicode escapes, duplicate keys and malformed lines;
 *   - the shared inference helpers (strict recognizers, widening rules).
 *
 * The test writes its throw-away fixtures into <this dir>/_tmp_reader_tests and
 * deletes that directory at the end.  Exit code 0 means every check passed.
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "../file_sql_csv.h"
#include "../file_sql_infer.h"
#include "../file_sql_jsonl.h"

namespace fs = std::filesystem;
using namespace oceanbase::sql::filesql;

namespace {

int g_checks = 0;
int g_failures = 0;
std::string g_temp_dir;

const char *kSourceName = "test_main.cpp";

// ---------------------------------------------------------------------------
// Check helpers
// ---------------------------------------------------------------------------

void fail(int line, const std::string &message) {
  ++g_failures;
  std::printf("FAIL %s:%d: %s\n", kSourceName, line, message.c_str());
}

void check_true(bool condition, const char *expr, int line) {
  ++g_checks;
  if (!condition) {
    fail(line, std::string("expected true: ") + expr);
  }
}

void check_str(const std::string &actual, const std::string &expected, const char *what,
               int line) {
  ++g_checks;
  if (actual != expected) {
    fail(line, std::string(what) + ": expected \"" + expected + "\", got \"" + actual + "\"");
  }
}

void check_i64(int64_t actual, int64_t expected, const char *what, int line) {
  ++g_checks;
  if (actual != expected) {
    fail(line, std::string(what) + ": expected " + std::to_string(expected) + ", got " +
                   std::to_string(actual));
  }
}

void check_u64(uint64_t actual, uint64_t expected, const char *what, int line) {
  ++g_checks;
  if (actual != expected) {
    fail(line, std::string(what) + ": expected " + std::to_string(expected) + ", got " +
                   std::to_string(actual));
  }
}

void check_double(double actual, double expected, const char *what, int line) {
  ++g_checks;
  const double diff = actual > expected ? actual - expected : expected - actual;
  const double scale = expected > 1.0 || expected < -1.0 ? expected : 1.0;
  if (!(diff <= 1e-9 * (scale > 0 ? scale : -scale))) {
    char buf[160];
    std::snprintf(buf, sizeof(buf), "%s: expected %.17g, got %.17g", what, expected, actual);
    fail(line, buf);
  }
}

void check_type(LogicalType actual, LogicalType expected, const char *what, int line) {
  ++g_checks;
  if (actual != expected) {
    fail(line, std::string(what) + ": expected type " + filesql_type_name(expected) + ", got " +
                   filesql_type_name(actual));
  }
}

void check_status(ReadStatus actual, ReadStatus expected, const char *what, int line) {
  ++g_checks;
  static const char *kNames[] = {"OK", "END", "ERROR"};
  const int a = static_cast<int>(actual);
  const int e = static_cast<int>(expected);
  if (a != e) {
    fail(line, std::string(what) + ": expected " + kNames[e] + ", got " + kNames[a]);
  }
}

#define CHECK(cond) check_true((cond), #cond, __LINE__)
#define CHECK_STR(actual, expected, what) check_str((actual), (expected), (what), __LINE__)
#define CHECK_I64(actual, expected) check_i64((actual), (expected), #actual, __LINE__)
#define CHECK_I64_MSG(actual, expected, what) check_i64((actual), (expected), (what), __LINE__)
#define CHECK_U64(actual, expected) check_u64((actual), (expected), #actual, __LINE__)
#define CHECK_DOUBLE(actual, expected, what) check_double((actual), (expected), (what), __LINE__)
#define CHECK_TYPE(actual, expected, what) check_type((actual), (expected), (what), __LINE__)
#define CHECK_STATUS(actual, expected, what) check_status((actual), (expected), (what), __LINE__)

// ---------------------------------------------------------------------------
// Cell helpers
// ---------------------------------------------------------------------------

std::string printable(const std::string &text) {
  std::string out;
  out.reserve(text.size());
  for (size_t i = 0; i < text.size(); ++i) {
    const char c = text[i];
    if (c == '\n') {
      out += "\\n";
    } else if (c == '\r') {
      out += "\\r";
    } else if (c == '\t') {
      out += "\\t";
    } else {
      out.push_back(c);
    }
  }
  return out;
}

std::string cell_to_string(const Cell &cell, LogicalType type) {
  if (cell.is_null) {
    return "NULL";
  }
  char buf[64];
  switch (type) {
    case LogicalType::BIGINT:
      std::snprintf(buf, sizeof(buf), "%lld", static_cast<long long>(cell.i));
      return buf;
    case LogicalType::BOOLEAN:
      return cell.i != 0 ? "true" : "false";
    case LogicalType::DOUBLE: {
      std::snprintf(buf, sizeof(buf), "%.15g", cell.d);
      return buf;
    }
    case LogicalType::DATE:
    case LogicalType::DATETIME:
    case LogicalType::VARCHAR:
      return printable(cell.s);
    case LogicalType::NULLTYPE:
    default:
      return "NULL";
  }
}

void check_cell_int(const Cell &cell, int64_t expected, const std::string &what, int line) {
  ++g_checks;
  if (cell.is_null) {
    fail(line, what + ": expected " + std::to_string(expected) + ", got NULL");
  } else if (cell.i != expected) {
    fail(line, what + ": expected " + std::to_string(expected) + ", got " +
                   std::to_string(cell.i));
  }
}

void check_cell_double(const Cell &cell, double expected, const std::string &what, int line) {
  ++g_checks;
  if (cell.is_null) {
    fail(line, what + ": expected a double, got NULL");
    return;
  }
  const double diff = cell.d > expected ? cell.d - expected : expected - cell.d;
  if (!(diff <= 1e-9 * (expected == 0.0 ? 1.0 : (expected > 0 ? expected : -expected)))) {
    char buf[160];
    std::snprintf(buf, sizeof(buf), "%s: expected %.17g, got %.17g", what.c_str(), expected,
                  cell.d);
    fail(line, buf);
  }
}

void check_cell_bool(const Cell &cell, bool expected, const std::string &what, int line) {
  ++g_checks;
  const int64_t want = expected ? 1 : 0;
  if (cell.is_null) {
    fail(line, what + ": expected bool, got NULL");
  } else if (cell.i != want) {
    fail(line, what + ": expected " + (expected ? "true" : "false") + ", got " +
                   std::to_string(cell.i));
  }
}

void check_cell_str(const Cell &cell, const std::string &expected, const std::string &what,
                    int line) {
  ++g_checks;
  if (cell.is_null) {
    fail(line, what + ": expected \"" + printable(expected) + "\", got NULL");
  } else if (cell.s != expected) {
    fail(line, what + ": expected \"" + printable(expected) + "\", got \"" +
                   printable(cell.s) + "\"");
  }
}

void check_cell_null(const Cell &cell, const std::string &what, int line) {
  ++g_checks;
  if (!cell.is_null) {
    fail(line, what + ": expected NULL");
  }
}

#define CHECK_CELL_INT(cell, expected, what) check_cell_int((cell), (expected), (what), __LINE__)
#define CHECK_CELL_DOUBLE(cell, expected, what) \
  check_cell_double((cell), (expected), (what), __LINE__)
#define CHECK_CELL_BOOL(cell, expected, what) check_cell_bool((cell), (expected), (what), __LINE__)
#define CHECK_CELL_STR(cell, expected, what) \
  check_cell_str((cell), (expected), (what), __LINE__)
#define CHECK_CELL_NULL(cell, what) check_cell_null((cell), (what), __LINE__)

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

std::string temp_path(const std::string &name) { return g_temp_dir + "/" + name; }

void write_fixture(const std::string &name, const std::string &bytes) {
  const std::string path = temp_path(name);
  std::ofstream out(path.c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
  if (!out.is_open()) {
    fail(__LINE__, "cannot create fixture " + path);
    return;
  }
  out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  out.close();
  ++g_checks;
}

void dump_row(const std::vector<Cell> &row, const std::vector<ColumnSchema> &schema,
              int64_t row_no) {
  std::printf("  row %lld:", static_cast<long long>(row_no));
  for (size_t j = 0; j < schema.size() && j < row.size(); ++j) {
    std::printf(" %s=%s", schema[j].name.c_str(), cell_to_string(row[j], schema[j].type).c_str());
  }
  std::printf("\n");
}

// ---------------------------------------------------------------------------
// Locating the shipped fixtures
// ---------------------------------------------------------------------------

std::string repo_root_from_source_path() {
  std::string file = __FILE__;
  for (size_t i = 0; i < file.size(); ++i) {
    if (file[i] == '\\') {
      file[i] = '/';
    }
  }
  const std::string marker = "/src/sql/engine/file_scan/test/";
  const size_t pos = file.find(marker);
  if (pos == std::string::npos) {
    return std::string();
  }
  return file.substr(0, pos);
}

fs::path fixture_relative_path() {
  return fs::path("tools") / "deploy" / "mysql_test" / "test_suite" / "file_sql" / "data";
}

bool has_basic_csv(const fs::path &dir) {
  std::error_code ec;
  const bool exists = fs::exists(dir / "basic.csv", ec);
  return !ec && exists;
}

std::string search_upwards(fs::path start) {
  for (int i = 0; i < 12; ++i) {
    const fs::path candidate = start / fixture_relative_path();
    if (has_basic_csv(candidate)) {
      return candidate.string();
    }
    if (!start.has_parent_path() || start.parent_path() == start) {
      break;
    }
    start = start.parent_path();
  }
  return std::string();
}

std::string find_data_dir(int argc, char **argv) {
  if (argc > 1) {
    const fs::path given(argv[1]);
    if (has_basic_csv(given)) {
      return given.string();
    }
    std::printf("warning: argv[1] \"%s\" does not contain basic.csv, auto-detecting\n", argv[1]);
  }
  const std::string root = repo_root_from_source_path();
  if (!root.empty()) {
    const fs::path candidate = fs::path(root) / fixture_relative_path();
    if (has_basic_csv(candidate)) {
      return candidate.string();
    }
  }
  std::error_code ec;
  const fs::path cwd = fs::current_path(ec);
  if (!ec) {
    const std::string found = search_upwards(cwd);
    if (!found.empty()) {
      return found;
    }
  }
  return std::string();
}

// ---------------------------------------------------------------------------
// basic.csv / basic.jsonl (the shipped competition fixtures)
// ---------------------------------------------------------------------------

void test_basic_csv(const std::string &data_dir) {
  std::printf("[test] basic.csv (shipped fixture)\n");
  CsvFileReader reader;
  FileOptions opts;  // header = true, ',' delimiter, '"' quote, sample_size = -1
  CHECK_STATUS(reader.open(data_dir + "/basic.csv", opts), ReadStatus::OK, "open basic.csv");
  if (!reader.last_error().empty()) {
    std::printf("  open error: %s\n", reader.last_error().c_str());
  }
  CHECK(!reader.empty());

  // DESCRIBE expectations from file_sql_basic.result.
  static const char *kNames[8] = {"id", "name", "score", "active",
                                  "day", "created", "note", "id__2"};
  static const LogicalType kTypes[8] = {
      LogicalType::BIGINT,  LogicalType::VARCHAR, LogicalType::DOUBLE, LogicalType::BOOLEAN,
      LogicalType::DATE,    LogicalType::DATETIME, LogicalType::VARCHAR, LogicalType::BIGINT};
  static const bool kNullable[8] = {false, false, false, false, true, true, false, false};
  CHECK_U64(reader.schema().size(), 8);
  for (size_t j = 0; j < reader.schema().size() && j < 8; ++j) {
    CHECK_STR(reader.schema()[j].name, kNames[j], "column name");
    CHECK_TYPE(reader.schema()[j].type, kTypes[j], "column type");
    CHECK(reader.schema()[j].nullable == kNullable[j]);
  }

  std::vector<Cell> row;
  std::string err;

  // Row 1: quoted field with an embedded newline and an embedded delimiter.
  CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "row 1");
  CHECK_U64(row.size(), 8);
  if (row.size() == 8) {
    dump_row(row, reader.schema(), 1);
    CHECK_CELL_INT(row[0], 1, "row1 id");
    CHECK_CELL_STR(row[1], "Alice", "row1 name");
    CHECK_CELL_DOUBLE(row[2], 98.5, "row1 score");
    CHECK_CELL_BOOL(row[3], true, "row1 active");
    CHECK_CELL_STR(row[4], "2026-08-01", "row1 day");
    CHECK_CELL_STR(row[5], "2026-08-01 10:11:12", "row1 created");
    CHECK_CELL_STR(row[6], "hello,\nworld", "row1 note");
    CHECK_CELL_INT(row[7], 101, "row1 id__2");
  }

  // Row 2: empty unquoted day/created are NULL.
  CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "row 2");
  if (row.size() == 8) {
    dump_row(row, reader.schema(), 2);
    CHECK_CELL_INT(row[0], 2, "row2 id");
    CHECK_CELL_STR(row[1], "Bob", "row2 name");
    CHECK_CELL_DOUBLE(row[2], 87.0, "row2 score");
    CHECK_CELL_BOOL(row[3], false, "row2 active");
    CHECK_CELL_NULL(row[4], "row2 day");
    CHECK_CELL_NULL(row[5], "row2 created");
    CHECK_CELL_STR(row[6], "plain", "row2 note");
    CHECK_CELL_INT(row[7], 102, "row2 id__2");
  }

  // Row 3: doubled quotes -> literal quote, ISO 'T' datetime separator.
  CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "row 3");
  if (row.size() == 8) {
    dump_row(row, reader.schema(), 3);
    CHECK_CELL_INT(row[0], 3, "row3 id");
    CHECK_CELL_STR(row[1], "Chen", "row3 name");
    CHECK_CELL_DOUBLE(row[2], 90.25, "row3 score");
    CHECK_CELL_BOOL(row[3], true, "row3 active");
    CHECK_CELL_STR(row[4], "2026-08-03", "row3 day");
    CHECK_CELL_STR(row[5], "2026-08-03 12:13:14", "row3 created");
    CHECK_CELL_STR(row[6], "say \"hi\"", "row3 note");
    CHECK_CELL_INT(row[7], 103, "row3 id__2");
  }

  CHECK_STATUS(reader.next_row(row, err), ReadStatus::END, "eof after 3 rows");
  CHECK_STATUS(reader.next_row(row, err), ReadStatus::END, "repeated eof");

  // The reader must be reusable through the factory interface as well.
  std::unique_ptr<IFileReader> generic = create_csv_reader();
  CHECK(generic.get() != nullptr);
  CHECK_STATUS(generic->open(data_dir + "/basic.csv", opts), ReadStatus::OK, "factory open");
  CHECK_U64(generic->schema().size(), 8);
  CHECK_STATUS(generic->next_row(row, err), ReadStatus::OK, "factory row 1");
  generic->close();
  CHECK_STATUS(generic->next_row(row, err), ReadStatus::ERROR, "closed reader");
}

void test_basic_jsonl(const std::string &data_dir) {
  std::printf("[test] basic.jsonl (shipped fixture)\n");
  JsonlFileReader reader;
  FileOptions opts;
  opts.newline_delimited = true;
  CHECK_STATUS(reader.open(data_dir + "/basic.jsonl", opts), ReadStatus::OK, "open basic.jsonl");
  if (!reader.last_error().empty()) {
    std::printf("  open error: %s\n", reader.last_error().c_str());
  }

  // Union schema: id, name, score, active, day, note (first-appearance order).
  static const char *kNames[6] = {"id", "name", "score", "active", "day", "note"};
  static const LogicalType kTypes[6] = {LogicalType::BIGINT, LogicalType::VARCHAR,
                                        LogicalType::DOUBLE, LogicalType::BOOLEAN,
                                        LogicalType::DATE,   LogicalType::VARCHAR};
  static const bool kNullable[6] = {false, false, true, false, true, true};
  CHECK_U64(reader.schema().size(), 6);
  for (size_t j = 0; j < reader.schema().size() && j < 6; ++j) {
    CHECK_STR(reader.schema()[j].name, kNames[j], "column name");
    CHECK_TYPE(reader.schema()[j].type, kTypes[j], "column type");
    CHECK(reader.schema()[j].nullable == kNullable[j]);
  }

  std::vector<Cell> row;
  std::string err;

  CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "jsonl row 1");
  CHECK_U64(row.size(), 6);
  if (row.size() == 6) {
    dump_row(row, reader.schema(), 1);
    CHECK_CELL_INT(row[0], 1, "row1 id");
    CHECK_CELL_STR(row[1], "Alice", "row1 name");
    CHECK_CELL_DOUBLE(row[2], 98.5, "row1 score");
    CHECK_CELL_BOOL(row[3], true, "row1 active");
    CHECK_CELL_NULL(row[4], "row1 day");
    CHECK_CELL_NULL(row[5], "row1 note");
  }

  CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "jsonl row 2");
  if (row.size() == 6) {
    dump_row(row, reader.schema(), 2);
    CHECK_CELL_INT(row[0], 2, "row2 id");
    CHECK_CELL_STR(row[1], "Bob", "row2 name");
    CHECK_CELL_NULL(row[2], "row2 score");
    CHECK_CELL_BOOL(row[3], false, "row2 active");
    CHECK_CELL_STR(row[4], "2026-08-02", "row2 day");
    CHECK_CELL_NULL(row[5], "row2 note");
  }

  // score is a mixed integer/float column -> DOUBLE, 90 must arrive as 90.0.
  CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "jsonl row 3");
  if (row.size() == 6) {
    dump_row(row, reader.schema(), 3);
    CHECK_CELL_INT(row[0], 3, "row3 id");
    CHECK_CELL_STR(row[1], "Chen", "row3 name");
    CHECK_CELL_DOUBLE(row[2], 90.0, "row3 score");
    CHECK_CELL_BOOL(row[3], true, "row3 active");
    CHECK_CELL_NULL(row[4], "row3 day");
    CHECK_CELL_STR(row[5], "ok", "row3 note");
  }

  CHECK_STATUS(reader.next_row(row, err), ReadStatus::END, "jsonl eof");
}

// ---------------------------------------------------------------------------
// CSV robustness / option coverage
// ---------------------------------------------------------------------------

void test_csv_edge_cases() {
  std::printf("[test] CSV edge cases\n");
  std::vector<Cell> row;
  std::string err;

  // Nonexistent file.
  {
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("does_not_exist.csv"), opts), ReadStatus::ERROR,
                 "missing file");
    CHECK(reader.last_error().find("cannot open file") != std::string::npos);
  }

  // The engine-side bridge that exposes open() diagnostics through IFileReader
  // (open() has no err out parameter in the contract header).
  {
    std::unique_ptr<IFileReader> generic = create_csv_reader();
    FileOptions opts;
    CHECK_STATUS(generic->open(temp_path("does_not_exist.csv"), opts), ReadStatus::ERROR,
                 "bridge open");
    const std::string *message = filesql_last_error(generic.get(), FileFormat::CSV);
    CHECK(message != nullptr);
    if (message != nullptr) {
      CHECK(message->find("cannot open file") != std::string::npos);
    }
    CHECK(filesql_last_error(generic.get(), FileFormat::PARQUET) == nullptr);
  }

  // Empty file: valid, zero columns, immediate EOF.
  {
    write_fixture("empty.csv", "");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("empty.csv"), opts), ReadStatus::OK, "empty file open");
    CHECK_U64(reader.schema().size(), 0);
    CHECK(reader.empty());
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::END, "empty file row");
  }

  // Header only: the header defines the columns, all-VARCHAR nullable.
  {
    write_fixture("header_only.csv", "a,b,c\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("header_only.csv"), opts), ReadStatus::OK, "header only");
    CHECK_U64(reader.schema().size(), 3);
    CHECK_STR(reader.schema()[0].name, "a", "header name 0");
    CHECK_STR(reader.schema()[2].name, "c", "header name 2");
    for (size_t j = 0; j < reader.schema().size(); ++j) {
      CHECK_TYPE(reader.schema()[j].type, LogicalType::VARCHAR, "header-only type");
      CHECK(reader.schema()[j].nullable);
    }
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::END, "header only row");
  }

  // CRLF file, with an embedded CRLF inside a quoted field normalized to LF.
  {
    write_fixture("crlf.csv", "a,b\r\n1,\"x\r\ny\"\r\n2,plain\r\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("crlf.csv"), opts), ReadStatus::OK, "crlf open");
    CHECK_U64(reader.schema().size(), 2);
    CHECK_TYPE(reader.schema()[0].type, LogicalType::BIGINT, "crlf col a");
    CHECK_TYPE(reader.schema()[1].type, LogicalType::VARCHAR, "crlf col b");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "crlf row 1");
    if (row.size() == 2) {
      CHECK_CELL_INT(row[0], 1, "crlf row1 a");
      CHECK_CELL_STR(row[1], "x\ny", "crlf row1 b");
    }
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "crlf row 2");
    if (row.size() == 2) {
      CHECK_CELL_STR(row[1], "plain", "crlf row2 b");
    }
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::END, "crlf eof");
  }

  // Quoted empty string is an empty VARCHAR, unquoted empty is NULL.
  {
    write_fixture("empty_vs_null.csv", "a,b\n,\"\"\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("empty_vs_null.csv"), opts), ReadStatus::OK, "empty/null");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::VARCHAR, "all-null column type");
    CHECK(reader.schema()[0].nullable);
    CHECK_TYPE(reader.schema()[1].type, LogicalType::VARCHAR, "quoted empty column type");
    CHECK(!reader.schema()[1].nullable);
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "empty/null row");
    if (row.size() == 2) {
      CHECK_CELL_NULL(row[0], "unquoted empty");
      CHECK(!row[1].is_null);
      CHECK_STR(row[1].s, "", "quoted empty string");
    }
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::END, "empty/null eof");
  }

  // Duplicate header names: name, name__2, name__3, ... (also when name__2
  // already exists in the file).
  {
    write_fixture("dup_headers.csv", "id,id__2,id,name,id\n1,2,3,x,4\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("dup_headers.csv"), opts), ReadStatus::OK, "dup headers");
    static const char *kExpected[5] = {"id", "id__2", "id__3", "name", "id__4"};
    CHECK_U64(reader.schema().size(), 5);
    for (size_t j = 0; j < reader.schema().size() && j < 5; ++j) {
      CHECK_STR(reader.schema()[j].name, kExpected[j], "deduped header name");
    }
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "dup headers row");
    CHECK_U64(row.size(), 5);
  }

  // Missing / extra fields: error with the physical line number.
  {
    write_fixture("bad_fields.csv", "a,b\n1,2\n1,2,3\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("bad_fields.csv"), opts), ReadStatus::OK, "bad fields open");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "bad fields row 1");
    if (row.size() == 2) {
      CHECK_CELL_INT(row[0], 1, "bad fields row1 a");
      CHECK_CELL_INT(row[1], 2, "bad fields row1 b");
    }
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::ERROR, "bad fields row 2");
    CHECK(err.find("line 3") != std::string::npos);
    CHECK(err.find("expected 2 fields but found 3") != std::string::npos);
  }
  {
    write_fixture("short_fields.csv", "a,b,c\n1,2\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("short_fields.csv"), opts), ReadStatus::OK, "short open");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::ERROR, "short row");
    CHECK(err.find("line 2") != std::string::npos);
    CHECK(err.find("expected 3 fields but found 2") != std::string::npos);
  }

  // Quoted delimiter / newline inside a field.
  {
    write_fixture("quoted.csv", "a,b\n\"x,y\",2\n\"m\nn\",3\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("quoted.csv"), opts), ReadStatus::OK, "quoted open");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "quoted row 1");
    if (row.size() == 2) {
      CHECK_CELL_STR(row[0], "x,y", "quoted comma");
      CHECK_CELL_INT(row[1], 2, "quoted row1 b");
    }
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "quoted row 2");
    if (row.size() == 2) {
      CHECK_CELL_STR(row[0], "m\nn", "quoted newline");
    }
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::END, "quoted eof");
  }

  // Custom delimiter and escape character.
  {
    write_fixture("escape.csv", "a;b\n\"x\\\"y\";7\n");
    CsvFileReader reader;
    FileOptions opts;
    opts.delimiter = ';';
    opts.escape = '\\';
    CHECK_STATUS(reader.open(temp_path("escape.csv"), opts), ReadStatus::OK, "escape open");
    CHECK_U64(reader.schema().size(), 2);
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "escape row");
    if (row.size() == 2) {
      CHECK_CELL_STR(row[0], "x\"y", "escaped quote");
      CHECK_CELL_INT(row[1], 7, "escape row b");
    }
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::END, "escape eof");
  }

  // Malformed record: a quote inside an unquoted field is rejected with a line
  // number instead of silently mangling the data.
  {
    write_fixture("stray_quote.csv", "a,b\nx\"y,2\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("stray_quote.csv"), opts), ReadStatus::OK, "stray quote open");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::ERROR, "stray quote row");
    CHECK(err.find("line 2") != std::string::npos);
    CHECK(err.find("quote") != std::string::npos);
  }

  // Unterminated quote.
  {
    write_fixture("unterminated.csv", "a\n\"oops\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("unterminated.csv"), opts), ReadStatus::OK,
                 "unterminated open");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::ERROR, "unterminated row");
    CHECK(err.find("unterminated quoted field") != std::string::npos);
  }

  // Strict numeric / boolean / date / datetime recognition.
  {
    write_fixture("strict.csv", "v\n12abc\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("strict.csv"), opts), ReadStatus::OK, "12abc open");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::VARCHAR, "\"12abc\" is not BIGINT");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "12abc row");
  }
  {
    write_fixture("strict_date.csv", "v\n2026-02-30\n2026-02-28\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("strict_date.csv"), opts), ReadStatus::OK, "bad date open");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::VARCHAR, "\"2026-02-30\" is not DATE");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "bad date row");
  }
  {
    write_fixture("leap.csv", "v\n2024-02-29\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("leap.csv"), opts), ReadStatus::OK, "leap open");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::DATE, "2024-02-29 is DATE");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "leap row");
    CHECK_CELL_STR(row[0], "2024-02-29", "leap value");
  }
  {
    write_fixture("bad_time.csv", "v\n2026-08-01 25:00:00\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("bad_time.csv"), opts), ReadStatus::OK, "bad time open");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::VARCHAR, "hour 25 is not DATETIME");
  }
  {
    write_fixture("bool.csv", "v\nTRUE\nFalse\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("bool.csv"), opts), ReadStatus::OK, "bool open");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::BOOLEAN, "TRUE/False is BOOLEAN");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "bool row 1");
    CHECK_CELL_BOOL(row[0], true, "TRUE");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "bool row 2");
    CHECK_CELL_BOOL(row[0], false, "False");
  }

  // int64 boundary: max int64 stays BIGINT; one more overflows to DOUBLE.
  {
    write_fixture("overflow.csv", "v\n9223372036854775807\n9223372036854775808\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("overflow.csv"), opts), ReadStatus::OK, "overflow open");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::DOUBLE, "int64 overflow -> DOUBLE");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "overflow row 1");
    CHECK_CELL_DOUBLE(row[0], 9223372036854775807.0, "max int64 as double");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "overflow row 2");
    CHECK_CELL_DOUBLE(row[0], 9223372036854775808.0, "int64+1 as double");
  }
  {
    write_fixture("int64_min.csv", "v\n-9223372036854775808\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("int64_min.csv"), opts), ReadStatus::OK, "int64 min open");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::BIGINT, "INT64_MIN is BIGINT");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "int64 min row");
    CHECK_CELL_INT(row[0], INT64_MIN, "INT64_MIN value");
  }

  // header = false: generated column0..columnN names.
  {
    write_fixture("no_header.csv", "1,2\n3,4\n");
    CsvFileReader reader;
    FileOptions opts;
    opts.header = false;
    CHECK_STATUS(reader.open(temp_path("no_header.csv"), opts), ReadStatus::OK, "no header open");
    CHECK_U64(reader.schema().size(), 2);
    CHECK_STR(reader.schema()[0].name, "column0", "generated name 0");
    CHECK_STR(reader.schema()[1].name, "column1", "generated name 1");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::BIGINT, "no header col type");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "no header row 1");
    if (row.size() == 2) {
      CHECK_CELL_INT(row[0], 1, "no header row1 a");
      CHECK_CELL_INT(row[1], 2, "no header row1 b");
    }
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "no header row 2");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::END, "no header eof");
  }

  // UTF-8 BOM is stripped; UTF-8 payload bytes survive.
  {
    write_fixture("bom.csv", "\xEF\xBB\xBF""a,b\n1,\xE4\xBD\xA0\xE5\xA5\xBD\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("bom.csv"), opts), ReadStatus::OK, "bom open");
    CHECK_STR(reader.schema()[0].name, "a", "BOM stripped from first header");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::BIGINT, "bom col a");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "bom row");
    if (row.size() == 2) {
      CHECK_CELL_STR(row[1], "\xE4\xBD\xA0\xE5\xA5\xBD", "UTF-8 payload");
    }
  }

  // Huge single field (~1 MiB) must stream without truncation.
  {
    std::string huge(1024 * 1024, 'x');
    write_fixture("huge.csv", "v\n" + huge + "\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("huge.csv"), opts), ReadStatus::OK, "huge open");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::VARCHAR, "huge column type");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "huge row");
    if (row.size() == 1) {
      CHECK_U64(row[0].s.size(), huge.size());
    }
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::END, "huge eof");
  }

  // Blank lines are skipped; a single-column line with only spaces is data.
  {
    write_fixture("blank_lines.csv", "a\n\n  \n1\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("blank_lines.csv"), opts), ReadStatus::OK, "blank open");
    CHECK_U64(reader.schema().size(), 1);
    // "  " is text and "1" is numeric -> the column widens to VARCHAR.
    CHECK_TYPE(reader.schema()[0].type, LogicalType::VARCHAR, "space text + number -> VARCHAR");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "blank row 1");
    if (row.size() == 1) {
      CHECK_CELL_STR(row[0], "  ", "spaces-only field is data");
    }
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "blank row 2");
    if (row.size() == 1) {
      CHECK_CELL_STR(row[0], "1", "numeric value widened to text");
    }
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::END, "blank eof");
  }

  // Trailing padding after a closing quote is tolerated.
  {
    write_fixture("pad.csv", "a,b\n\"x\"  ,2\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("pad.csv"), opts), ReadStatus::OK, "pad open");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "pad row");
    if (row.size() == 2) {
      CHECK_CELL_STR(row[0], "x", "padded quoted field");
    }
  }

  // Empty unquoted field in a numeric column is NULL, not an error.
  {
    write_fixture("null_numeric.csv", "a,b\n1,\n2,3\n");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("null_numeric.csv"), opts), ReadStatus::OK, "null num open");
    CHECK_TYPE(reader.schema()[1].type, LogicalType::BIGINT, "nullable numeric type");
    CHECK(reader.schema()[1].nullable);
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "null num row 1");
    if (row.size() == 2) {
      CHECK_CELL_NULL(row[1], "empty numeric field");
    }
  }

  // File without a trailing newline.
  {
    write_fixture("no_trailing_nl.csv", "a,b\n1,2");
    CsvFileReader reader;
    FileOptions opts;
    CHECK_STATUS(reader.open(temp_path("no_trailing_nl.csv"), opts), ReadStatus::OK, "no trail open");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "no trail row");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::END, "no trail eof");
  }

  // Invalid / unsupported options are reported from open().
  {
    CsvFileReader reader;
    FileOptions opts;
    opts.sample_size = 10;
    CHECK_STATUS(reader.open(temp_path("no_trailing_nl.csv"), opts), ReadStatus::ERROR,
                 "sample_size");
    CHECK(reader.last_error().find("sample_size") != std::string::npos);
    opts.sample_size = -1;
    opts.quote = opts.delimiter;
    CHECK_STATUS(reader.open(temp_path("no_trailing_nl.csv"), opts), ReadStatus::ERROR,
                 "quote == delimiter");
  }
}

// ---------------------------------------------------------------------------
// JSONL robustness / union schema coverage
// ---------------------------------------------------------------------------

void test_jsonl_edge_cases() {
  std::printf("[test] JSONL edge cases\n");
  std::vector<Cell> row;
  std::string err;
  FileOptions opts;
  opts.newline_delimited = true;

  // Missing file.
  {
    JsonlFileReader reader;
    CHECK_STATUS(reader.open(temp_path("missing.jsonl"), opts), ReadStatus::ERROR, "missing jsonl");
    CHECK(reader.last_error().find("cannot open file") != std::string::npos);
  }

  // Empty file.
  {
    write_fixture("empty.jsonl", "");
    JsonlFileReader reader;
    CHECK_STATUS(reader.open(temp_path("empty.jsonl"), opts), ReadStatus::OK, "empty jsonl open");
    CHECK_U64(reader.schema().size(), 0);
    CHECK(reader.empty());
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::END, "empty jsonl row");
  }

  // Union schema across lines, missing -> NULL, first-appearance column order.
  {
    write_fixture("union.jsonl", "{\"a\":1}\n{\"b\":\"x\"}\n");
    JsonlFileReader reader;
    CHECK_STATUS(reader.open(temp_path("union.jsonl"), opts), ReadStatus::OK, "union open");
    CHECK_U64(reader.schema().size(), 2);
    CHECK_STR(reader.schema()[0].name, "a", "union col 0");
    CHECK_STR(reader.schema()[1].name, "b", "union col 1");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::BIGINT, "union col a type");
    CHECK_TYPE(reader.schema()[1].type, LogicalType::VARCHAR, "union col b type");
    CHECK(reader.schema()[0].nullable);
    CHECK(reader.schema()[1].nullable);
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "union row 1");
    if (row.size() == 2) {
      CHECK_CELL_INT(row[0], 1, "union row1 a");
      CHECK_CELL_NULL(row[1], "union row1 b");
    }
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "union row 2");
    if (row.size() == 2) {
      CHECK_CELL_NULL(row[0], "union row2 a");
      CHECK_CELL_STR(row[1], "x", "union row2 b");
    }
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::END, "union eof");
  }

  // Keys on the same line keep their position order.
  {
    write_fixture("order.jsonl", "{\"b\":1,\"a\":2}\n");
    JsonlFileReader reader;
    CHECK_STATUS(reader.open(temp_path("order.jsonl"), opts), ReadStatus::OK, "order open");
    CHECK_U64(reader.schema().size(), 2);
    CHECK_STR(reader.schema()[0].name, "b", "key order 0");
    CHECK_STR(reader.schema()[1].name, "a", "key order 1");
  }

  // boolean + integer conflict -> VARCHAR.
  {
    write_fixture("bool_int.jsonl", "{\"x\":true}\n{\"x\":1}\n");
    JsonlFileReader reader;
    CHECK_STATUS(reader.open(temp_path("bool_int.jsonl"), opts), ReadStatus::OK, "bool/int open");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::VARCHAR, "bool+int -> VARCHAR");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "bool/int row 1");
    CHECK_CELL_STR(row[0], "true", "bool/int row1 text");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "bool/int row 2");
    CHECK_CELL_STR(row[0], "1", "bool/int row2 text");
  }

  // number + non-numeric string -> VARCHAR.
  {
    write_fixture("num_str.jsonl", "{\"x\":1}\n{\"x\":\"abc\"}\n");
    JsonlFileReader reader;
    CHECK_STATUS(reader.open(temp_path("num_str.jsonl"), opts), ReadStatus::OK, "num/str open");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::VARCHAR, "number+text -> VARCHAR");
  }

  // number + numeric string -> DOUBLE (all values are numeric).
  {
    write_fixture("num_numstr.jsonl", "{\"x\":1}\n{\"x\":\"2.5\"}\n");
    JsonlFileReader reader;
    CHECK_STATUS(reader.open(temp_path("num_numstr.jsonl"), opts), ReadStatus::OK, "num/numstr");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::DOUBLE, "all numeric -> DOUBLE");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "num/numstr row 1");
    CHECK_CELL_DOUBLE(row[0], 1.0, "numeric string widened row1");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "num/numstr row 2");
    CHECK_CELL_DOUBLE(row[0], 2.5, "numeric string widened row2");
  }

  // Date / datetime strings, including the ISO 'T' separator.
  {
    write_fixture("dates.jsonl", "{\"d\":\"2026-08-02\",\"t\":\"2026-08-03T12:13:14\"}\n");
    JsonlFileReader reader;
    CHECK_STATUS(reader.open(temp_path("dates.jsonl"), opts), ReadStatus::OK, "dates open");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::DATE, "date string -> DATE");
    CHECK_TYPE(reader.schema()[1].type, LogicalType::DATETIME, "T datetime -> DATETIME");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "dates row");
    if (row.size() == 2) {
      CHECK_CELL_STR(row[0], "2026-08-02", "date value");
      CHECK_CELL_STR(row[1], "2026-08-03 12:13:14", "normalized datetime value");
    }
  }

  // Explicit null marks nullable, does not change the type.
  {
    write_fixture("nulls.jsonl", "{\"x\":1}\n{\"x\":null}\n");
    JsonlFileReader reader;
    CHECK_STATUS(reader.open(temp_path("nulls.jsonl"), opts), ReadStatus::OK, "nulls open");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::BIGINT, "null does not change type");
    CHECK(reader.schema()[0].nullable);
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "nulls row 1");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "nulls row 2");
    CHECK_CELL_NULL(row[0], "explicit null");
  }

  // int64 overflow in JSON -> DOUBLE.
  {
    write_fixture("overflow.jsonl", "{\"x\":9223372036854775807}\n{\"x\":9223372036854775808}\n");
    JsonlFileReader reader;
    CHECK_STATUS(reader.open(temp_path("overflow.jsonl"), opts), ReadStatus::OK, "json overflow");
    CHECK_TYPE(reader.schema()[0].type, LogicalType::DOUBLE, "json int64 overflow -> DOUBLE");
  }

  // \u escapes and surrogate pairs decode to UTF-8.
  {
    write_fixture("unicode.jsonl",
                  "{\"s\":\"caf\\u00e9\",\"e\":\"\\uD83D\\uDE00\"}\n");
    JsonlFileReader reader;
    CHECK_STATUS(reader.open(temp_path("unicode.jsonl"), opts), ReadStatus::OK, "unicode open");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "unicode row");
    if (row.size() == 2) {
      CHECK_CELL_STR(row[0], "caf\xC3\xA9", "\\u00e9 -> UTF-8");
      CHECK_CELL_STR(row[1], "\xF0\x9F\x98\x80", "surrogate pair -> UTF-8 emoji");
    }
  }

  // CRLF line endings and blank lines.
  {
    write_fixture("crlf.jsonl", "{\"a\":1}\r\n\r\n{\"a\":2}\r\n");
    JsonlFileReader reader;
    CHECK_STATUS(reader.open(temp_path("crlf.jsonl"), opts), ReadStatus::OK, "crlf jsonl open");
    CHECK_U64(reader.schema().size(), 1);
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "crlf jsonl row 1");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "crlf jsonl row 2");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::END, "crlf jsonl eof");
  }

  // Nested object: row before it is readable, error names line and column.
  {
    write_fixture("nested.jsonl", "{\"a\":1}\n{\"a\":{\"b\":2}}\n");
    JsonlFileReader reader;
    CHECK_STATUS(reader.open(temp_path("nested.jsonl"), opts), ReadStatus::OK, "nested open");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "nested row 1");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::ERROR, "nested row 2");
    std::printf("  nested error: %s\n", err.c_str());
    CHECK(err.find("line 2") != std::string::npos);
    CHECK(err.find("\"a\"") != std::string::npos);
    CHECK(err.find("nested object/array") != std::string::npos);
  }

  // Array value is rejected the same way.
  {
    write_fixture("array.jsonl", "{\"a\":1}\n{\"a\":[1,2]}\n");
    JsonlFileReader reader;
    CHECK_STATUS(reader.open(temp_path("array.jsonl"), opts), ReadStatus::OK, "array open");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::OK, "array row 1");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::ERROR, "array row 2");
    CHECK(err.find("line 2") != std::string::npos);
    CHECK(err.find("nested object/array") != std::string::npos);
  }

  // Malformed JSON, duplicate keys, trailing garbage.
  {
    write_fixture("malformed.jsonl", "{\"a\":}\n");
    JsonlFileReader reader;
    CHECK_STATUS(reader.open(temp_path("malformed.jsonl"), opts), ReadStatus::OK, "malformed open");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::ERROR, "malformed row");
    CHECK(err.find("line 1") != std::string::npos);
  }
  {
    write_fixture("dup_key.jsonl", "{\"a\":1,\"a\":2}\n");
    JsonlFileReader reader;
    CHECK_STATUS(reader.open(temp_path("dup_key.jsonl"), opts), ReadStatus::OK, "dup key open");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::ERROR, "dup key row");
    CHECK(err.find("duplicate key") != std::string::npos);
    CHECK(err.find("line 1") != std::string::npos);
  }
  {
    write_fixture("trailing.jsonl", "{} junk\n");
    JsonlFileReader reader;
    CHECK_STATUS(reader.open(temp_path("trailing.jsonl"), opts), ReadStatus::OK, "trailing open");
    CHECK_STATUS(reader.next_row(row, err), ReadStatus::ERROR, "trailing row");
    CHECK(err.find("trailing characters") != std::string::npos);
  }

  // Unsupported options.
  {
    JsonlFileReader reader;
    FileOptions bad;
    bad.newline_delimited = false;
    CHECK_STATUS(reader.open(temp_path("union.jsonl"), bad), ReadStatus::ERROR, "format opt");
    CHECK(reader.last_error().find("newline_delimited") != std::string::npos);
    bad.newline_delimited = true;
    bad.sample_size = 5;
    CHECK_STATUS(reader.open(temp_path("union.jsonl"), bad), ReadStatus::ERROR, "sample opt");
  }

  // The factory-created reader behaves identically.
  {
    std::unique_ptr<IFileReader> generic = create_jsonl_reader();
    CHECK(generic.get() != nullptr);
    CHECK_STATUS(generic->open(temp_path("union.jsonl"), opts), ReadStatus::OK, "jsonl factory");
    CHECK_U64(generic->schema().size(), 2);
    CHECK_STATUS(generic->next_row(row, err), ReadStatus::OK, "jsonl factory row");
  }
}

// ---------------------------------------------------------------------------
// Shared inference helpers
// ---------------------------------------------------------------------------

void test_infer_helpers() {
  std::printf("[test] shared inference helpers\n");
  int64_t int_value = 0;
  double double_value = 0.0;
  bool bool_value = false;
  std::string normalized;

  CHECK(filesql_is_integer_text("0", int_value));
  CHECK_I64(int_value, 0);
  CHECK(filesql_is_integer_text("-42", int_value));
  CHECK_I64(int_value, -42);
  CHECK(filesql_is_integer_text("+7", int_value));
  CHECK_I64(int_value, 7);
  CHECK(filesql_is_integer_text("007", int_value));
  CHECK_I64(int_value, 7);
  CHECK(filesql_is_integer_text("9223372036854775807", int_value));
  CHECK_I64(int_value, INT64_MAX);
  CHECK(filesql_is_integer_text("-9223372036854775808", int_value));
  CHECK_I64(int_value, INT64_MIN);
  CHECK(!filesql_is_integer_text("9223372036854775808", int_value));
  CHECK(!filesql_is_integer_text("-9223372036854775809", int_value));
  CHECK(!filesql_is_integer_text("12abc", int_value));
  CHECK(!filesql_is_integer_text(" 1", int_value));
  CHECK(!filesql_is_integer_text("1 ", int_value));
  CHECK(!filesql_is_integer_text("", int_value));
  CHECK(!filesql_is_integer_text("+", int_value));
  CHECK(!filesql_is_integer_text("1.0", int_value));

  CHECK(filesql_is_double_text("98.5", double_value));
  CHECK_DOUBLE(double_value, 98.5, "double 98.5");
  CHECK(filesql_is_double_text("1e3", double_value));
  CHECK_DOUBLE(double_value, 1000.0, "double 1e3");
  CHECK(filesql_is_double_text(".5", double_value));
  CHECK_DOUBLE(double_value, 0.5, "double .5");
  CHECK(filesql_is_double_text("-2.", double_value));
  CHECK_DOUBLE(double_value, -2.0, "double -2.");
  CHECK(!filesql_is_double_text("nan", double_value));
  CHECK(!filesql_is_double_text("inf", double_value));
  CHECK(!filesql_is_double_text("0x10", double_value));
  CHECK(!filesql_is_double_text("1e", double_value));
  CHECK(!filesql_is_double_text("1.2.3", double_value));
  CHECK(!filesql_is_double_text("1e999", double_value));

  CHECK(filesql_is_boolean_text("true", bool_value));
  CHECK(bool_value);
  CHECK(filesql_is_boolean_text("FALSE", bool_value));
  CHECK(!bool_value);
  CHECK(!filesql_is_boolean_text("yes", bool_value));

  CHECK(filesql_is_valid_date_ymd(2024, 2, 29));
  CHECK(!filesql_is_valid_date_ymd(2026, 2, 29));
  CHECK(!filesql_is_valid_date_ymd(2026, 2, 30));
  CHECK(!filesql_is_valid_date_ymd(2026, 13, 1));
  CHECK(!filesql_is_valid_date_ymd(2026, 0, 1));
  CHECK(!filesql_is_valid_date_ymd(2026, 4, 31));
  CHECK(filesql_is_valid_date_ymd(2100, 2, 28));
  CHECK(!filesql_is_valid_date_ymd(2100, 2, 29));
  CHECK(filesql_is_valid_date_ymd(2000, 2, 29));

  CHECK(filesql_is_date_text("2026-08-01"));
  CHECK(!filesql_is_date_text("2026-8-1"));
  CHECK(!filesql_is_date_text("2026/08/01"));
  CHECK(!filesql_is_date_text("2026-08-01 "));
  CHECK(!filesql_is_date_text(""));

  CHECK(filesql_is_datetime_text("2026-08-01 10:11:12", normalized));
  CHECK_STR(normalized, "2026-08-01 10:11:12", "datetime space");
  CHECK(filesql_is_datetime_text("2026-08-03T12:13:14", normalized));
  CHECK_STR(normalized, "2026-08-03 12:13:14", "datetime T separator");
  CHECK(!filesql_is_datetime_text("2026-08-03 12:13:14.5", normalized));
  CHECK(!filesql_is_datetime_text("2026-08-03 24:00:00", normalized));
  CHECK(!filesql_is_datetime_text("2026-08-03", normalized));

  // Widening rules.
  CHECK_TYPE(widen_type(LogicalType::NULLTYPE, LogicalType::BIGINT), LogicalType::BIGINT,
             "NULL+BIGINT");
  CHECK_TYPE(widen_type(LogicalType::BIGINT, LogicalType::DOUBLE), LogicalType::DOUBLE,
             "BIGINT+DOUBLE");
  CHECK_TYPE(widen_type(LogicalType::DOUBLE, LogicalType::BIGINT), LogicalType::DOUBLE,
             "DOUBLE+BIGINT");
  CHECK_TYPE(widen_type(LogicalType::DATE, LogicalType::DATETIME), LogicalType::DATETIME,
             "DATE+DATETIME");
  CHECK_TYPE(widen_type(LogicalType::BOOLEAN, LogicalType::BIGINT), LogicalType::VARCHAR,
             "BOOLEAN+BIGINT");
  CHECK_TYPE(widen_type(LogicalType::VARCHAR, LogicalType::DATE), LogicalType::VARCHAR,
             "VARCHAR+DATE");
  CHECK_TYPE(widen_type(LogicalType::DATE, LogicalType::BIGINT), LogicalType::VARCHAR,
             "DATE+BIGINT");

  // All-NULL column becomes a nullable VARCHAR.
  TypeInferencer infer;
  infer.add_null();
  CHECK_TYPE(infer.type(), LogicalType::VARCHAR, "all-null column type");
  CHECK(infer.nullable());
  CHECK(!infer.has_value());

  // classify_csv_field distinguishes quoted and unquoted empties.
  CHECK(classify_csv_field("", false).kind == TokenKind::NULL_VALUE);
  const InferredToken quoted_empty = classify_csv_field("", true);
  CHECK(quoted_empty.kind == TokenKind::TEXT);
  CHECK_STR(quoted_empty.text, "", "quoted empty text");
  const InferredToken date_token = classify_csv_field("2026-08-01", true);
  CHECK(date_token.kind == TokenKind::DATE);
  CHECK_STR(date_token.text, "2026-08-01", "date token text");
  const InferredToken text_token = classify_csv_field("12abc", true);
  CHECK(text_token.kind == TokenKind::TEXT);
  CHECK_STR(text_token.raw, "12abc", "text token raw");

  // token_to_cell widening: DATE value into a DATETIME column -> midnight.
  Cell cell;
  std::string cell_err;
  const InferredToken date2 = classify_csv_field("2024-02-29", true);
  CHECK(token_to_cell(date2, LogicalType::DATETIME, cell, cell_err));
  CHECK_STR(cell.s, "2024-02-29 00:00:00", "date widened to datetime");
  const InferredToken integer = classify_csv_field("7", true);
  CHECK(token_to_cell(integer, LogicalType::DOUBLE, cell, cell_err));
  CHECK_DOUBLE(cell.d, 7.0, "integer widened to double");
  CHECK(token_to_cell(integer, LogicalType::VARCHAR, cell, cell_err));
  CHECK_STR(cell.s, "7", "integer as text");
  const InferredToken null_token;  // NULL_VALUE
  CHECK(token_to_cell(null_token, LogicalType::BIGINT, cell, cell_err));
  CHECK(cell.is_null);
  CHECK(!token_to_cell(classify_csv_field("abc", true), LogicalType::BIGINT, cell, cell_err));

  // The contract's type-name utility is implemented by this module.
  CHECK_STR(logical_type_name(LogicalType::BIGINT), "BIGINT", "logical_type_name BIGINT");
  CHECK_STR(logical_type_name(LogicalType::NULLTYPE), "NULL", "logical_type_name NULL");
}

}  // namespace

int main(int argc, char **argv) {
  // Unbuffered output so that a crash still shows how far the test got.
  std::setvbuf(stdout, nullptr, _IONBF, 0);
  const std::string data_dir = find_data_dir(argc, argv);
  std::printf("file_sql reader unit test\n");
  std::printf("  data dir: %s\n", data_dir.empty() ? "<not found>" : data_dir.c_str());

  // Temporary fixtures next to this source file: guaranteed writable because it
  // is the same tree the build happens in.  Removed again at the end.
  g_temp_dir = repo_root_from_source_path();
  if (!g_temp_dir.empty()) {
    g_temp_dir += "/src/sql/engine/file_scan/test/_tmp_reader_tests";
  } else {
    g_temp_dir = "test/_tmp_reader_tests";
  }
  std::error_code ec;
  fs::remove_all(g_temp_dir, ec);
  ec.clear();
  fs::create_directories(g_temp_dir, ec);
  if (ec) {
    std::printf("FATAL: cannot create temp dir %s: %s\n", g_temp_dir.c_str(),
                ec.message().c_str());
    return 1;
  }

  if (!data_dir.empty()) {
    test_basic_csv(data_dir);
    test_basic_jsonl(data_dir);
  } else {
    std::printf("FATAL: cannot locate tools/deploy/mysql_test/test_suite/file_sql/data.\n");
    std::printf("       Pass the directory as the first argument.\n");
    ++g_failures;
  }
  test_csv_edge_cases();
  test_jsonl_edge_cases();
  test_infer_helpers();

  fs::remove_all(g_temp_dir, ec);

  std::printf("\n%d checks, %d failure(s)\n", g_checks, g_failures);
  if (g_failures == 0) {
    std::printf("ALL TESTS PASSED\n");
    return 0;
  }
  std::printf("TESTS FAILED\n");
  return 1;
}
