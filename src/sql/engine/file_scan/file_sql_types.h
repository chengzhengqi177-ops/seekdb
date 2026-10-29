#pragma once
/**
 * Self-contained value/schema types shared by the file-as-database readers.
 *
 * These types deliberately use only the C++ standard library so that the
 * readers can be compiled and unit-tested independently of the seekdb
 * build (see tools/filesql/README.md).
 */

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace oceanbase {
namespace sql {
namespace filesql {

enum class FileFormat {
  CSV = 0,
  JSONL = 1,
  PARQUET = 2,
};

// Logical column types required by the task specification (BIGINT, DOUBLE,
// BOOLEAN, VARCHAR, DATE, DATETIME, NULL).
enum class LogicalType {
  BIGINT = 0,
  DOUBLE,
  BOOLEAN,
  VARCHAR,
  DATE,
  DATETIME,
  NULLTYPE,
};

struct ColumnSchema {
  std::string name;
  LogicalType type = LogicalType::VARCHAR;
  bool nullable = true;
};

struct FileOptions {
  // CSV options.
  bool header = true;
  char delimiter = ',';
  char quote = '"';
  char escape = '"';
  // -1 means "scan the whole file", only -1 is accepted by this task.
  int64_t sample_size = -1;
  // JSONL: only "newline_delimited" / "ndjson" is accepted.
  bool newline_delimited = true;
};

// One cell. Numbers are carried in their widest form; DATE is carried as
// "YYYY-MM-DD", DATETIME as "YYYY-MM-DD HH:MM:SS", VARCHAR as raw text,
// BOOLEAN as 0/1 in `i`.
struct Cell {
  bool is_null = true;
  int64_t i = 0;
  double d = 0;
  std::string s;
};

inline Cell make_null() { return Cell(); }
inline Cell make_int(int64_t v) { Cell c; c.is_null = false; c.i = v; return c; }
inline Cell make_double(double v) { Cell c; c.is_null = false; c.d = v; return c; }
inline Cell make_bool(bool v) { Cell c; c.is_null = false; c.i = v ? 1 : 0; return c; }
inline Cell make_string(const std::string &v) { Cell c; c.is_null = false; c.s = v; return c; }

// Reader status codes. The numeric values are mapped to seekdb error codes by
// the integration layer.
enum class ReadStatus {
  OK = 0,      // a row was produced
  END = 1,     // end of file
  ERROR = 2,   // malformed input, see error message
};

class IFileReader {
public:
  virtual ~IFileReader() {}
  // Opens `path` and (for CSV/JSONL) performs schema inference. Returns
  // ReadStatus::OK on success.
  virtual ReadStatus open(const std::string &path, const FileOptions &opts) = 0;
  virtual const std::vector<ColumnSchema> &schema() const = 0;
  // Reads the next row into `row` (resized to schema().size()). Rows are
  // produced lazily/streaming; the reader must not materialize the file.
  virtual ReadStatus next_row(std::vector<Cell> &row, std::string &err) = 0;
  virtual void close() = 0;
};

std::unique_ptr<IFileReader> create_csv_reader();
std::unique_ptr<IFileReader> create_jsonl_reader();
std::unique_ptr<IFileReader> create_parquet_reader();

// Utility used by the SQL layer to map a format name.
bool parse_format_name(const std::string &name, FileFormat &fmt);
const char *logical_type_name(LogicalType type);

}  // namespace filesql
}  // namespace sql
}  // namespace oceanbase
