#pragma once
/**
 * Descriptor of a file-backed table ("file as database").
 *
 * A file table is a FUNCTION_TABLE TableItem whose file_table_def_ points at a
 * FileTableDef allocated from the statement allocator. The descriptor carries
 * everything the resolver, the optimizer and the executor need:
 *   - which reader to use (csv / jsonl / parquet / glob),
 *   - the file path and reader options,
 *   - the inferred column schema,
 *   - the file identity used for change detection between resolve and execute.
 */

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace sql
{

static const int64_t OB_MAX_FILE_SQL_COLUMNS = 256;

enum FileSqlFormat : int64_t
{
  FILE_SQL_FORMAT_CSV = 0,
  FILE_SQL_FORMAT_JSONL = 1,
  FILE_SQL_FORMAT_PARQUET = 2,
  FILE_SQL_FORMAT_GLOB = 3,
};

enum FileSqlLogicalType : int64_t
{
  FILE_SQL_TYPE_BIGINT = 0,
  FILE_SQL_TYPE_DOUBLE = 1,
  FILE_SQL_TYPE_BOOLEAN = 2,
  FILE_SQL_TYPE_VARCHAR = 3,
  FILE_SQL_TYPE_DATE = 4,
  FILE_SQL_TYPE_DATETIME = 5,
  FILE_SQL_TYPE_NULL = 6,
};

struct FileSqlColumnDef
{
  common::ObString name_;
  int64_t logical_type_;
  bool nullable_;
  FileSqlColumnDef() : name_(), logical_type_(FILE_SQL_TYPE_VARCHAR), nullable_(true) {}
};

struct FileTableDef
{
  int64_t format_;
  common::ObString path_;
  bool has_header_;
  char delimiter_;
  char quote_;
  char escape_;
  int64_t sample_size_;
  bool newline_delimited_;
  int64_t file_size_;
  int64_t file_mtime_;
  int64_t column_count_;
  FileSqlColumnDef columns_[OB_MAX_FILE_SQL_COLUMNS];

  FileTableDef()
    : format_(FILE_SQL_FORMAT_CSV),
      path_(),
      has_header_(true),
      delimiter_(','),
      quote_('"'),
      escape_('"'),
      sample_size_(-1),
      newline_delimited_(true),
      file_size_(-1),
      file_mtime_(0),
      column_count_(0)
  {}

  bool is_glob() const { return FILE_SQL_FORMAT_GLOB == format_; }
};

} // namespace sql
} // namespace oceanbase
