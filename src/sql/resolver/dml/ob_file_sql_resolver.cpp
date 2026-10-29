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

#define USING_LOG_PREFIX SQL_RESV

#include <sys/stat.h>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/engine/file_scan/file_sql_def.h"
#include "sql/engine/file_scan/file_sql_types.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/charset/ob_charset_string_helper.h"
#include "common/object/ob_obj_type.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace sql
{

using namespace common;

namespace
{

// ---------------------------------------------------------------- option parse

struct FileSqlOptions
{
  int64_t format_;
  ObString path_;
  bool has_header_;
  char delimiter_;
  char quote_;
  char escape_;
  int64_t sample_size_;
  bool newline_delimited_;

  FileSqlOptions()
    : format_(FILE_SQL_FORMAT_CSV),
      path_(),
      has_header_(true),
      delimiter_(','),
      quote_('"'),
      escape_('"'),
      sample_size_(-1),
      newline_delimited_(true)
  {}
};

bool file_name_is(const ObString &name, const char *expect)
{
  const int64_t len = name.length();
  const int64_t expect_len = static_cast<int64_t>(strlen(expect));
  return len == expect_len && 0 == strncasecmp(name.ptr(), expect, static_cast<size_t>(len));
}

bool parse_logical_format(const ObString &func_name, int64_t &format)
{
  bool ok = true;
  if (file_name_is(func_name, "read_csv")) {
    format = FILE_SQL_FORMAT_CSV;
  } else if (file_name_is(func_name, "read_json") || file_name_is(func_name, "read_ndjson")) {
    format = FILE_SQL_FORMAT_JSONL;
  } else if (file_name_is(func_name, "read_parquet")) {
    format = FILE_SQL_FORMAT_PARQUET;
  } else if (file_name_is(func_name, "glob")) {
    format = FILE_SQL_FORMAT_GLOB;
  } else {
    ok = false;
  }
  return ok;
}

// Extract the identifier of a relation_factor parse node (db.tbl is rejected).
int get_relation_factor_name(const ParseNode &node, ObString &name)
{
  int ret = OB_SUCCESS;
  if (T_RELATION_FACTOR == node.type_) {
    if (OB_ISNULL(node.children_) || node.num_child_ < 1 || OB_ISNULL(node.children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid relation factor node", K(ret));
    } else if (OB_UNLIKELY(node.num_child_ > 1 && NULL != node.children_[1])) {
      // db.tbl(...) is not accepted for file table functions
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("file table function name must be a plain identifier", K(ret));
    } else {
      const ParseNode *ident = node.children_[0];
      name.assign_ptr(ident->str_value_, static_cast<int32_t>(ident->str_len_));
    }
  } else if (NULL != node.str_value_ && node.str_len_ > 0) {
    // NAME_OB terminal produced by the grammar for read_csv()/read_json()/...
    name.assign_ptr(node.str_value_, static_cast<int32_t>(node.str_len_));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table function name node", K(ret), "node_type", node.type_);
  }
  return ret;
}

// Normalize the user supplied path the same way SELECT ... INTO OUTFILE does,
// so that secure_file_priv prefix matching works with mixed separators on Windows.
int normalize_file_path(ObIAllocator &alloc, const ObString &in, ObString &out)
{
  int ret = OB_SUCCESS;
  char buf[PATH_MAX + 1] = { 0 };
  ObCStringHelper helper;
  char *real_path = NULL;
#ifdef _WIN32
  real_path = _fullpath(buf, helper.convert(in), PATH_MAX);
#else
  real_path = realpath(helper.convert(in), buf);
#endif
  if (OB_ISNULL(real_path)) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("normalize file path failed", K(ret), K(in));
  } else if (OB_FAIL(ob_write_string(alloc, ObString(real_path), out))) {
    LOG_WARN("copy normalized path failed", K(ret));
  }
  return ret;
}

// The fast parser may parameterize literals in the SQL text (they arrive as
// T_QUESTIONMARK nodes whose values live in the static param store), so option
// values are read from either the parse node or the param store.
int get_literal_obj(const ParseNode &node, const common::ParamStore *param_list, ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (T_QUESTIONMARK == node.type_) {
    if (OB_ISNULL(param_list) || node.value_ < 0 || node.value_ >= param_list->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid static parameter index", K(ret), K(node.value_));
    } else {
      obj = param_list->at(node.value_);
    }
  } else {
    switch (node.type_) {
      case T_VARCHAR:
      case T_CHAR:
        obj.set_string(ObVarcharType, ObString(static_cast<int32_t>(node.str_len_), node.str_value_));
        break;
      case T_INT:
        obj.set_int(node.value_);
        break;
      case T_NUMBER:
        obj.set_double(strtod(node.str_value_, NULL));
        break;
      case T_BOOL:
        obj.set_tinyint(0 != node.value_);
        break;
      case T_NULL:
        obj.set_null();
        break;
      default:
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("expect a literal argument", K(ret), "node_type", node.type_);
        break;
    }
  }
  return ret;
}

int value_to_bool(const ParseNode &node, const common::ParamStore *param_list, bool &v)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_FAIL(get_literal_obj(node, param_list, obj))) {
  } else if (obj.is_tinyint() || obj.is_int() || obj.is_uint64()) {
    v = (0 != obj.get_int());
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("expect a boolean literal", K(ret));
  }
  return ret;
}

int value_to_int(const ParseNode &node, const common::ParamStore *param_list, int64_t &v)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_FAIL(get_literal_obj(node, param_list, obj))) {
  } else if (obj.is_integer_type()) {
    v = obj.get_int();
  } else if (obj.is_double()) {
    v = static_cast<int64_t>(obj.get_double());
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("expect an integer literal", K(ret));
  }
  return ret;
}

int value_to_string(const ParseNode &node, const common::ParamStore *param_list, ObString &v)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_FAIL(get_literal_obj(node, param_list, obj))) {
  } else if (obj.is_varchar_or_char() || obj.is_string_type()) {
    v = obj.get_string();
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("expect a string literal", K(ret));
  }
  return ret;
}

int parse_one_option(const ParseNode &arg, const common::ParamStore *param_list, FileSqlOptions &opts)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.children_) || 2 != arg.num_child_ || OB_ISNULL(arg.children_[0])
      || OB_ISNULL(arg.children_[1])) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid named argument", K(ret));
  } else {
    const ParseNode *name_node = arg.children_[0];
    const ParseNode *value_node = arg.children_[1];
    if (T_COLUMN_REF != name_node->type_ || OB_ISNULL(name_node->children_)
        || OB_ISNULL(name_node->children_[2])) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("option name must be an identifier", K(ret));
    } else {
      const ParseNode *ident = name_node->children_[2];
      ObString opt_name(static_cast<int32_t>(ident->str_len_), ident->str_value_);
      if (file_name_is(opt_name, "header")) {
        OZ (value_to_bool(*value_node, param_list, opts.has_header_));
      } else if (file_name_is(opt_name, "sample_size")) {
        OZ (value_to_int(*value_node, param_list, opts.sample_size_));
      } else if (file_name_is(opt_name, "format")) {
        ObString fmt;
        OZ (value_to_string(*value_node, param_list, fmt));
        if (OB_SUCC(ret) && !file_name_is(fmt, "newline_delimited")
            && !file_name_is(fmt, "ndjson")) {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("only format='newline_delimited' is supported", K(ret));
        }
      } else if (file_name_is(opt_name, "delimiter")) {
        ObString delim;
        OZ (value_to_string(*value_node, param_list, delim));
        if (OB_SUCC(ret) && 1 != delim.length()) {
          ret = OB_ERR_PARSER_SYNTAX;
        } else if (OB_SUCC(ret)) {
          opts.delimiter_ = delim[0];
        }
      } else if (file_name_is(opt_name, "quote")) {
        ObString q;
        OZ (value_to_string(*value_node, param_list, q));
        if (OB_SUCC(ret) && 1 != q.length()) {
          ret = OB_ERR_PARSER_SYNTAX;
        } else if (OB_SUCC(ret)) {
          opts.quote_ = q[0];
        }
      } else if (file_name_is(opt_name, "escape")) {
        ObString esc;
        OZ (value_to_string(*value_node, param_list, esc));
        if (OB_SUCC(ret) && 1 != esc.length()) {
          ret = OB_ERR_PARSER_SYNTAX;
        } else if (OB_SUCC(ret)) {
          opts.escape_ = esc[0];
        }
      } else {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("unknown file table function option", K(ret), K(opt_name));
      }
    }
  }
  return ret;
}

// --------------------------------------------------------------- schema bridge

int build_obj_meta(const int64_t logical_type, ObObjMeta &meta, ObAccuracy &accuracy)
{
  int ret = OB_SUCCESS;
  switch (logical_type) {
    case FILE_SQL_TYPE_BIGINT:
      meta.set_int();
      accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][ObIntType];
      break;
    case FILE_SQL_TYPE_DOUBLE:
      meta.set_double();
      accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][ObDoubleType];
      break;
    case FILE_SQL_TYPE_BOOLEAN:
      meta.set_tinyint();
      accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][ObTinyIntType];
      break;
    case FILE_SQL_TYPE_VARCHAR:
      meta.set_varchar();
      meta.set_collation_level(CS_LEVEL_COERCIBLE);
      meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][ObVarcharType];
      accuracy.set_length(OB_MAX_VARCHAR_LENGTH);
      break;
    case FILE_SQL_TYPE_DATE:
      meta.set_date();
      accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][ObDateType];
      break;
    case FILE_SQL_TYPE_DATETIME:
      meta.set_datetime();
      accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][ObDateTimeType];
      break;
    case FILE_SQL_TYPE_NULL:
    default:
      meta.set_null();
      accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][ObNullType];
      break;
  }
  return ret;
}

int fill_file_table_def(ObIAllocator &alloc,
                        const FileSqlOptions &opts,
                        const int64_t file_size,
                        const int64_t file_mtime,
                        const std::vector<filesql::ColumnSchema> &columns,
                        FileTableDef *&def)
{
  int ret = OB_SUCCESS;
  FileTableDef *tmp = NULL;
  if (OB_ISNULL(tmp = OB_NEWx(FileTableDef, &alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate file table def failed", K(ret));
  } else {
    tmp->format_ = opts.format_;
    tmp->has_header_ = opts.has_header_;
    tmp->delimiter_ = opts.delimiter_;
    tmp->quote_ = opts.quote_;
    tmp->escape_ = opts.escape_;
    tmp->sample_size_ = opts.sample_size_;
    tmp->newline_delimited_ = opts.newline_delimited_;
    tmp->file_size_ = file_size;
    tmp->file_mtime_ = file_mtime;
    OZ (ob_write_string(alloc, opts.path_, tmp->path_));
    int64_t count = static_cast<int64_t>(columns.size());
    if (OB_SUCC(ret) && count > OB_MAX_FILE_SQL_COLUMNS) {
      count = OB_MAX_FILE_SQL_COLUMNS;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      OZ (ob_write_string(alloc, ObString(static_cast<int32_t>(columns[i].name.size()),
                                          columns[i].name.data()),
                          tmp->columns_[i].name_));
      tmp->columns_[i].logical_type_ = static_cast<int64_t>(columns[i].type);
      tmp->columns_[i].nullable_ = columns[i].nullable;
    }
    if (OB_SUCC(ret)) {
      tmp->column_count_ = count;
      def = tmp;
    }
  }
  return ret;
}

int do_file_stat(const char *path, int64_t &file_size, int64_t &file_mtime)
{
  int ret = OB_SUCCESS;
  struct stat st;
  if (0 != stat(path, &st)) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("stat file failed", K(ret), K(path));
  } else if (S_ISDIR(st.st_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expect a regular file", K(ret), K(path));
  } else {
    file_size = static_cast<int64_t>(st.st_size);
    file_mtime = static_cast<int64_t>(st.st_mtime);
  }
  return ret;
}

int check_file_sql_enabled(ObSQLSessionInfo *session_info, bool &enabled)
{
  int ret = OB_SUCCESS;
  enabled = false;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(session_info->get_enable_file_sql(enabled))) {
    LOG_WARN("get enable_file_sql failed", K(ret));
  }
  return ret;
}

} // namespace

int ObDMLResolver::resolve_file_table_item(const ParseNode &parse_tree, TableItem *&tbl_item)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *stmt = get_stmt();
  TableItem *item = NULL;
  FileSqlOptions opts;
  bool enabled = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(allocator_) || OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(T_FILE_TABLE_EXPRESSION != parse_tree.type_ || 3 != parse_tree.num_child_
                         || OB_ISNULL(parse_tree.children_) || OB_ISNULL(parse_tree.children_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid file table parse node", K(ret));
  }
  // feature switch: enable_file_sql defaults to OFF
  OZ (check_file_sql_enabled(session_info_, enabled));
  if (OB_SUCC(ret) && !enabled) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file as database");
  }
  ObString func_name;
  if (OB_SUCC(ret)) {
    OZ (get_relation_factor_name(*parse_tree.children_[0], func_name));
  }
  if (OB_SUCC(ret) && !parse_logical_format(func_name, opts.format_)) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("unknown file table function", K(ret), K(func_name));
  }
  // parse positional and named arguments
  const ParseNode *args = parse_tree.children_[1];
  bool has_path = false;
  if (OB_SUCC(ret) && NULL != args) {
    if (T_EXPR_LIST == args->type_ && NULL != args->children_) {
      // linked list: node type T_EXPR_LIST with linked children (left/right)
      const ParseNode *cur = args;
      while (OB_SUCC(ret) && NULL != cur) {
        const ParseNode *arg = NULL;
        const ParseNode *next = NULL;
        if (T_EXPR_LIST == cur->type_) {
          if (OB_ISNULL(cur->children_)) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            arg = cur->children_[0];
            next = cur->children_[1];
          }
        } else {
          arg = cur;
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(arg)) {
          if (T_OP_EQ == arg->type_) {
            OZ (parse_one_option(*arg, params_.param_list_, opts));
          } else if (!has_path) {
            OZ (value_to_string(*arg, params_.param_list_, opts.path_));
            has_path = true;
          } else {
            ret = OB_ERR_PARSER_SYNTAX;
            LOG_WARN("unexpected positional argument", K(ret));
          }
        }
        cur = next;
      }
    } else {
      // single argument list node (T_LINK_NODE / T_EXPR_LIST with 2 children)
      const ParseNode *cur = args;
      while (OB_SUCC(ret) && NULL != cur) {
        const ParseNode *arg = cur;
        const ParseNode *next = NULL;
        if (T_LINK_NODE == cur->type_ && NULL != cur->children_) {
          arg = cur->children_[0];
          next = cur->children_[1];
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(arg)) {
          if (T_OP_EQ == arg->type_) {
            OZ (parse_one_option(*arg, params_.param_list_, opts));
          } else if (!has_path) {
            OZ (value_to_string(*arg, params_.param_list_, opts.path_));
            has_path = true;
          } else {
            ret = OB_ERR_PARSER_SYNTAX;
            LOG_WARN("unexpected positional argument", K(ret));
          }
        }
        cur = next;
      }
    }
  }
  if (OB_SUCC(ret) && !has_path) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("file table function requires a file path", K(ret));
  }
  if (OB_SUCC(ret) && opts.sample_size_ != -1) {
    // only full-file schema inference is supported by this task
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sample_size other than -1");
  }
  // path must be absolute and inside secure_file_priv
  if (OB_SUCC(ret) && !opts.path_.empty() && !opts.path_.prefix_match("/")) {
#ifdef _WIN32
    // allow drive-letter paths such as D:/dir/file.csv
    if (!(opts.path_.length() >= 3 && opts.path_.ptr()[1] == ':')) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("file path must be absolute", K(ret), K(opts.path_));
    }
#else
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("file path must be absolute", K(ret), K(opts.path_));
#endif
  }
  if (OB_SUCC(ret)) {
    ObString normalized_path;
    OZ (normalize_file_path(*allocator_, opts.path_, normalized_path));
    OX (opts.path_ = normalized_path);
  }
  if (OB_SUCC(ret)) {
    ObString priv;
    OZ (session_info_->get_secure_file_priv(priv));
    if (OB_SUCC(ret) && OB_FAIL(ObResolverUtils::check_secure_path(priv, opts.path_))) {
      LOG_WARN("file path is not in secure_file_priv", K(ret), K(opts.path_));
    }
  }
  // infer schema and build descriptor
  FileTableDef *def = NULL;
  int64_t file_size = -1;
  int64_t file_mtime = 0;
  std::vector<filesql::ColumnSchema> columns;
  if (OB_SUCC(ret)) {
    if (FILE_SQL_FORMAT_GLOB == opts.format_) {
      filesql::ColumnSchema col;
      col.name = "file";
      col.type = filesql::LogicalType::VARCHAR;
      col.nullable = false;
      columns.push_back(col);
    } else {
      std::string path(opts.path_.ptr(), static_cast<size_t>(opts.path_.length()));
      int64_t expected_format = opts.format_;
      if (OB_FAIL(do_file_stat(path.c_str(), file_size, file_mtime))) {
        LOG_WARN("stat file failed", K(ret), K(path.c_str()));
      } else {
        std::unique_ptr<filesql::IFileReader> reader;
        if (FILE_SQL_FORMAT_CSV == expected_format) {
          reader = filesql::create_csv_reader();
        } else if (FILE_SQL_FORMAT_JSONL == expected_format) {
          reader = filesql::create_jsonl_reader();
        } else {
          reader = filesql::create_parquet_reader();
        }
        filesql::FileOptions reader_opts;
        reader_opts.header = opts.has_header_;
        reader_opts.delimiter = opts.delimiter_;
        reader_opts.quote = opts.quote_;
        reader_opts.escape = opts.escape_;
        reader_opts.sample_size = opts.sample_size_;
        reader_opts.newline_delimited = opts.newline_delimited_;
        if (OB_UNLIKELY(NULL == reader.get())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (filesql::ReadStatus::OK != reader->open(path, reader_opts)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("open file for schema inference failed", K(ret), K(path.c_str()));
        } else {
          columns = reader->schema();
          reader->close();
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    OZ (fill_file_table_def(*allocator_, opts, file_size, file_mtime, columns, def));
  }
  if (OB_SUCC(ret)) {
    ObString alias_name;
    if (OB_NOT_NULL(parse_tree.children_[2])) {
      const ParseNode *alias_node = parse_tree.children_[2];
      alias_name.assign_ptr(alias_node->str_value_, static_cast<int32_t>(alias_node->str_len_));
    }
    if (OB_ISNULL(item = stmt->create_table_item(*allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      item->table_id_ = generate_table_id();
      item->ref_id_ = OB_INVALID_ID;
      item->type_ = TableItem::FUNCTION_TABLE;
      if (!alias_name.empty()) {
        item->alias_name_ = alias_name;
        item->table_name_ = alias_name;
      } else {
        item->table_name_ = func_name;
        item->alias_name_ = func_name;
      }
      item->function_table_expr_ = NULL;
      item->file_table_def_ = def;
      OZ (stmt->add_table_item(session_info_, item));
    }
  }
  if (OB_SUCC(ret)) {
    ObSEArray<ColumnItem, 16> col_items;
    OZ (resolve_file_table_columns(*item, col_items));
  }
  if (OB_SUCC(ret)) {
    tbl_item = item;
  }
  return ret;
}

int ObDMLResolver::resolve_file_table_columns(const TableItem &table_item,
                                               ObIArray<ColumnItem> &col_items)
{
  int ret = OB_SUCCESS;
  const FileTableDef *def = table_item.file_table_def_;
  if (OB_ISNULL(def)) {
    ret = OB_ERR_UNEXPECTED;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < def->column_count_; ++i) {
    ColumnItem *col_item = NULL;
    OZ (resolve_file_table_column_item(table_item, i, col_item));
    if (OB_SUCC(ret)) {
      OZ (col_items.push_back(*col_item));
    }
  }
  return ret;
}

int ObDMLResolver::resolve_file_table_column_item(const TableItem &table_item,
                                                   const int64_t column_index,
                                                   ColumnItem *&col_item)
{
  int ret = OB_SUCCESS;
  const FileTableDef *def = table_item.file_table_def_;
  ObObjMeta meta;
  ObAccuracy accuracy;
  if (OB_ISNULL(def) || column_index < 0 || column_index >= def->column_count_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(build_obj_meta(def->columns_[column_index].logical_type_, meta, accuracy))) {
  } else if (OB_FAIL(resolve_function_table_column_item(table_item,
                                                        meta,
                                                        accuracy,
                                                        def->columns_[column_index].name_,
                                                        OB_APP_MIN_COLUMN_ID + column_index,
                                                        col_item))) {
    LOG_WARN("resolve file table column failed", K(ret), K(column_index));
  }
  return ret;
}

namespace filesql
{

bool parse_format_name(const std::string &name, FileFormat &fmt)
{
  bool ok = true;
  if (name == "csv") {
    fmt = FileFormat::CSV;
  } else if (name == "json" || name == "newline_delimited" || name == "ndjson") {
    fmt = FileFormat::JSONL;
  } else if (name == "parquet") {
    fmt = FileFormat::PARQUET;
  } else {
    ok = false;
  }
  return ok;
}

} // namespace filesql

} // namespace sql
} // namespace oceanbase
