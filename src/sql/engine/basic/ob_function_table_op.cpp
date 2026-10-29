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

#define USING_LOG_PREFIX SQL_ENG

#include <sys/stat.h>
#include <algorithm>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "sql/engine/basic/ob_function_table_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/file_scan/file_sql_types.h"
#include "sql/engine/file_scan/file_sql_def.h"
#include "common/timezone/ob_time_convert.h"


namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObFunctionTableSpec, ObOpSpec), value_expr_, column_exprs_, has_correlated_expr_,
                    file_format_, file_path_, file_flags_, file_size_, file_mtime_, file_column_types_);

namespace
{

bool file_wildcard_match(const char *pattern, const char *name)
{
  // supports '*' and '?' wildcards
  if ('\0' == *pattern) {
    return '\0' == *name;
  } else if ('*' == *pattern) {
    for (const char *p = name; ; ++p) {
      if (file_wildcard_match(pattern + 1, p)) {
        return true;
      }
      if ('\0' == *p) {
        break;
      }
    }
    return false;
  } else if ('?' == *pattern) {
    return '\0' != *name && file_wildcard_match(pattern + 1, name + 1);
  } else {
    return *pattern == *name && file_wildcard_match(pattern + 1, name + 1);
  }
}

} // namespace

int ObFunctionTableOp::inner_open()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  already_calc_ = false;
  file_reader_ = NULL;
  if (MY_SPEC.is_file_table()) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_file;
    if (OB_FAIL(open_file_reader())) {
      LOG_WARN("open file reader failed", K(ret));
    }
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObExtendType == MY_SPEC.value_expr_->datum_meta_.type_) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_udf;
  } else {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_sys_func;
  }
  return ret;
}

int ObFunctionTableOp::open_file_reader()
{
  int ret = OB_SUCCESS;
  const ObFunctionTableSpec &spec = MY_SPEC;
  const std::string path(spec.file_path_.ptr(), static_cast<size_t>(spec.file_path_.length()));
  if (static_cast<int64_t>(filesql::FileFormat::CSV) == spec.file_format_
      || static_cast<int64_t>(filesql::FileFormat::JSONL) == spec.file_format_
      || static_cast<int64_t>(filesql::FileFormat::PARQUET) == spec.file_format_) {
    struct stat st;
    if (0 != ::stat(path.c_str(), &st)) {
      ret = OB_FILE_NOT_EXIST;
      LOG_WARN("file does not exist at execution time", K(ret), K(path.c_str()));
    } else if (static_cast<int64_t>(st.st_size) != spec.file_size_
               || static_cast<int64_t>(st.st_mtime) != spec.file_mtime_) {
      // the file was modified between resolve and execute: reject instead of
      // returning rows for a schema that may no longer match the content.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file changed between resolve and execution", K(ret), K(path.c_str()),
               K(spec.file_size_), "real_size", st.st_size);
    } else {
      filesql::FileOptions opts;
      const int64_t flags = spec.file_flags_;
      opts.header = 0 != (flags & 1);
      opts.delimiter = static_cast<char>((flags >> 8) & 0xff);
      opts.quote = static_cast<char>((flags >> 16) & 0xff);
      opts.escape = static_cast<char>((flags >> 24) & 0xff);
      std::unique_ptr<filesql::IFileReader> reader;
      if (static_cast<int64_t>(filesql::FileFormat::CSV) == spec.file_format_) {
        reader = filesql::create_csv_reader();
      } else if (static_cast<int64_t>(filesql::FileFormat::JSONL) == spec.file_format_) {
        reader = filesql::create_jsonl_reader();
      } else {
        reader = filesql::create_parquet_reader();
      }
      if (OB_ISNULL(reader.get())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (filesql::ReadStatus::OK != reader->open(path, opts)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("open file reader failed", K(ret), K(path.c_str()));
      } else {
        file_reader_ = reader.release();
      }
    }
  } else {
    // glob(pattern): enumerate matching regular files once, rows are emitted from
    // inner_get_next_row_file().
    std::vector<std::string> *files = NULL;
    if (OB_ISNULL(files = new (std::nothrow) std::vector<std::string>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      std::error_code ec;
      std::filesystem::path pattern(path);
      std::string file_pattern = pattern.filename().string();
      std::filesystem::path dir = pattern.parent_path();
      if (dir.empty()) {
        dir = ".";
      }
      if (file_pattern.empty()) {
        file_pattern = "*";
      }
      if (!std::filesystem::exists(dir, ec)) {
        ret = OB_FILE_NOT_EXIST;
      } else {
        for (const auto &entry : std::filesystem::directory_iterator(
                 dir, std::filesystem::directory_options::skip_permission_denied, ec)) {
          if (ec) {
            break;
          }
          std::string name = entry.path().filename().string();
          if (entry.is_regular_file(ec) && file_wildcard_match(file_pattern.c_str(), name.c_str())) {
            // report paths with '/' separators so SQL string functions behave uniformly
            std::string matched = entry.path().string();
            std::replace(matched.begin(), matched.end(), '\\', '/');
            files->push_back(matched);
          }
        }
        std::sort(files->begin(), files->end());
      }
      if (OB_SUCC(ret)) {
        glob_files_ = files;
      } else {
        delete files;
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_file()
{
  int ret = OB_SUCCESS;
  const ObFunctionTableSpec &spec = MY_SPEC;
  clear_evaluated_flag();
  if (OB_FAIL(ctx_.check_status())) {
  } else if (FILE_SQL_FORMAT_GLOB == spec.file_format_) {
    std::vector<std::string> *files = static_cast<std::vector<std::string> *>(glob_files_);
    if (OB_ISNULL(files)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (glob_idx_ >= static_cast<int64_t>(files->size())) {
      ret = OB_ITER_END;
    } else {
      const std::string &f = files->at(glob_idx_++);
      ObExpr *expr = spec.column_exprs_.at(0);
      const int64_t str_len = static_cast<int64_t>(f.size());
      char *buf = expr->get_str_res_mem(eval_ctx_, str_len);
      if (OB_ISNULL(buf) && str_len > 0) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        if (str_len > 0) {
          MEMCPY(buf, f.data(), str_len);
        }
        ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
        datum.set_string(buf, str_len);
        expr->set_evaluated_projected(eval_ctx_);
      }
    }
  } else if (OB_ISNULL(file_reader_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    std::vector<filesql::Cell> cells;
    std::string err;
    filesql::ReadStatus status = file_reader_->next_row(cells, err);
    if (filesql::ReadStatus::END == status) {
      ret = OB_ITER_END;
    } else if (filesql::ReadStatus::ERROR == status) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("malformed file row", K(ret), K(err.c_str()));
    } else if (OB_UNLIKELY(cells.size() != spec.column_exprs_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column count mismatch", K(ret), "cells", cells.size(),
               "columns", spec.column_exprs_.count());
    } else {
      const ObTimeZoneInfo *tz_info = NULL;
      if (OB_NOT_NULL(ctx_.get_my_session())) {
        tz_info = ctx_.get_my_session()->get_timezone_info();
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < cells.size(); ++i) {
        ObObj obj;
        const filesql::Cell &cell = cells.at(i);
        ObExpr *expr = spec.column_exprs_.at(i);
        ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
        if (cell.is_null) {
          datum.set_null();
        } else if (FILE_SQL_TYPE_BIGINT == spec.file_column_types_.at(i)) {
          datum.set_int(cell.i);
        } else if (FILE_SQL_TYPE_DOUBLE == spec.file_column_types_.at(i)) {
          datum.set_double(cell.d);
        } else if (FILE_SQL_TYPE_BOOLEAN == spec.file_column_types_.at(i)) {
          datum.set_int(static_cast<int64_t>(static_cast<int8_t>(cell.i)));
        } else if (FILE_SQL_TYPE_DATE == spec.file_column_types_.at(i)) {
          int32_t date = 0;
          if (OB_FAIL(ObTimeConverter::str_to_date(
                ObString(static_cast<int32_t>(cell.s.size()), cell.s.data()), date))) {
            LOG_WARN("convert date failed", K(ret), K(cell.s.c_str()));
          } else {
            datum.set_date(date);
          }
        } else if (FILE_SQL_TYPE_DATETIME == spec.file_column_types_.at(i)) {
          int64_t datetime = 0;
          ObTimeConvertCtx cvrt_ctx(tz_info, false);
          if (OB_FAIL(ObTimeConverter::str_to_datetime(
                ObString(static_cast<int32_t>(cell.s.size()), cell.s.data()), cvrt_ctx, datetime))) {
            LOG_WARN("convert datetime failed", K(ret), K(cell.s.c_str()));
          } else {
            datum.set_datetime(datetime);
          }
        } else {
          const int64_t str_len = static_cast<int64_t>(cell.s.size());
          char *buf = expr->get_str_res_mem(eval_ctx_, str_len);
          if (OB_ISNULL(buf) && str_len > 0) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            if (str_len > 0) {
              MEMCPY(buf, cell.s.data(), str_len);
            }
            datum.set_string(buf, str_len);
          }
        }
        if (OB_SUCC(ret)) {
          expr->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else {
    node_idx_ = 0;
    if (MY_SPEC.has_correlated_expr_) {
      row_count_ = 0;
      col_count_ = 0;
      value_table_ = NULL;
      already_calc_ = false;
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  row_count_ = 0;
  col_count_ = 0;
  value_table_ = NULL;
  if (NULL != file_reader_) {
    file_reader_->close();
    delete file_reader_;
    file_reader_ = NULL;
  }
  if (NULL != glob_files_) {
    delete static_cast<std::vector<std::string> *>(glob_files_);
    glob_files_ = NULL;
  }
  glob_idx_ = 0;
  return ret;
}

void ObFunctionTableOp::destroy()
{
  if (NULL != file_reader_) {
    file_reader_->close();
    delete file_reader_;
    file_reader_ = NULL;
  }
  if (NULL != glob_files_) {
    delete static_cast<std::vector<std::string> *>(glob_files_);
    glob_files_ = NULL;
  }
  ObOperator::destroy();
}


int ObFunctionTableOp::get_current_result(ObObj &result)
{
  int ret = OB_SUCCESS;
  void *data = NULL;
  CK (already_calc_);
  if (node_idx_ < 0 || node_idx_ >= row_count_ * col_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get current result in table function",
              K(node_idx_), K(row_count_), K(col_count_));
  }
  do {
    CK (node_idx_ >= 0);
    if (OB_SUCC(ret) && node_idx_ >= row_count_) {
      ret = OB_ITER_END;
    }
    CK (OB_NOT_NULL(value_table_));
    OX (data = value_table_->get_data());
    CK (OB_NOT_NULL(data));
    OX (result = (static_cast<ObObj*>(data))[node_idx_++]);
  } while (OB_SUCC(ret) && result.get_meta().get_type() == ObMaxType);
  return ret;
}

int ObFunctionTableOp::inner_get_next_row()
{
  return (this->*next_row_func_)();
}

int ObFunctionTableOp::inner_get_next_row_udf()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObExtendType != MY_SPEC.value_expr_->datum_meta_.type_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (FALSE_IT(plan_ctx->set_autoinc_id_tmp(0))) {
  } else if (OB_FAIL(ctx_.check_status())) {
  } else {
    ObDatum *value = nullptr;
    if (!already_calc_) {
      if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
      } else if (value->is_null()) {
        //do nothing
      } else if (OB_ISNULL(value_table_ 
                 = reinterpret_cast<pl::ObPLCollection*>(value->get_ext()))) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        row_count_ = value_table_->is_inited() ? value_table_->get_count() : 0;
        col_count_ = value_table_->get_column_count();
      }
      already_calc_ = true;
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(node_idx_ >= row_count_)) {
      ret = OB_ITER_END;
    }
    CK (MY_SPEC.column_exprs_.count() >= col_count_);
    ObObj obj_stack[col_count_];
    if (OB_SUCC(ret)) {
      if (nullptr != value_table_ 
          && ObExtendType == value_table_->get_element_type().get_obj_type()) {
        pl::ObPLComposite *composite = NULL;
        pl::ObPLRecord *record = NULL;
        ObObj record_obj;
        OZ (get_current_result(record_obj));
        if (OB_FAIL(ret)) {
        } else if (ObUserDefinedSQLType == record_obj.get_type()) {
          obj_stack[0] = record_obj;   
        } else if (record_obj.is_pl_extend()) {
          CK (OB_NOT_NULL(composite = reinterpret_cast<pl::ObPLComposite*>(record_obj.get_ext())));
          if (OB_SUCC(ret)) {
            if (composite->is_record()) {
              OX (record = static_cast<pl::ObPLRecord*>(composite));
              CK (record->get_count() == col_count_);
              for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
                OZ (record->get_element(i, obj_stack[i]));
              }
            } else if (composite->is_collection()) {
              OX (obj_stack[0] = record_obj);
            } else {
              ret = OB_ERR_UNEXPECTED;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected here", K(ret), K(record_obj), K(record_obj.meta_));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
          if (OB_FAIL(get_current_result(obj_stack[i]))) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
        if (obj_stack[i].is_null()) {
          MY_SPEC.column_exprs_.at(i)->locate_datum_for_write(eval_ctx_).set_null();
        } else {
          const ObObjDatumMapType &datum_map = MY_SPEC.column_exprs_.at(i)->obj_datum_map_;
          ObExpr * const &expr = MY_SPEC.column_exprs_.at(i);
          ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
          if (OB_FAIL(datum.from_obj(obj_stack[i], datum_map))) {
          } else if (is_lob_storage(obj_stack[i].get_type()) &&
                     OB_FAIL(ob_adjust_lob_datum(get_exec_ctx(), obj_stack[i],
                                                 expr->obj_meta_, datum_map,
                                                 get_exec_ctx().get_allocator(), datum))) {
          }
        }
        if (OB_SUCC(ret)) {
          MY_SPEC.column_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_sys_func()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  ObDatum *value = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
    if (OB_ITER_END != ret) {
    }
  } else {
    MY_SPEC.column_exprs_.at(0)->locate_datum_for_write(eval_ctx_).set_datum(*value);
    MY_SPEC.column_exprs_.at(0)->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}


} // end namespace sql
} // end namespace oceanbase
