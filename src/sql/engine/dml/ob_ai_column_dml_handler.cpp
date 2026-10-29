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

#include <atomic>
#include <cctype>
#include "sql/engine/dml/ob_ai_column_dml_handler.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/dml/ob_dml_ctx_define.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_ai_column_info.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_sql_string.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace
{

std::atomic<int64_t> g_ai_task_counter(0);

const char *AI_TASK_TABLE = "oceanbase.__all_ai_column_task";

int find_column_index(const ObDMLBaseCtDef &ctdef,
                      const uint64_t column_id,
                      int64_t &idx)
{
  int ret = OB_SUCCESS;
  idx = -1;
  for (int64_t i = 0; OB_SUCC(ret) && i < ctdef.column_ids_.count(); ++i) {
    if (ctdef.column_ids_.at(i) == column_id) {
      idx = i;
      break;
    }
  }
  if (OB_SUCC(ret) && -1 == idx) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int eval_row_datum(ObTableModifyOp &op,
                   const ObDMLBaseCtDef &ctdef,
                   const int64_t idx,
                   const bool use_old_row,
                   const ObDatum *&datum)
{
  int ret = OB_SUCCESS;
  datum = NULL;
  const ObExprPtrIArray &row = use_old_row ? ctdef.old_row_ : ctdef.new_row_;
  ObDatum *tmp = NULL;
  if (OB_UNLIKELY(idx < 0 || idx >= row.count())) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_ISNULL(row.at(idx))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(row.at(idx)->eval(op.get_eval_ctx(), tmp))) {
    LOG_WARN("failed to eval row expr", KR(ret), K(idx));
  } else if (OB_ISNULL(tmp)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    datum = tmp;
  }
  return ret;
}

int append_sql_escaped(ObSqlString &sql, const char *str, const int64_t len)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
    const char c = str[i];
    if ('\\' == c || '\'' == c) {
      ret = sql.append_fmt("\\%c", c);
    } else {
      ret = sql.append_fmt("%c", c);
    }
  }
  return ret;
}

int datum_to_sql_literal(const ObColumnSchemaV2 &column,
                         const ObDatum &datum,
                         ObSqlString &literal)
{
  int ret = OB_SUCCESS;
  literal.reuse();
  const ObObjType type = column.get_data_type();
  if (datum.is_null()) {
    ret = literal.append("NULL");
  } else if (ob_is_integer_type(type)) {
    ret = literal.append_fmt("%ld", datum.get_int());
  } else if (ob_is_unsigned_type(type)) {
    ret = literal.append_fmt("%lu", datum.get_uint64());
  } else if (ob_is_string_type(type) || ob_is_text_tc(type)) {
    const ObString s = datum.get_string();
    if (OB_FAIL(literal.append("'"))) {
    } else if (OB_FAIL(append_sql_escaped(literal, s.ptr(), s.length()))) {
    } else if (OB_FAIL(literal.append("'"))) {
    }
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int build_rowkey_predicate(ObTableModifyOp &op,
                           const ObDMLBaseCtDef &ctdef,
                           const ObTableSchema &table_schema,
                           const bool use_old_row,
                           ObSqlString &predicate)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> rowkey_ids;
  predicate.reuse();
  if (OB_FAIL(table_schema.get_rowkey_column_ids(rowkey_ids))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_ids.count(); ++i) {
    const uint64_t column_id = rowkey_ids.at(i);
    const ObColumnSchemaV2 *column = table_schema.get_column_schema(column_id);
    int64_t idx = -1;
    const ObDatum *datum = NULL;
    ObSqlString literal;
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(find_column_index(ctdef, column_id, idx))) {
      LOG_WARN("rowkey column not found in dml column ids", KR(ret), K(column_id));
    } else if (OB_FAIL(eval_row_datum(op, ctdef, idx, use_old_row, datum))) {
    } else if (OB_ISNULL(datum) || datum->is_null()) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(datum_to_sql_literal(*column, *datum, literal))) {
    } else if (OB_FAIL(predicate.append_fmt("%s`%.*s` = %.*s",
                                           (0 == i) ? "" : " and ",
                                           column->get_column_name_str().length(),
                                           column->get_column_name_str().ptr(),
                                           static_cast<int>(literal.length()),
                                           literal.ptr()))) {
    }
  }
  return ret;
}

int column_value_equal(ObTableModifyOp &op,
                       const ObDMLBaseCtDef &ctdef,
                       const uint64_t column_id,
                       bool &equal)
{
  int ret = OB_SUCCESS;
  equal = false;
  int64_t idx = -1;
  const ObDatum *old_datum = NULL;
  const ObDatum *new_datum = NULL;
  if (OB_FAIL(find_column_index(ctdef, column_id, idx))) {
    ret = OB_SUCCESS;
    equal = false;
  } else if (OB_FAIL(eval_row_datum(op, ctdef, idx, true, old_datum))) {
    ret = OB_SUCCESS;
    equal = false;
  } else if (OB_FAIL(eval_row_datum(op, ctdef, idx, false, new_datum))) {
    ret = OB_SUCCESS;
    equal = false;
  } else if (OB_ISNULL(old_datum) || OB_ISNULL(new_datum)) {
    equal = false;
  } else {
    equal = ObDatum::binary_equal(*old_datum, *new_datum);
  }
  return ret;
}

const char *ai_function_name_for_task(const int64_t ai_function)
{
  const char *name = "AI_COMPLETE";
  switch (ai_function) {
    case AI_COLUMN_FUNC_EMBED: name = "AI_EMBED"; break;
    case AI_COLUMN_FUNC_RERANK: name = "AI_RERANK"; break;
    case AI_COLUMN_FUNC_PROMPT: name = "AI_PROMPT"; break;
    case AI_COLUMN_FUNC_COMPLETE:
    default: name = "AI_COMPLETE"; break;
  }
  return name;
}

} // anonymous namespace

int ObAiColumnDMLHandler::extract_prompt_expr(const ObString &expr,
                                              ObString &prompt_expr)
{
  int ret = OB_SUCCESS;
  prompt_expr.reset();
  const char *p = expr.ptr();
  const int64_t len = expr.length();
  int64_t pos = 0;
  while (pos < len && isspace(static_cast<unsigned char>(p[pos]))) {
    ++pos;
  }
  const int64_t name_begin = pos;
  while (pos < len && (isalnum(static_cast<unsigned char>(p[pos])) || '_' == p[pos])) {
    ++pos;
  }
  const int64_t name_len = pos - name_begin;
  while (pos < len && isspace(static_cast<unsigned char>(p[pos]))) {
    ++pos;
  }
  if (name_len <= 0 || pos >= len || '(' != p[pos]) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ++pos;
    int64_t depth = 0;
    int64_t arg_begin = pos;
    int64_t arg_index = 0;
    int64_t found_begin = -1;
    int64_t found_end = -1;
    char quote = '\0';
    for (; OB_SUCC(ret) && pos < len; ++pos) {
      const char c = p[pos];
      if ('\0' != quote) {
        if ('\\' == c) {
          ++pos;
        } else if (c == quote) {
          quote = '\0';
        }
      } else if ('\'' == c || '"' == c || '`' == c) {
        quote = c;
      } else if ('(' == c) {
        ++depth;
      } else if (')' == c) {
        if (0 == depth) {
          if (1 == arg_index) {
            found_begin = arg_begin;
            found_end = pos;
          }
          break;
        } else {
          --depth;
        }
      } else if (',' == c && 0 == depth) {
        if (1 == arg_index) {
          found_begin = arg_begin;
          found_end = pos;
        }
        ++arg_index;
        arg_begin = pos + 1;
      }
    }
    if (OB_SUCC(ret) && found_begin >= 0 && found_end > found_begin) {
      while (found_begin < found_end && isspace(static_cast<unsigned char>(p[found_begin]))) {
        ++found_begin;
      }
      while (found_end > found_begin && isspace(static_cast<unsigned char>(p[found_end - 1]))) {
        --found_end;
      }
      prompt_expr.assign_ptr(p + found_begin, static_cast<int32_t>(found_end - found_begin));
    } else if (OB_SUCC(ret)) {
      ret = OB_INVALID_ARGUMENT;
    }
  }
  return ret;
}

int ObAiColumnDMLHandler::handle_row(ObTableModifyOp &op,
                                     const ObDMLBaseCtDef &ctdef,
                                     ObDMLBaseRtDef &dml_rtdef,
                                     const ObDmlEventType dml_event)
{
  int ret = OB_SUCCESS;
  int step = 0;
  UNUSED(dml_rtdef);
  ObExecContext &exec_ctx = op.get_exec_ctx();
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  const uint64_t table_id = ctdef.das_base_ctdef_.index_tid_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const ObDatabaseSchema *db_schema = NULL;
  ObSEArray<const ObColumnSchemaV2 *, 4> ai_columns;
  const bool is_insert = (ObDmlEventType::DE_INSERTING == dml_event);
  const bool is_update = (ObDmlEventType::DE_UPDATING == dml_event);
  const bool is_delete = (ObDmlEventType::DE_DELETING == dml_event);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (session->is_inner()) {
    // nested SQL (including our own task writes) must not trigger ai processing again
    ret = OB_SUCCESS;
  } else if (OB_ISNULL(op.inner_conn_) && OB_FAIL(op.open_inner_conn())) {
    LOG_WARN("failed to open inner connection for ai column", KR(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (FALSE_IT(step = 1)) {
  } else if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(schema_guard))) {
  } else if (FALSE_IT(step = 2)) {
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_column_count(); ++i) {
      const ObColumnSchemaV2 *column = table_schema->get_column_schema_by_idx(i);
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (column->is_ai_column() && OB_FAIL(ai_columns.push_back(column))) {
        LOG_WARN("failed to push ai column", KR(ret));
      }
    }
    step = 3;
    if (OB_FAIL(ret) || ai_columns.empty()) {
      // nothing to do
    } else if (FALSE_IT(step = 4)) {
    } else if (OB_FAIL(schema_guard.get_database_schema(table_schema->get_database_id(), db_schema))) {
    } else if (OB_ISNULL(db_schema)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const ObString &db_name = db_schema->get_database_name_str();
      ObArenaAllocator allocator(ObModIds::OB_MODULE_PAGE_ALLOCATOR);
      ObSqlString predicate;
      ObSqlString old_predicate;
      bool rowkey_changed = false;
      step = 5;
      if (!is_delete && OB_FAIL(build_rowkey_predicate(op, ctdef, *table_schema, false, predicate))) {
        LOG_WARN("failed to build rowkey predicate", KR(ret));
      }
      if (OB_SUCC(ret) && is_update) {
        ObSEArray<uint64_t, 4> rowkey_ids;
        step = 6;
        if (OB_FAIL(build_rowkey_predicate(op, ctdef, *table_schema, true, old_predicate))) {
        } else if (OB_FAIL(table_schema->get_rowkey_column_ids(rowkey_ids))) {
        }
        for (int64_t i = 0; OB_SUCC(ret) && !rowkey_changed && i < rowkey_ids.count(); ++i) {
          bool equal = false;
          if (OB_FAIL(column_value_equal(op, ctdef, rowkey_ids.at(i), equal))) {
          } else if (!equal) {
            rowkey_changed = true;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_delete) {
        step = 7;
        ObSqlString del_sql;
        if (OB_FAIL(build_rowkey_predicate(op, ctdef, *table_schema, true, predicate))) {
        } else if (OB_FAIL(del_sql.append_fmt("DELETE FROM %s WHERE table_id = %lu AND rowkey_predicate = '",
                                              AI_TASK_TABLE, table_id))) {
        } else if (OB_FAIL(append_sql_escaped(del_sql, predicate.ptr(), predicate.length()))) {
        } else if (OB_FAIL(del_sql.append("'"))) {
        } else {
          const bool saved_inner = session->is_inner();
          const ObSQLSessionInfo::SessionType saved_type = session->get_session_type();
          const bool saved_fk_cascading = op.get_exec_ctx().get_das_ctx().is_fk_cascading_;
          const bool saved_iter_uncommitted = op.get_exec_ctx().get_das_ctx().iter_uncommitted_row_;
          op.get_exec_ctx().get_das_ctx().is_fk_cascading_ = true;
          op.get_exec_ctx().get_das_ctx().iter_uncommitted_row_ = true;
          session->set_inner_session();
          if (OB_FAIL(op.begin_nested_session(true))) {
          } else {
            const int tmp_ret = op.execute_write(del_sql.ptr());
            if (OB_SUCCESS != tmp_ret) {
              LOG_WARN("failed to delete ai tasks", K(tmp_ret));
              ret = tmp_ret;
            }
            const int end_ret = op.end_nested_session();
            if (OB_SUCCESS != end_ret && OB_SUCCESS == ret) {
              ret = end_ret;
            }
          }
          if (saved_inner) {
            session->set_inner_session();
            session->set_session_type(saved_type);
          } else {
            session->set_user_session();
          }
          op.get_exec_ctx().get_das_ctx().is_fk_cascading_ = saved_fk_cascading;
          op.get_exec_ctx().get_das_ctx().iter_uncommitted_row_ = saved_iter_uncommitted;
        }
      } else {
        ObSEArray<const ObColumnSchemaV2 *, 4> invalidated_columns;
        for (int64_t i = 0; OB_SUCC(ret) && i < ai_columns.count(); ++i) {
          const ObColumnSchemaV2 *column = ai_columns.at(i);
          const ObAiColumnInfo *info = column->get_ai_column_info();
          bool invalidate = false;
          if (OB_ISNULL(info)) {
            ret = OB_ERR_UNEXPECTED;
          } else if (is_insert || rowkey_changed) {
            invalidate = true;
          } else {
            const ObAiColumnInfo::DependencyColumnIds &deps = info->get_dependency_column_ids();
            if (deps.empty()) {
              // dependency information missing (e.g. column added by ALTER), be conservative
              invalidate = true;
            }
            for (int64_t d = 0; OB_SUCC(ret) && !invalidate && d < deps.count(); ++d) {
              bool equal = false;
              if (OB_FAIL(column_value_equal(op, ctdef, deps.at(d), equal))) {
              } else if (!equal) {
                invalidate = true;
              }
            }
          }
          if (OB_SUCC(ret) && invalidate && OB_FAIL(invalidated_columns.push_back(column))) {
            LOG_WARN("failed to push invalidated column", KR(ret));
          }
        }
        step = 8;
        if (OB_FAIL(ret) || invalidated_columns.empty()) {
          // nothing to invalidate
        } else {
          step = 9;
          char rid_buf[96];
          const int64_t rid_len = snprintf(rid_buf, sizeof(rid_buf), "%ld_%lu_%ld",
              ObTimeUtility::current_time(), table_id,
              g_ai_task_counter.fetch_add(1));
          ObSqlString update_sql;
          if (rid_len <= 0 || rid_len >= static_cast<int64_t>(sizeof(rid_buf))) {
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(update_sql.append_fmt("UPDATE `%.*s`.`%.*s` SET ",
                     db_name.length(), db_name.ptr(),
                     table_schema->get_table_name_str().length(),
                     table_schema->get_table_name_str().ptr()))) {
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < invalidated_columns.count(); ++i) {
            const ObColumnSchemaV2 *column = invalidated_columns.at(i);
            const ObColumnSchemaV2 *state_col = table_schema->get_column_schema(
                column->get_ai_state_column_id());
            const ObColumnSchemaV2 *request_col = table_schema->get_column_schema(
                column->get_ai_request_column_id());
            const ObColumnSchemaV2 *spec_col = table_schema->get_column_schema(
                column->get_ai_result_spec_column_id());
            if (OB_ISNULL(state_col) || OB_ISNULL(request_col) || OB_ISNULL(spec_col)) {
              ret = OB_ERR_UNEXPECTED;
            } else if (OB_FAIL(update_sql.append_fmt(
                         "%s`%.*s` = NULL, `%.*s` = 0, `%.*s` = '%s', `%.*s` = NULL",
                         (0 == i) ? "" : ", ",
                         column->get_column_name_str().length(), column->get_column_name_str().ptr(),
                         state_col->get_column_name_str().length(), state_col->get_column_name_str().ptr(),
                         request_col->get_column_name_str().length(), request_col->get_column_name_str().ptr(),
                         rid_buf,
                         spec_col->get_column_name_str().length(), spec_col->get_column_name_str().ptr()))) {
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(update_sql.append_fmt(" WHERE %.*s",
                     static_cast<int>(predicate.length()), predicate.ptr()))) {
          } else {
            const bool saved_inner = session->is_inner();
            const ObSQLSessionInfo::SessionType saved_type = session->get_session_type();
            const bool saved_fk_cascading = op.get_exec_ctx().get_das_ctx().is_fk_cascading_;
            const bool saved_iter_uncommitted = op.get_exec_ctx().get_das_ctx().iter_uncommitted_row_;
            op.get_exec_ctx().get_das_ctx().is_fk_cascading_ = true;
            op.get_exec_ctx().get_das_ctx().iter_uncommitted_row_ = true;
            session->set_inner_session();
            if (OB_FAIL(op.begin_nested_session(true))) {
            } else {
              step = 11;
              int tmp_ret = op.execute_write(update_sql.ptr());
              if (OB_SUCCESS != tmp_ret) {
                LOG_WARN("failed to invalidate ai column", K(tmp_ret), K(update_sql));
                ret = tmp_ret;
              }
              if (OB_SUCC(ret) && rowkey_changed && old_predicate.length() > 0) {
                ObSqlString del_sql;
                if (OB_FAIL(del_sql.append_fmt("DELETE FROM %s WHERE table_id = %lu AND rowkey_predicate = '",
                                               AI_TASK_TABLE, table_id))) {
                } else if (OB_FAIL(append_sql_escaped(del_sql, old_predicate.ptr(), old_predicate.length()))) {
                } else if (OB_FAIL(del_sql.append("'"))) {
                } else if (OB_FAIL(op.execute_write(del_sql.ptr()))) {
                }
              }
              for (int64_t i = 0; OB_SUCC(ret) && i < invalidated_columns.count(); ++i) {
                const ObColumnSchemaV2 *column = invalidated_columns.at(i);
                const ObAiColumnInfo *info = column->get_ai_column_info();
                ObString prompt_expr;
                ObSqlString del_sql;
                ObSqlString ins_sql;
                if (OB_ISNULL(info)) {
                  ret = OB_ERR_UNEXPECTED;
                } else if (OB_FAIL(extract_prompt_expr(info->get_expr_sql(), prompt_expr))) {
                  prompt_expr = info->get_expr_sql();
                  ret = OB_SUCCESS;
                }
                if (OB_FAIL(ret)) {
                } else if (OB_FAIL(del_sql.append_fmt(
                             "DELETE FROM %s WHERE table_id = %lu AND column_id = %lu "
                             "AND rowkey_predicate = '", AI_TASK_TABLE, table_id,
                             column->get_column_id()))) {
                } else if (OB_FAIL(append_sql_escaped(del_sql, predicate.ptr(), predicate.length()))) {
                } else if (OB_FAIL(del_sql.append("'"))) {
                } else if (OB_FAIL(op.execute_write(del_sql.ptr()))) {
                } else if (OB_FAIL(ins_sql.append_fmt(
                             "INSERT INTO %s (table_id, request_id, column_id, state, retry_count, "
                             "eligible_ts, rowkey_predicate, definition_version, ai_function, model_name, "
                             "result_type, expr) VALUES (%lu, '%s', %lu, 0, 0, now(6), '",
                             AI_TASK_TABLE, table_id, rid_buf, column->get_column_id()))) {
                } else if (OB_FAIL(append_sql_escaped(ins_sql, predicate.ptr(), predicate.length()))) {
                } else if (OB_FAIL(ins_sql.append_fmt("', %ld, %ld, '",
                                                       info->get_definition_version(),
                                                       info->get_ai_function()))) {
                } else if (OB_FAIL(append_sql_escaped(ins_sql,
                                                       info->get_model_name().ptr(),
                                                       info->get_model_name().length()))) {
                } else if (OB_FAIL(ins_sql.append_fmt("', %ld, '", info->get_result_type()))) {
                } else if (OB_FAIL(append_sql_escaped(ins_sql, prompt_expr.ptr(), prompt_expr.length()))) {
                } else if (OB_FAIL(ins_sql.append("')"))) {
                } else if (OB_FAIL(op.execute_write(ins_sql.ptr()))) {
                  LOG_WARN("failed to insert ai task", KR(ret), K(ins_sql));
                }
              }
              const int end_ret = op.end_nested_session();
              if (OB_SUCCESS != end_ret && OB_SUCCESS == ret) {
                ret = end_ret;
              }
              if (saved_inner) {
                session->set_inner_session();
                session->set_session_type(saved_type);
              } else {
                session->set_user_session();
              }
              op.get_exec_ctx().get_das_ctx().is_fk_cascading_ = saved_fk_cascading;
              op.get_exec_ctx().get_das_ctx().iter_uncommitted_row_ = saved_iter_uncommitted;
            }
          }
        }
      }
      UNUSED(allocator);
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("ai column handler failed", KR(ret), K(step), K(table_id), K(is_insert), K(is_update));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
