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

#define USING_LOG_PREFIX SERVER

#include "observer/ai_service/ob_ai_column_task_worker.h"
#include "share/json/ob_json_helper.h"
#include "share/ob_server_struct.h"
#include "share/rc/ob_server_runtime.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ai_service/ob_ai_service_struct.h"
#include "query/api/query/ai/ob_ai_endpoint_resolver.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "common/mysqlclient/ob_mysql_result.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "lib/allocator/page_arena.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace observer
{

const int64_t ObAiColumnTaskWorker::TASK_PERIOD;
using namespace common;
using namespace share;
using namespace share::schema;
using namespace sql;

namespace
{

const char *AI_COLUMN_TASK_TABLE = "oceanbase.__all_ai_column_task";
const char *AI_COLUMN_META_TABLE = "oceanbase.__all_ai_column";

int copy_string(ObIAllocator &allocator, const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  dst.reset();
  if (!src.empty()) {
    char *buf = static_cast<char *>(allocator.alloc(src.length()));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(buf, src.ptr(), src.length());
      dst.assign_ptr(buf, src.length());
    }
  }
  return ret;
}

int escape_sql_string(ObSqlString &sql, const char *ptr, const int64_t len)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
    const char c = ptr[i];
    if ('\\' == c || '\'' == c) {
      ret = sql.append_fmt("\\%c", c);
    } else {
      ret = sql.append_fmt("%c", c);
    }
  }
  return ret;
}

const char *type_hint_for(const int64_t result_type)
{
  const ObObjType type = static_cast<ObObjType>(result_type);
  if (ObJsonType == type) {
    return "Return exactly one valid JSON value.";
  } else if (ob_is_integer_type(type)) {
    return "Return a base-10 integer.";
  } else {
    return "";
  }
}

int read_string_cell(sqlclient::ObMySQLResult &row, const char *name,
                     ObIAllocator &allocator, ObString &value)
{
  int ret = OB_SUCCESS;
  ObString tmp;
  value.reset();
  if (OB_FAIL(row.get_varchar(name, tmp))) {
    if (OB_ERR_NULL_VALUE == ret) {
      ret = OB_SUCCESS;
      value.reset();
    }
  } else if (!tmp.empty() && OB_FAIL(copy_string(allocator, tmp, value))) {
  }
  return ret;
}

int read_int_cell(sqlclient::ObMySQLResult &row, const char *name, int64_t &value)
{
  int ret = OB_SUCCESS;
  ObString str;
  value = 0;
  if (OB_SUCC(row.get_int(name, value))) {
    // numeric column
  } else {
    ret = OB_SUCCESS;
    if (OB_FAIL(row.get_varchar(name, str))) {
      if (OB_ERR_NULL_VALUE == ret) {
        ret = OB_SUCCESS;
      }
    } else if (!str.empty()) {
      char buf[64];
      const int64_t len = MIN(static_cast<int64_t>(sizeof(buf) - 1),
                              static_cast<int64_t>(str.length()));
      MEMCPY(buf, str.ptr(), len);
      buf[len] = '\0';
      value = strtoll(buf, NULL, 10);
    }
  }
  return ret;
}

int read_uint_cell(sqlclient::ObMySQLResult &row, const char *name, uint64_t &value)
{
  int ret = OB_SUCCESS;
  int64_t tmp = 0;
  if (OB_FAIL(row.get_int(name, tmp))) {
  } else {
    value = static_cast<uint64_t>(tmp);
  }
  return ret;
}

} // anonymous namespace

ObAiColumnTaskWorker::ObAiColumnTaskWorker()
  : sql_proxy_(nullptr),
    timer_(),
    is_inited_(false),
    processing_(false)
{
}

ObAiColumnTaskWorker::~ObAiColumnTaskWorker()
{
  destroy();
}

ObAiColumnTaskWorker &ObAiColumnTaskWorker::get_instance()
{
  static ObAiColumnTaskWorker instance;
  return instance;
}

int ObAiColumnTaskWorker::init(common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(timer_.init("AiColTask", common::ObMemAttr("AiColTask")))) {
    LOG_WARN("failed to init ai column task timer", KR(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    disable_timeout_check();
    if (OB_FAIL(timer_.schedule(*this, TASK_PERIOD, true))) {
      LOG_WARN("failed to schedule ai column task worker", KR(ret));
    } else {
      is_inited_ = true;
      LOG_INFO("ai column task worker started", KP(this), K(TASK_PERIOD));
    }
  }
  return ret;
}

void ObAiColumnTaskWorker::destroy()
{
  if (timer_.inited()) {
    timer_.stop();
    timer_.wait();
    timer_.destroy();
  }
  is_inited_ = false;
  sql_proxy_ = nullptr;
}

void ObAiColumnTaskWorker::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || processing_) {
    // skip
  } else {
    processing_ = true;
    if (OB_FAIL(do_work_())) {
      LOG_WARN("ai column task worker failed", KR(ret));
    }
    processing_ = false;
  }
}

int ObAiColumnTaskWorker::do_work_()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_MODULE_PAGE_ALLOCATOR);
  ObSEArray<TaskInfo, 8> tasks;
  ObSEArray<bool, 8> processed;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(load_tasks_(allocator, tasks))) {
    LOG_WARN("failed to load ai column tasks", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
    if (OB_FAIL(processed.push_back(false))) {
      break;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
    if (i < processed.count() && processed.at(i)) {
      continue;
    }
    ObSEArray<TaskInfo, 4> group;
    const TaskInfo &head = tasks.at(i);
    if (OB_FAIL(group.push_back(head))) {
      break;
    }
    if (i < processed.count()) {
      processed.at(i) = true;
    }
    for (int64_t j = i + 1; j < tasks.count(); ++j) {
      if (j < processed.count() && !processed.at(j)
          && tasks.at(j).table_id_ == head.table_id_
          && 0 == tasks.at(j).request_id_.compare(head.request_id_)) {
        if (OB_FAIL(group.push_back(tasks.at(j)))) {
          break;
        }
        processed.at(j) = true;
      }
    }
    const int tmp_ret = process_group_(group);
    if (OB_SUCCESS != tmp_ret) {
      LOG_ERROR("failed to process ai task group", K(tmp_ret),
               K(head.table_id_), K(head.request_id_));
    }
  }
  return ret;
}

int ObAiColumnTaskWorker::mark_row_failed_(const TaskInfo &task, const bool drop_task)
{
  int ret = OB_SUCCESS;
  ColumnMeta meta;
  ObSchemaGetterGuard guard;
  const ObTableSchema *table = NULL;
  const ObDatabaseSchema *db = NULL;
  const ObColumnSchemaV2 *state_col = NULL;
  const ObColumnSchemaV2 *request_col = NULL;
  ObSqlString update_sql;
  ObSqlString delete_sql;
  ObMySQLTransaction trans;
  int64_t affected_rows = 0;
  if (OB_FAIL(load_column_meta_(task, meta))) {
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(guard))) {
  } else if (OB_FAIL(guard.get_table_schema(task.table_id_, table))) {
  } else if (OB_ISNULL(table)) {
    ret = OB_TABLE_NOT_EXIST;
  } else if (OB_FAIL(guard.get_database_schema(table->get_database_id(), db))) {
  } else if (OB_ISNULL(db)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(state_col = table->get_column_schema(meta.state_column_id_))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(request_col = table->get_column_schema(meta.request_column_id_))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(update_sql.append_fmt(
                 "UPDATE `%.*s`.`%.*s` SET `%.*s` = 2 WHERE %.*s AND `%.*s` = '",
                 db->get_database_name_str().length(), db->get_database_name_str().ptr(),
                 table->get_table_name_str().length(), table->get_table_name_str().ptr(),
                 state_col->get_column_name_str().length(), state_col->get_column_name_str().ptr(),
                 task.rowkey_predicate_.length(), task.rowkey_predicate_.ptr(),
                 request_col->get_column_name_str().length(),
                 request_col->get_column_name_str().ptr()))) {
  } else if (OB_FAIL(escape_sql_string(update_sql, task.request_id_.ptr(),
                                       task.request_id_.length()))) {
  } else if (OB_FAIL(update_sql.append("'"))) {
  } else if (drop_task && OB_FAIL(delete_sql.append_fmt(
                 "DELETE FROM %s WHERE table_id = %lu AND request_id = '",
                 AI_COLUMN_TASK_TABLE, task.table_id_))) {
  } else if (drop_task && OB_FAIL(escape_sql_string(delete_sql, task.request_id_.ptr(),
                                                    task.request_id_.length()))) {
  } else if (drop_task && OB_FAIL(delete_sql.append_fmt("' AND column_id = %lu",
                                                        task.column_id_))) {
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_))) {
    LOG_ERROR("ai mark failed: start trans", KR(ret));
  } else {
    if (OB_FAIL(trans.write(update_sql.ptr(), affected_rows))) {
      LOG_ERROR("ai mark failed: update row", KR(ret), K(update_sql));
    } else if (drop_task) {
      int64_t delete_rows = 0;
      if (OB_FAIL(trans.write(delete_sql.ptr(), delete_rows))) {
        LOG_ERROR("ai mark failed: delete task", KR(ret), K(delete_sql));
      }
    }
    if (trans.is_started()) {
      const int end_ret = trans.end(OB_SUCC(ret));
      if (OB_SUCCESS != end_ret && OB_SUCCESS == ret) {
        ret = end_ret;
      }
    }
  }
  return ret;
}

int ObAiColumnTaskWorker::retry_or_fail_task_(const TaskInfo &task, const int64_t max_retries)
{
  int ret = OB_SUCCESS;
  if (task.retry_count_ < max_retries) {
    if (OB_FAIL(cleanup_task_(task, false, task.retry_count_ + 1))) {
      LOG_ERROR("failed to mark ai task retry", KR(ret));
    }
  } else {
    if (OB_FAIL(mark_row_failed_(task, true))) {
      LOG_ERROR("failed to mark/drop failed ai task", KR(ret));
    }
  }
  return ret;
}

int ObAiColumnTaskWorker::process_group_(common::ObIArray<TaskInfo> &group)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator group_allocator(ObModIds::OB_MODULE_PAGE_ALLOCATOR);
  ObSEArray<ObString, 4> results;
  ObSEArray<ColumnMeta, 4> metas;
  bool group_ok = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < group.count(); ++i) {
    const TaskInfo &task = group.at(i);
    ColumnMeta meta;
    ObString prompt;
    ObString result;
    int tmp_ret = OB_SUCCESS;
    bool drop = false;
    if (OB_SUCCESS != (tmp_ret = load_column_meta_(task, meta))) {
      LOG_ERROR("ai task meta missing", K(tmp_ret), K(task.table_id_), K(task.column_id_));
      drop = true;
    } else if (OB_SUCCESS != (tmp_ret = read_prompt_(task.table_id_, task.expr_,
                                                      task.rowkey_predicate_,
                                                      group_allocator, prompt))) {
      LOG_ERROR("ai task source row missing", K(tmp_ret), K(task.table_id_));
      drop = true;
    } else {
      // bound the endpoint call in the background worker so that a dead or
      // hanging endpoint cannot block the task scheduler
      const int64_t saved_timeout_ts = THIS_WORKER.get_timeout_ts();
      THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 3 * 1000 * 1000);
      const int call_ret = call_ai_(group_allocator, task, prompt, result);
      THIS_WORKER.set_timeout_ts(saved_timeout_ts);
      if (OB_SUCCESS != call_ret) {
        tmp_ret = call_ret;
        LOG_ERROR("ai task model call failed", K(tmp_ret), K(task.table_id_), K(task.column_id_));
      }
    }
    if (OB_SUCC(ret)) {
      if (drop) {
        (void)drop_task_(task);
        group_ok = false;
      } else if (OB_SUCCESS != tmp_ret) {
        (void)retry_or_fail_task_(task, meta.max_retries_);
        group_ok = false;
      } else if (OB_FAIL(results.push_back(result)) || OB_FAIL(metas.push_back(meta))) {
      }
    }
  }
  if (OB_SUCC(ret) && group_ok && !group.empty()) {
    if (OB_FAIL(publish_group_(group, results, metas))) {
      LOG_ERROR("failed to publish ai task group", KR(ret));
    }
  }
  // whatever happened, do not let one bad group block the worker
  return ret;
}

int ObAiColumnTaskWorker::publish_group_(common::ObIArray<TaskInfo> &group,
                                         common::ObIArray<common::ObString> &results,
                                         common::ObIArray<ColumnMeta> &metas)
{
  int ret = OB_SUCCESS;
  if (group.empty() || results.count() != group.count() || metas.count() != group.count()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const TaskInfo &head = group.at(0);
    ObSchemaGetterGuard guard;
    const ObTableSchema *table = NULL;
    const ObDatabaseSchema *db = NULL;
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(guard))) {
    } else if (OB_FAIL(guard.get_table_schema(head.table_id_, table))) {
    } else if (OB_ISNULL(table)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < group.count(); ++i) {
        (void)drop_task_(group.at(i));
      }
      ret = OB_SUCCESS;
    } else if (OB_FAIL(guard.get_database_schema(table->get_database_id(), db))) {
    } else if (OB_ISNULL(db)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(GCTX.sql_proxy_))) {
      } else {
        bool all_affected = true;
        for (int64_t i = 0; OB_SUCC(ret) && i < group.count(); ++i) {
          const TaskInfo &task = group.at(i);
          const ColumnMeta &meta = metas.at(i);
          const ObColumnSchemaV2 *result_col = NULL;
          const ObColumnSchemaV2 *state_col = NULL;
          const ObColumnSchemaV2 *request_col = NULL;
          ObSqlString update_sql;
          int64_t affected_rows = 0;
          if (OB_ISNULL(result_col = table->get_column_schema(meta.result_column_id_))
              || OB_ISNULL(state_col = table->get_column_schema(meta.state_column_id_))
              || OB_ISNULL(request_col = table->get_column_schema(meta.request_column_id_))) {
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(update_sql.append_fmt(
                       "UPDATE `%.*s`.`%.*s` SET `%.*s` = '",
                       db->get_database_name_str().length(), db->get_database_name_str().ptr(),
                       table->get_table_name_str().length(), table->get_table_name_str().ptr(),
                       result_col->get_column_name_str().length(),
                       result_col->get_column_name_str().ptr()))) {
          } else if (OB_FAIL(escape_sql_string(update_sql, results.at(i).ptr(), results.at(i).length()))) {
          } else if (OB_FAIL(update_sql.append_fmt("', `%.*s` = 1 WHERE %.*s AND `%.*s` = '",
                         state_col->get_column_name_str().length(),
                         state_col->get_column_name_str().ptr(),
                         task.rowkey_predicate_.length(), task.rowkey_predicate_.ptr(),
                         request_col->get_column_name_str().length(),
                         request_col->get_column_name_str().ptr()))) {
          } else if (OB_FAIL(escape_sql_string(update_sql, task.request_id_.ptr(),
                                               task.request_id_.length()))) {
          } else if (OB_FAIL(update_sql.append("'"))) {
          } else if (OB_FAIL(trans.write(update_sql.ptr(), affected_rows))) {
            LOG_ERROR("failed to publish ai result", KR(ret), K(update_sql));
          } else if (1 != affected_rows) {
            all_affected = false;
            LOG_INFO("ai task result superseded", K(task.table_id_), K(task.column_id_));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (all_affected) {
          ObSqlString delete_sql;
          if (OB_FAIL(delete_sql.append_fmt(
                  "DELETE FROM %s WHERE table_id = %lu AND request_id = '",
                  AI_COLUMN_TASK_TABLE, head.table_id_))) {
          } else if (OB_FAIL(escape_sql_string(delete_sql, head.request_id_.ptr(),
                                               head.request_id_.length()))) {
          } else if (OB_FAIL(delete_sql.append("'"))) {
          } else {
            int64_t delete_rows = 0;
            if (OB_FAIL(trans.write(delete_sql.ptr(), delete_rows))) {
            }
          }
        }
        if (trans.is_started()) {
          const int end_ret = trans.end(OB_SUCC(ret));
          if (OB_SUCCESS != end_ret && OB_SUCCESS == ret) {
            ret = end_ret;
          }
        }
        if (OB_SUCC(ret) && !all_affected) {
          // stale group: remove the outdated tasks, keep the newer results
          for (int64_t i = 0; OB_SUCC(ret) && i < group.count(); ++i) {
            (void)drop_task_(group.at(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObAiColumnTaskWorker::load_tasks_(common::ObIAllocator &allocator,
                                      common::ObIArray<TaskInfo> &tasks)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::MySQLResult res;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(sql.append_fmt(
                 "SELECT table_id, request_id, column_id, state, retry_count, ai_function, "
                 "model_name, result_type, expr, rowkey_predicate FROM %s "
                 "WHERE state IN (0, 3) AND eligible_ts <= now(6) "
                 "ORDER BY eligible_ts LIMIT 16", AI_COLUMN_TASK_TABLE))) {
  } else if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {
    LOG_ERROR("failed to read ai column tasks", KR(ret), K(sql));
  } else {
    sqlclient::ObMySQLResult *row = res.get_result();
    if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
    }
    while (OB_SUCC(ret) && OB_SUCC(row->next())) {
      TaskInfo task;
      const int64_t parse_row_no = tasks.count();
      if (OB_FAIL(read_uint_cell(*row, "table_id", task.table_id_))) {
      } else if (OB_FAIL(read_uint_cell(*row, "column_id", task.column_id_))) {
      } else if (OB_FAIL(read_string_cell(*row, "request_id", allocator, task.request_id_))) {
      } else if (OB_FAIL(read_string_cell(*row, "rowkey_predicate", allocator, task.rowkey_predicate_))) {
      } else if (OB_FAIL(read_string_cell(*row, "expr", allocator, task.expr_))) {
      } else if (OB_FAIL(read_string_cell(*row, "model_name", allocator, task.model_name_))) {
      } else if (OB_FAIL(read_int_cell(*row, "ai_function", task.ai_function_))) {
      } else if (OB_FAIL(read_int_cell(*row, "result_type", task.result_type_))) {
      } else if (OB_FAIL(read_int_cell(*row, "retry_count", task.retry_count_))) {
      } else if (OB_FAIL(tasks.push_back(task))) {
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("ai task row parse failed", KR(ret), K(parse_row_no), K(task.table_id_));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObAiColumnTaskWorker::load_column_meta_(const TaskInfo &task, ColumnMeta &meta)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::MySQLResult res;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(sql.append_fmt(
                 "SELECT state_column_id, request_column_id, result_spec_column_id, max_retries "
                 "FROM %s WHERE table_id = %lu AND column_id = %lu",
                 AI_COLUMN_META_TABLE, task.table_id_, task.column_id_))) {
  } else if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {
  } else {
    sqlclient::ObMySQLResult *row = res.get_result();
    if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(row->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else if (OB_FAIL(read_uint_cell(*row, "state_column_id", meta.state_column_id_))) {
    } else if (OB_FAIL(read_uint_cell(*row, "request_column_id", meta.request_column_id_))) {
    } else if (OB_FAIL(read_uint_cell(*row, "result_spec_column_id", meta.result_spec_column_id_))) {
    } else if (OB_FAIL(read_int_cell(*row, "max_retries", meta.max_retries_))) {
    } else {
      meta.result_column_id_ = task.column_id_;
    }
  }
  return ret;
}

int ObAiColumnTaskWorker::read_prompt_(const uint64_t table_id,
                                       const common::ObString &expr,
                                       const common::ObString &predicate,
                                       common::ObIAllocator &allocator,
                                       common::ObString &prompt)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  const ObTableSchema *table = nullptr;
  const ObDatabaseSchema *db = nullptr;
  ObSqlString sql;
  ObMySQLProxy::MySQLResult res;
  prompt.reset();
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(guard))) {
  } else if (OB_FAIL(guard.get_table_schema(table_id, table))) {
  } else if (OB_ISNULL(table)) {
    ret = OB_TABLE_NOT_EXIST;
  } else if (OB_FAIL(guard.get_database_schema(table->get_database_id(), db))) {
  } else if (OB_ISNULL(db)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(sql.append_fmt("SELECT (%.*s) AS ai_prompt FROM `%.*s`.`%.*s` WHERE %.*s",
                                    expr.length(), expr.ptr(),
                                    db->get_database_name_str().length(),
                                    db->get_database_name_str().ptr(),
                                    table->get_table_name_str().length(),
                                    table->get_table_name_str().ptr(),
                                    predicate.length(), predicate.ptr()))) {
  } else if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {
    LOG_WARN("failed to read ai prompt", KR(ret), K(sql));
  } else {
    sqlclient::ObMySQLResult *row = res.get_result();
    if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(row->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else if (OB_FAIL(read_string_cell(*row, "ai_prompt", allocator, prompt))) {
    } else if (prompt.empty()) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObAiColumnTaskWorker::call_ai_(common::ObIAllocator &allocator,
                                   const TaskInfo &task,
                                   common::ObString &prompt,
                                   common::ObString &result)
{
  int ret = OB_SUCCESS;
  query::ObIAiEndpointResolver *resolver =
      share::server_service<query::ObIAiEndpointResolver>();
  share::ObAiModelEndpointInfo endpoint;
  ObString access_key;
  ObArray<ObString> headers;
  ObJsonObject *body = nullptr;
  ObJsonObject *response = nullptr;
  ObIJsonBase *result_base = nullptr;
  ObAIFuncClient client;
  // the background worker must never block on a dead endpoint: use a short
  // per-attempt timeout and let the task-level retry policy handle failures
  client.set_timeout_sec(2);
  client.set_max_retry_times(0);
  result.reset();
  if (OB_ISNULL(resolver)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(resolver->resolve_by_model_name(task.model_name_, allocator, endpoint, false))) {
  } else if (OB_FAIL(endpoint.get_unencrypted_access_key(allocator, access_key))) {
  } else if (AI_COLUMN_FUNC_EMBED == task.ai_function_) {
    ObAIFuncIEmbed *embed = nullptr;
    ObArray<ObString> contents;
    ObString request_model_name = endpoint.get_request_model_name();
    if (OB_FAIL(ObAIFuncUtils::get_embed_provider(allocator, endpoint.get_provider(), embed))) {
    } else if (OB_ISNULL(embed)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(embed->get_header(allocator, access_key, headers))) {
    } else if (OB_FAIL(contents.push_back(prompt))) {
    } else if (OB_FAIL(embed->get_body(allocator, request_model_name,
                                       contents, nullptr, body))) {
    } else if (OB_FAIL(client.send_post(allocator, endpoint.get_url(), headers, body, response))) {
    } else if (OB_FAIL(embed->parse_output(allocator, response, result_base))) {
    } else if (OB_NOT_NULL(result_base)
               && ObJsonNodeType::J_ARRAY == result_base->json_type()) {
      // parse_embed_output returns an array of embedding arrays, take the first one
      ObIJsonBase *first_embedding = NULL;
      ObJsonReaderHelper json_reader(allocator);
      if (OB_FAIL(json_reader.get_array_element(result_base, 0, first_embedding))) {
      } else if (OB_ISNULL(first_embedding)
                 || ObJsonNodeType::J_ARRAY != first_embedding->json_type()) {
        ret = OB_INVALID_DATA;
      } else {
        result_base = first_embedding;
      }
    }
  } else {
    ObAIFuncIComplete *complete = nullptr;
    ObString system_hint(static_cast<int32_t>(strlen(type_hint_for(task.result_type_))),
                         type_hint_for(task.result_type_));
    ObString request_model_name = endpoint.get_request_model_name();
    if (OB_FAIL(ObAIFuncUtils::get_complete_provider(allocator, endpoint.get_provider(), complete))) {
    } else if (OB_ISNULL(complete)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(complete->get_header(allocator, access_key, headers))) {
    } else if (OB_FAIL(complete->get_body(allocator, request_model_name,
                                          system_hint, prompt, nullptr, body))) {
    } else if (OB_FAIL(client.send_post(allocator, endpoint.get_url(), headers, body, response))) {
    } else if (OB_FAIL(complete->parse_output(allocator, response, result_base))) {
    }
  }
  if (OB_SUCC(ret)) {
    ObString result_str;
    if (OB_ISNULL(result_base)) {
      ret = OB_INVALID_DATA;
    } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(allocator, result_base, result_str))) {
    } else if (result_str.empty()) {
      ret = OB_INVALID_DATA;
    } else if (OB_FAIL(copy_string(allocator, result_str, result))) {
    }
  }
  return ret;
}

int ObAiColumnTaskWorker::publish_result_(const TaskInfo &task,
                                          const ColumnMeta &meta,
                                          const common::ObString &result)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  const ObTableSchema *table = nullptr;
  const ObDatabaseSchema *db = nullptr;
  const ObColumnSchemaV2 *result_col = nullptr;
  const ObColumnSchemaV2 *state_col = nullptr;
  const ObColumnSchemaV2 *request_col = nullptr;
  ObSqlString update_sql;
  ObSqlString delete_sql;
  ObMySQLTransaction trans;
  int64_t affected_rows = 0;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(guard))) {
  } else if (OB_FAIL(guard.get_table_schema(task.table_id_, table))) {
  } else if (OB_ISNULL(table)) {
    ret = OB_TABLE_NOT_EXIST;
  } else if (OB_FAIL(guard.get_database_schema(table->get_database_id(), db))) {
  } else if (OB_ISNULL(db)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(result_col = table->get_column_schema(meta.result_column_id_))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(state_col = table->get_column_schema(meta.state_column_id_))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(request_col = table->get_column_schema(meta.request_column_id_))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(update_sql.append_fmt(
                 "UPDATE `%.*s`.`%.*s` SET `%.*s` = '",
                 db->get_database_name_str().length(), db->get_database_name_str().ptr(),
                 table->get_table_name_str().length(), table->get_table_name_str().ptr(),
                 result_col->get_column_name_str().length(),
                 result_col->get_column_name_str().ptr()))) {
  } else if (OB_FAIL(escape_sql_string(update_sql, result.ptr(), result.length()))) {
  } else if (OB_FAIL(update_sql.append_fmt("', `%.*s` = 1 WHERE %.*s AND `%.*s` = '",
                 state_col->get_column_name_str().length(), state_col->get_column_name_str().ptr(),
                 task.rowkey_predicate_.length(), task.rowkey_predicate_.ptr(),
                 request_col->get_column_name_str().length(),
                 request_col->get_column_name_str().ptr()))) {
  } else if (OB_FAIL(escape_sql_string(update_sql, task.request_id_.ptr(), task.request_id_.length()))) {
  } else if (OB_FAIL(update_sql.append("'"))) {
  } else if (OB_FAIL(delete_sql.append_fmt(
                 "DELETE FROM %s WHERE table_id = %lu AND request_id = '",
                 AI_COLUMN_TASK_TABLE, task.table_id_))) {
  } else if (OB_FAIL(escape_sql_string(delete_sql, task.request_id_.ptr(), task.request_id_.length()))) {
  } else if (OB_FAIL(delete_sql.append_fmt("' AND column_id = %lu", task.column_id_))) {
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_))) {
    LOG_WARN("failed to start transaction", KR(ret));
  } else {
    if (OB_FAIL(trans.write(update_sql.ptr(), affected_rows))) {
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      // stale task or row deleted: drop the task row anyway
      int64_t delete_rows = 0;
      if (OB_FAIL(trans.write(delete_sql.ptr(), delete_rows))) {
      }
    }
    if (trans.is_started()) {
      const int tmp_ret = trans.end(OB_SUCC(ret));
      if (OB_SUCCESS != tmp_ret && OB_SUCCESS == ret) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

int ObAiColumnTaskWorker::cleanup_task_(const TaskInfo &task,
                                        const bool failed,
                                        const int64_t next_retry_count)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
  } else if (failed) {
    if (OB_FAIL(sql.append_fmt(
            "UPDATE %s SET state = 2, retry_count = %ld WHERE table_id = %lu AND request_id = '",
            AI_COLUMN_TASK_TABLE, next_retry_count, task.table_id_))) {
    }
  } else if (OB_FAIL(sql.append_fmt(
            "UPDATE %s SET state = 3, retry_count = %ld, eligible_ts = now(6) + interval 1 second "
            "WHERE table_id = %lu AND request_id = '",
            AI_COLUMN_TASK_TABLE, next_retry_count, task.table_id_))) {
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(escape_sql_string(sql, task.request_id_.ptr(), task.request_id_.length()))) {
  } else if (OB_FAIL(sql.append_fmt("' AND column_id = %lu", task.column_id_))) {
  } else if (OB_FAIL(sql_proxy_->write(sql.string(), affected_rows))) {
    LOG_WARN("failed to update ai task state", KR(ret), K(sql));
  }
  return ret;
}

int ObAiColumnTaskWorker::drop_task_(const TaskInfo &task)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(sql.append_fmt(
                 "DELETE FROM %s WHERE table_id = %lu AND request_id = '",
                 AI_COLUMN_TASK_TABLE, task.table_id_))) {
  } else if (OB_FAIL(escape_sql_string(sql, task.request_id_.ptr(), task.request_id_.length()))) {
  } else if (OB_FAIL(sql.append_fmt("' AND column_id = %lu", task.column_id_))) {
  } else if (OB_FAIL(sql_proxy_->write(sql.string(), affected_rows))) {
    LOG_WARN("failed to drop ai task", KR(ret), K(sql));
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
