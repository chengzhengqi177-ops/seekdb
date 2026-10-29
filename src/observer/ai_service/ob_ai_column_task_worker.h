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

#ifndef OCEANBASE_OBSERVER_OB_AI_COLUMN_TASK_WORKER_H_
#define OCEANBASE_OBSERVER_OB_AI_COLUMN_TASK_WORKER_H_

#include "lib/task/ob_timer.h"
#include "lib/string/ob_string.h"
#include "common/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase
{
namespace observer
{

// Background worker of the async AI columns:
//  - scans oceanbase.__all_ai_column_task
//  - reads the prompt value of the target row
//  - calls the AI model endpoint (with the typed system prompt hint)
//  - publishes the result into the hidden result column, only if the task is
//    still the newest one for that row (request id / stale guard)
class ObAiColumnTaskWorker : public common::ObTimerTask
{
public:
  static const int64_t TASK_PERIOD = 300 * 1000L; // us
  static ObAiColumnTaskWorker &get_instance();

  ObAiColumnTaskWorker();
  virtual ~ObAiColumnTaskWorker();
  int init(common::ObMySQLProxy &sql_proxy);
  void destroy();
  virtual void runTimerTask() override;

private:
  struct TaskInfo
  {
    TaskInfo()
      : table_id_(0), column_id_(0), request_id_(), rowkey_predicate_(), expr_(),
        model_name_(), ai_function_(0), result_type_(0), retry_count_(0) {}
    uint64_t table_id_;
    uint64_t column_id_;
    common::ObString request_id_;
    common::ObString rowkey_predicate_;
    common::ObString expr_;
    common::ObString model_name_;
    int64_t ai_function_;
    int64_t result_type_;
    int64_t retry_count_;
  };
  struct ColumnMeta
  {
    ColumnMeta()
      : state_column_id_(0), request_column_id_(0), result_spec_column_id_(0),
        result_column_id_(0), max_retries_(0) {}
    uint64_t state_column_id_;
    uint64_t request_column_id_;
    uint64_t result_spec_column_id_;
    uint64_t result_column_id_;
    int64_t max_retries_;
  };

  int do_work_();
  int process_group_(common::ObIArray<TaskInfo> &group);
  int publish_group_(common::ObIArray<TaskInfo> &group,
                     common::ObIArray<common::ObString> &results,
                     common::ObIArray<ColumnMeta> &metas);
  int retry_or_fail_task_(const TaskInfo &task, const int64_t max_retries);
  int mark_row_failed_(const TaskInfo &task, const bool drop_task);
  int load_tasks_(common::ObIAllocator &allocator, common::ObIArray<TaskInfo> &tasks);
  int load_column_meta_(const TaskInfo &task, ColumnMeta &meta);
  int read_prompt_(const uint64_t table_id, const common::ObString &expr,
                   const common::ObString &predicate, common::ObIAllocator &allocator,
                   common::ObString &prompt);
  int call_ai_(common::ObIAllocator &allocator, const TaskInfo &task,
               common::ObString &prompt, common::ObString &result);
  int publish_result_(const TaskInfo &task, const ColumnMeta &meta,
                      const common::ObString &result);
  int cleanup_task_(const TaskInfo &task, const bool failed, const int64_t next_retry_count);
  int drop_task_(const TaskInfo &task);

private:
  common::ObMySQLProxy *sql_proxy_;
  common::ObTimer timer_;
  bool is_inited_;
  bool processing_;
  DISALLOW_COPY_AND_ASSIGN(ObAiColumnTaskWorker);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_AI_COLUMN_TASK_WORKER_H_
