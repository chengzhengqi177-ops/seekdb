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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_AI_COLUMN_DML_HANDLER_H_
#define OCEANBASE_SQL_ENGINE_DML_OB_AI_COLUMN_DML_HANDLER_H_

#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace sql
{
class ObTableModifyOp;
struct ObDMLBaseCtDef;
struct ObDMLBaseRtDef;

// Creates / invalidates the AI column tasks of the modified rows, inside the
// current DML transaction. Called from ObDMLService::handle_after_row_processing.
class ObAiColumnDMLHandler
{
public:
  // extract the prompt (or embedding input) sub expression of a serialized AI
  // column expression like AI_COMPLETE('model', concat('x', c))
  static int extract_prompt_expr(const common::ObString &expr,
                                 common::ObString &prompt_expr);
  static int handle_row(ObTableModifyOp &op,
                        const ObDMLBaseCtDef &dml_ctdef,
                        ObDMLBaseRtDef &dml_rtdef,
                        const common::ObDmlEventType dml_event);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAiColumnDMLHandler);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_DML_OB_AI_COLUMN_DML_HANDLER_H_
