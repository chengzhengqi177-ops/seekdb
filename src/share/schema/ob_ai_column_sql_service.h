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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_AI_COLUMN_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_AI_COLUMN_SQL_SERVICE_H_

#include "share/schema/ob_ddl_sql_service.h"

namespace oceanbase
{
namespace common
{
class ObString;
class ObISQLClient;
}
namespace share
{
namespace schema
{
class ObTableSchema;
class ObColumnSchemaV2;
class ObAiColumnInfo;

class ObAiColumnSqlService final : public ObDDLSqlService
{
public:
  explicit ObAiColumnSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service)
  { }
  virtual ~ObAiColumnSqlService() = default;

  // write one __all_ai_column row for every AI column of the table,
  // inside the caller's DDL transaction
  int insert_ai_columns(const ObTableSchema &table,
                        common::ObISQLClient &sql_client);

  int insert_ai_column(const ObTableSchema &table,
                       const ObColumnSchemaV2 &column,
                       const ObAiColumnInfo &info,
                       common::ObISQLClient &sql_client);

  int delete_ai_column(const uint64_t table_id,
                       const uint64_t column_id,
                       common::ObISQLClient &sql_client);

  int delete_ai_columns(const uint64_t table_id,
                        common::ObISQLClient &sql_client);

private:
  DISALLOW_COPY_AND_ASSIGN(ObAiColumnSqlService);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SCHEMA_OB_AI_COLUMN_SQL_SERVICE_H_
