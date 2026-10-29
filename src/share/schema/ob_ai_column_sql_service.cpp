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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_ai_column_sql_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_ai_column_info.h"
#include "share/ob_dml_sql_splicer.h"
#include "mysqlclient/ob_isql_client.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

int ObAiColumnSqlService::insert_ai_columns(const ObTableSchema &table,
                                            common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObTableSchema::const_column_iterator iter = table.column_begin();
  ObTableSchema::const_column_iterator end = table.column_end();
  for (; OB_SUCC(ret) && iter != end; ++iter) {
    const ObColumnSchemaV2 *column = *iter;
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column is null", KR(ret), K(table.get_table_id()));
    } else if (!column->is_ai_column()) {
      // skip normal columns
    } else if (OB_ISNULL(column->get_ai_column_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ai column info is null", KR(ret), K(table.get_table_id()),
               K(column->get_column_id()));
    } else if (OB_FAIL(insert_ai_column(table, *column, *column->get_ai_column_info(), sql_client))) {
      LOG_WARN("failed to insert ai column", KR(ret), K(table.get_table_id()),
               K(column->get_column_id()));
    }
  }
  return ret;
}

int ObAiColumnSqlService::insert_ai_column(const ObTableSchema &table,
                                           const ObColumnSchemaV2 &column,
                                           const ObAiColumnInfo &info,
                                           common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  int64_t affected_rows = 0;
  common::ObSqlString deps;
  const ObAiColumnInfo::DependencyColumnIds &dependency_ids = info.get_dependency_column_ids();
  for (int64_t i = 0; OB_SUCC(ret) && i < dependency_ids.count(); ++i) {
    if (OB_FAIL(deps.append_fmt("%s%lu", (0 == i) ? "" : ",", dependency_ids.at(i)))) {
      LOG_WARN("failed to append dependency column id", KR(ret), K(i));
    }
  }
  ObString deps_str;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.add_pk_column("table_id", table.get_table_id()))) {
  } else if (OB_FAIL(sql.add_pk_column("column_id", column.get_column_id()))) {
  } else if (OB_FAIL(sql.add_column("column_name", ObHexEscapeSqlStr(column.get_column_name_str())))) {
  } else if (OB_FAIL(sql.add_column("sync_mode", info.get_sync_mode()))) {
  } else if (OB_FAIL(sql.add_column("max_retries", info.get_max_retries()))) {
  } else if (OB_FAIL(sql.add_column("state_column_id", static_cast<int64_t>(info.get_state_column_id())))) {
  } else if (OB_FAIL(sql.add_column("request_column_id", static_cast<int64_t>(info.get_request_column_id())))) {
  } else if (OB_FAIL(sql.add_column("result_spec_column_id", static_cast<int64_t>(info.get_result_spec_column_id())))) {
  } else if (OB_FAIL(sql.add_column("expr", ObHexEscapeSqlStr(info.get_expr_sql())))) {
  } else if (OB_FAIL(sql.add_column("normalized_expr", ObHexEscapeSqlStr(info.get_normalized_expr())))) {
  } else if (FALSE_IT(deps_str = deps.string())) {
  } else if (OB_FAIL(sql.add_column("dependencies", ObHexEscapeSqlStr(deps_str)))) {
  } else if (OB_FAIL(sql.add_column("definition_version", info.get_definition_version()))) {
  } else if (OB_FAIL(sql.add_column("ai_function", info.get_ai_function()))) {
  } else if (OB_FAIL(sql.add_column("model_name", ObHexEscapeSqlStr(info.get_model_name())))) {
  } else if (OB_FAIL(sql.add_column("result_type", info.get_result_type()))) {
  } else if (OB_FAIL(sql.splice_insert_sql(OB_ALL_AI_COLUMN_TNAME, buffer))) {
  } else if (OB_FAIL(sql_client.write(buffer.ptr(), affected_rows))) {
  } else if (affected_rows > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObAiColumnSqlService::delete_ai_column(const uint64_t table_id,
                                           const uint64_t column_id,
                                           common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  int64_t affected_rows = 0;
  if (OB_FAIL(sql.add_pk_column("table_id", table_id))) {
  } else if (OB_FAIL(sql.add_pk_column("column_id", column_id))) {
  } else if (OB_FAIL(sql.splice_delete_sql(OB_ALL_AI_COLUMN_TNAME, buffer))) {
  } else if (OB_FAIL(sql_client.write(buffer.ptr(), affected_rows))) {
  } else if (affected_rows > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(table_id), K(column_id), K(affected_rows));
  }
  return ret;
}

int ObAiColumnSqlService::delete_ai_columns(const uint64_t table_id,
                                            common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  int64_t affected_rows = 0;
  if (OB_FAIL(sql.add_pk_column("table_id", table_id))) {
  } else if (OB_FAIL(sql.splice_delete_sql(OB_ALL_AI_COLUMN_TNAME, buffer))) {
  } else if (OB_FAIL(sql_client.write(buffer.ptr(), affected_rows))) {
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
