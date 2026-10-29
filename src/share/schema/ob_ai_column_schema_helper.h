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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_AI_COLUMN_SCHEMA_HELPER_H_
#define OCEANBASE_SHARE_SCHEMA_OB_AI_COLUMN_SCHEMA_HELPER_H_

#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_ai_column_info.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

// DDL helpers shared by CREATE TABLE / ALTER TABLE ADD COLUMN:
//  - fill the runtime metadata of an AI column (result type + prompt type hint)
//  - build the 3 hidden columns (state / request / result spec)
class ObAiColumnSchemaHelper
{
public:
  enum HiddenColumnKind
  {
    AI_STATE_COLUMN = 0,
    AI_REQUEST_COLUMN = 1,
    AI_RESULT_SPEC_COLUMN = 2,
    AI_HIDDEN_COLUMN_COUNT = 3
  };

  static const char *hidden_column_suffix(const int64_t kind)
  {
    const char *suffix = NULL;
    switch (kind) {
      case AI_STATE_COLUMN: suffix = "state"; break;
      case AI_REQUEST_COLUMN: suffix = "request"; break;
      case AI_RESULT_SPEC_COLUMN: suffix = "result_spec"; break;
      default: suffix = NULL; break;
    }
    return suffix;
  }

  // fill result_type_ and normalized_expr_ (append the prompt type hint)
  static int fill_ai_column_metadata(ObColumnSchemaV2 &column)
  {
    int ret = OB_SUCCESS;
    ObAiColumnInfo *info = column.get_ai_column_info();
    if (OB_ISNULL(info)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ai column info is null", KR(ret), K(column.get_column_id()));
    } else {
      const ObObjType type = column.get_data_type();
      info->set_result_type(static_cast<int64_t>(type));
      const char *hint = NULL;
      if (ObJsonType == type) {
        hint = " Return exactly one valid JSON value.";
      } else if (ob_is_integer_type(type)) {
        hint = " Return a base-10 integer.";
      } else if (ob_is_collection_sql_type(type)) {
        hint = " Return a JSON array of numbers.";
      }
      if (NULL != hint && OB_FAIL(info->set_normalized_expr(
              ObString(static_cast<int32_t>(strlen(hint)), hint)))) {
        LOG_WARN("failed to set normalized expr", KR(ret));
      } else if (NULL != hint && !info->get_expr_sql().empty()) {
        ObSqlString normalized;
        if (OB_FAIL(normalized.append(info->get_expr_sql()))) {
          LOG_WARN("failed to append expr", KR(ret));
        } else if (OB_FAIL(normalized.append(hint))) {
          LOG_WARN("failed to append hint", KR(ret));
        } else if (OB_FAIL(info->set_normalized_expr(normalized.string()))) {
          LOG_WARN("failed to set normalized expr", KR(ret));
        }
      }
    }
    return ret;
  }

  // Build one hidden column of the given kind with a pre-allocated column id.
  // The caller must construct hidden_column with the target allocator:
  //   ObColumnSchemaV2 hidden(&allocator);
  static int make_hidden_column(const ObColumnSchemaV2 &ai_column,
                                const uint64_t table_id,
                                const uint64_t column_id,
                                const int64_t kind,
                                ObColumnSchemaV2 &hidden_column)
  {
    int ret = OB_SUCCESS;
    const char *suffix = hidden_column_suffix(kind);
    if (OB_ISNULL(suffix)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      char name_buf[common::OB_MAX_COLUMN_NAME_LENGTH + 1];
      const int64_t max_name_len = MIN(static_cast<int64_t>(common::OB_MAX_COLUMN_NAME_LENGTH - 32),
                                       static_cast<int64_t>(ai_column.get_column_name_str().length()));
      const int64_t name_len = snprintf(name_buf, sizeof(name_buf), "__ai_%.*s_%s",
                                        static_cast<int32_t>(max_name_len),
                                        ai_column.get_column_name(), suffix);
      if (OB_UNLIKELY(name_len <= 0 || name_len >= static_cast<int64_t>(sizeof(name_buf)))) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(hidden_column.set_column_name(ObString(static_cast<int32_t>(name_len), name_buf)))) {
      } else {
        hidden_column.set_table_id(table_id);
        hidden_column.set_column_id(column_id);
        hidden_column.set_is_hidden(true);
        hidden_column.set_nullable(true);
        hidden_column.set_rowkey_position(0);
        hidden_column.set_index_position(0);
        hidden_column.set_tbl_part_key_pos(0);
        hidden_column.set_prev_column_id(common::OB_INVALID_ID);
        hidden_column.set_next_column_id(common::OB_INVALID_ID);
        switch (kind) {
          case AI_STATE_COLUMN: {
            hidden_column.set_data_type(ObIntType);
            hidden_column.set_data_length(static_cast<int32_t>(sizeof(int64_t)));
            hidden_column.set_collation_type(CS_TYPE_BINARY);
            break;
          }
          case AI_REQUEST_COLUMN: {
            hidden_column.set_data_type(ObVarcharType);
            hidden_column.set_data_length(64);
            hidden_column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
            break;
          }
          case AI_RESULT_SPEC_COLUMN: {
            hidden_column.set_data_type(ObVarcharType);
            hidden_column.set_data_length(4096);
            hidden_column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
            break;
          }
          default: {
            ret = OB_INVALID_ARGUMENT;
            break;
          }
        }
      }
    }
    return ret;
  }

  static int set_hidden_column_ids(ObColumnSchemaV2 &ai_column,
                                   const uint64_t state_column_id,
                                   const uint64_t request_column_id,
                                   const uint64_t result_spec_column_id)
  {
    int ret = OB_SUCCESS;
    ObAiColumnInfo *info = ai_column.get_ai_column_info();
    if (OB_ISNULL(info)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      info->set_state_column_id(state_column_id);
      info->set_request_column_id(request_column_id);
      info->set_result_spec_column_id(result_spec_column_id);
    }
    return ret;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObAiColumnSchemaHelper);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SCHEMA_OB_AI_COLUMN_SCHEMA_HELPER_H_
