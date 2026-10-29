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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_AI_COLUMN_INFO_H_
#define OCEANBASE_SHARE_SCHEMA_OB_AI_COLUMN_INFO_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/utility/serialization.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

// AI function type of an AI column. The values are self-contained on purpose:
// the schema layer must not depend on the SQL layer (expr operator types).
enum ObAiColumnFunctionType
{
  AI_COLUMN_FUNC_INVALID = 0,
  AI_COLUMN_FUNC_COMPLETE = 1,
  AI_COLUMN_FUNC_EMBED = 2,
  AI_COLUMN_FUNC_RERANK = 3,
  AI_COLUMN_FUNC_PROMPT = 4,
  AI_COLUMN_FUNC_MAX
};

// sync_mode of an AI column: 0 = sync (value filled on write), 1 = async (task based).
enum ObAiColumnSyncMode
{
  AI_COLUMN_SYNC_MODE = 0,
  AI_COLUMN_ASYNC_MODE = 1,
  AI_COLUMN_SYNC_MODE_MAX
};

// Per-column metadata of an AI column.
//
// The object is always owned by an allocator (the one of the column schema that
// holds it), therefore it has no default constructor: use
// ObColumnSchemaV2::set_ai_column_info() which constructs it on the column
// allocator. All the strings are deep copied into that allocator and are
// released together with the allocator (arena style), reset() only drops the
// references.
//
// Serialization format (all integers are vi64, strings are vstr):
//   format_version
//   sync_mode
//   max_retries
//   definition_version
//   state_column_id
//   request_column_id
//   result_spec_column_id
//   ai_function
//   result_type
//   expr_sql
//   normalized_expr
//   model_name
//   dependency_count
//   dependency_column_id * dependency_count
class ObAiColumnInfo
{
public:
  static const int64_t AI_COLUMN_INFO_VERSION = 1;
  static const int64_t DEFAULT_SYNC_MODE = AI_COLUMN_ASYNC_MODE;
  static const int64_t DEFAULT_MAX_RETRIES = 0;
  static const int64_t DEFAULT_DEFINITION_VERSION = 1;
  static const int64_t DEPS_LOCAL_CAPACITY = 4;
  typedef common::ObSEArray<uint64_t, DEPS_LOCAL_CAPACITY> DependencyColumnIds;

  explicit ObAiColumnInfo(common::ObIAllocator &allocator)
      : allocator_(allocator),
        sync_mode_(DEFAULT_SYNC_MODE),
        max_retries_(DEFAULT_MAX_RETRIES),
        definition_version_(DEFAULT_DEFINITION_VERSION),
        state_column_id_(0),
        request_column_id_(0),
        result_spec_column_id_(0),
        ai_function_(AI_COLUMN_FUNC_INVALID),
        result_type_(0),
        expr_sql_(),
        normalized_expr_(),
        model_name_(),
        dependency_column_ids_()
  {
  }
  ~ObAiColumnInfo() {}

  void reset()
  {
    sync_mode_ = DEFAULT_SYNC_MODE;
    max_retries_ = DEFAULT_MAX_RETRIES;
    definition_version_ = DEFAULT_DEFINITION_VERSION;
    state_column_id_ = 0;
    request_column_id_ = 0;
    result_spec_column_id_ = 0;
    ai_function_ = AI_COLUMN_FUNC_INVALID;
    result_type_ = 0;
    expr_sql_.reset();
    normalized_expr_.reset();
    model_name_.reset();
    dependency_column_ids_.reset();
  }

  bool is_valid() const { return definition_version_ > 0; }

  // Deep copy from another info object (allocator of *this is used).
  int assign(const ObAiColumnInfo &other)
  {
    int ret = common::OB_SUCCESS;
    if (this != &other) {
      reset();
      sync_mode_ = other.sync_mode_;
      max_retries_ = other.max_retries_;
      definition_version_ = other.definition_version_;
      state_column_id_ = other.state_column_id_;
      request_column_id_ = other.request_column_id_;
      result_spec_column_id_ = other.result_spec_column_id_;
      ai_function_ = other.ai_function_;
      result_type_ = other.result_type_;
      if (OB_FAIL(deep_copy_str_(other.expr_sql_, expr_sql_))) {
      } else if (OB_FAIL(deep_copy_str_(other.normalized_expr_, normalized_expr_))) {
      } else if (OB_FAIL(deep_copy_str_(other.model_name_, model_name_))) {
      } else if (OB_FAIL(dependency_column_ids_.assign(other.dependency_column_ids_))) {
      }
    }
    return ret;
  }

  int64_t get_convert_size() const
  {
    int64_t convert_size = static_cast<int64_t>(sizeof(*this));
    convert_size += expr_sql_.length();
    convert_size += normalized_expr_.length();
    convert_size += model_name_.length();
    if (dependency_column_ids_.count() > DEPS_LOCAL_CAPACITY) {
      convert_size += (dependency_column_ids_.count() - DEPS_LOCAL_CAPACITY)
                      * static_cast<int64_t>(sizeof(uint64_t));
    }
    return convert_size;
  }

  // Remap state/request/result_spec column ids and dependency column ids with
  // the given map. Ids missing from the map are left unchanged.
  int convert_column_id(const common::hash::ObHashMap<uint64_t, uint64_t> &column_id_map)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(convert_one_column_id_(column_id_map, state_column_id_))) {
    } else if (OB_FAIL(convert_one_column_id_(column_id_map, request_column_id_))) {
    } else if (OB_FAIL(convert_one_column_id_(column_id_map, result_spec_column_id_))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < dependency_column_ids_.count(); ++i) {
        if (OB_FAIL(convert_one_column_id_(column_id_map, dependency_column_ids_.at(i)))) {
        }
      }
    }
    return ret;
  }

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    using namespace common::serialization;
    int ret = common::OB_SUCCESS;
    int64_t dependency_count = dependency_column_ids_.count();
    if (OB_FAIL(encode_vi64(buf, buf_len, pos, AI_COLUMN_INFO_VERSION))) {
    } else if (OB_FAIL(encode_vi64(buf, buf_len, pos, sync_mode_))) {
    } else if (OB_FAIL(encode_vi64(buf, buf_len, pos, max_retries_))) {
    } else if (OB_FAIL(encode_vi64(buf, buf_len, pos, definition_version_))) {
    } else if (OB_FAIL(encode_vi64(buf, buf_len, pos, static_cast<int64_t>(state_column_id_)))) {
    } else if (OB_FAIL(encode_vi64(buf, buf_len, pos, static_cast<int64_t>(request_column_id_)))) {
    } else if (OB_FAIL(encode_vi64(buf, buf_len, pos, static_cast<int64_t>(result_spec_column_id_)))) {
    } else if (OB_FAIL(encode_vi64(buf, buf_len, pos, ai_function_))) {
    } else if (OB_FAIL(encode_vi64(buf, buf_len, pos, result_type_))) {
    } else if (OB_FAIL(encode_str_(buf, buf_len, pos, expr_sql_))) {
    } else if (OB_FAIL(encode_str_(buf, buf_len, pos, normalized_expr_))) {
    } else if (OB_FAIL(encode_str_(buf, buf_len, pos, model_name_))) {
    } else if (OB_FAIL(encode_vi64(buf, buf_len, pos, dependency_count))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < dependency_count; ++i) {
        if (OB_FAIL(encode_vi64(buf, buf_len, pos,
                                static_cast<int64_t>(dependency_column_ids_.at(i))))) {
        }
      }
    }
    return ret;
  }

  int deserialize(const char *buf, const int64_t data_len, int64_t &pos)
  {
    using namespace common::serialization;
    int ret = common::OB_SUCCESS;
    int64_t format_version = 0;
    int64_t state_column_id = 0;
    int64_t request_column_id = 0;
    int64_t result_spec_column_id = 0;
    int64_t dependency_count = 0;
    reset();
    if (OB_FAIL(decode_vi64(buf, data_len, pos, &format_version))) {
    } else if (OB_UNLIKELY(AI_COLUMN_INFO_VERSION != format_version)) {
      ret = common::OB_DESERIALIZE_ERROR;
    } else if (OB_FAIL(decode_vi64(buf, data_len, pos, &sync_mode_))) {
    } else if (OB_FAIL(decode_vi64(buf, data_len, pos, &max_retries_))) {
    } else if (OB_FAIL(decode_vi64(buf, data_len, pos, &definition_version_))) {
    } else if (OB_FAIL(decode_vi64(buf, data_len, pos, &state_column_id))) {
    } else if (OB_FAIL(decode_vi64(buf, data_len, pos, &request_column_id))) {
    } else if (OB_FAIL(decode_vi64(buf, data_len, pos, &result_spec_column_id))) {
    } else if (OB_FAIL(decode_vi64(buf, data_len, pos, &ai_function_))) {
    } else if (OB_FAIL(decode_vi64(buf, data_len, pos, &result_type_))) {
    } else if (OB_FAIL(decode_str_(buf, data_len, pos, expr_sql_))) {
    } else if (OB_FAIL(decode_str_(buf, data_len, pos, normalized_expr_))) {
    } else if (OB_FAIL(decode_str_(buf, data_len, pos, model_name_))) {
    } else if (OB_FAIL(decode_vi64(buf, data_len, pos, &dependency_count))) {
    } else if (OB_UNLIKELY(dependency_count < 0)) {
      ret = common::OB_DESERIALIZE_ERROR;
    } else {
      state_column_id_ = static_cast<uint64_t>(state_column_id);
      request_column_id_ = static_cast<uint64_t>(request_column_id);
      result_spec_column_id_ = static_cast<uint64_t>(result_spec_column_id);
      for (int64_t i = 0; OB_SUCC(ret) && i < dependency_count; ++i) {
        int64_t dependency_column_id = 0;
        if (OB_FAIL(decode_vi64(buf, data_len, pos, &dependency_column_id))) {
        } else if (OB_FAIL(dependency_column_ids_.push_back(
                     static_cast<uint64_t>(dependency_column_id)))) {
        }
      }
    }
    return ret;
  }

  int64_t get_serialize_size() const
  {
    using namespace common::serialization;
    int64_t len = 0;
    len += encoded_length_vi64(AI_COLUMN_INFO_VERSION);
    len += encoded_length_vi64(sync_mode_);
    len += encoded_length_vi64(max_retries_);
    len += encoded_length_vi64(definition_version_);
    len += encoded_length_vi64(static_cast<int64_t>(state_column_id_));
    len += encoded_length_vi64(static_cast<int64_t>(request_column_id_));
    len += encoded_length_vi64(static_cast<int64_t>(result_spec_column_id_));
    len += encoded_length_vi64(ai_function_);
    len += encoded_length_vi64(result_type_);
    len += encoded_length_vstr(static_cast<int64_t>(expr_sql_.length()));
    len += encoded_length_vstr(static_cast<int64_t>(normalized_expr_.length()));
    len += encoded_length_vstr(static_cast<int64_t>(model_name_.length()));
    len += encoded_length_vi64(dependency_column_ids_.count());
    for (int64_t i = 0; i < dependency_column_ids_.count(); ++i) {
      len += encoded_length_vi64(static_cast<int64_t>(dependency_column_ids_.at(i)));
    }
    return len;
  }

  // getters / setters
  int set_expr_sql(const common::ObString &expr_sql) { return deep_copy_str_(expr_sql, expr_sql_); }
  int set_normalized_expr(const common::ObString &normalized_expr)
  { return deep_copy_str_(normalized_expr, normalized_expr_); }
  int set_model_name(const common::ObString &model_name)
  { return deep_copy_str_(model_name, model_name_); }
  void set_sync_mode(const int64_t sync_mode) { sync_mode_ = sync_mode; }
  void set_max_retries(const int64_t max_retries) { max_retries_ = max_retries; }
  void set_definition_version(const int64_t definition_version)
  { definition_version_ = definition_version; }
  void set_state_column_id(const uint64_t column_id) { state_column_id_ = column_id; }
  void set_request_column_id(const uint64_t column_id) { request_column_id_ = column_id; }
  void set_result_spec_column_id(const uint64_t column_id) { result_spec_column_id_ = column_id; }
  void set_ai_function(const int64_t ai_function) { ai_function_ = ai_function; }
  void set_result_type(const int64_t result_type) { result_type_ = result_type; }
  int add_dependency_column_id(const uint64_t column_id)
  { return dependency_column_ids_.push_back(column_id); }
  int set_dependency_column_ids(const common::ObIArray<uint64_t> &column_ids)
  { return dependency_column_ids_.assign(column_ids); }
  void clear_dependency_column_ids() { dependency_column_ids_.reset(); }

  const common::ObString &get_expr_sql() const { return expr_sql_; }
  const common::ObString &get_normalized_expr() const { return normalized_expr_; }
  const common::ObString &get_model_name() const { return model_name_; }
  int64_t get_sync_mode() const { return sync_mode_; }
  int64_t get_max_retries() const { return max_retries_; }
  int64_t get_definition_version() const { return definition_version_; }
  uint64_t get_state_column_id() const { return state_column_id_; }
  uint64_t get_request_column_id() const { return request_column_id_; }
  uint64_t get_result_spec_column_id() const { return result_spec_column_id_; }
  int64_t get_ai_function() const { return ai_function_; }
  int64_t get_result_type() const { return result_type_; }
  const DependencyColumnIds &get_dependency_column_ids() const { return dependency_column_ids_; }
  DependencyColumnIds &get_dependency_column_ids() { return dependency_column_ids_; }

  TO_STRING_KV(K_(sync_mode),
               K_(max_retries),
               K_(definition_version),
               K_(state_column_id),
               K_(request_column_id),
               K_(result_spec_column_id),
               K_(ai_function),
               K_(result_type),
               K_(expr_sql),
               K_(normalized_expr),
               K_(model_name),
               K_(dependency_column_ids));

private:
  static int deep_copy_str_(const common::ObString &src,
                            common::ObString &dst,
                            common::ObIAllocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    dst.reset();
    if (!src.empty()) {
      char *buf = static_cast<char *>(allocator.alloc(src.length()));
      if (OB_ISNULL(buf)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(buf, src.ptr(), src.length());
        dst.assign_ptr(buf, src.length());
      }
    }
    return ret;
  }
  int deep_copy_str_(const common::ObString &src, common::ObString &dst)
  { return deep_copy_str_(src, dst, allocator_); }

  static int encode_str_(char *buf, const int64_t buf_len, int64_t &pos,
                         const common::ObString &str)
  {
    return common::serialization::encode_vstr(buf, buf_len, pos,
                                              str.ptr(), static_cast<int64_t>(str.length()));
  }
  int decode_str_(const char *buf, const int64_t data_len, int64_t &pos,
                  common::ObString &str)
  {
    int ret = common::OB_SUCCESS;
    int64_t len = 0;
    str.reset();
    const char *src = common::serialization::decode_vstr(buf, data_len, pos, &len);
    if (OB_ISNULL(src) || OB_UNLIKELY(len < 0)) {
      ret = common::OB_DESERIALIZE_ERROR;
    } else if (len > 0) {
      char *copy = static_cast<char *>(allocator_.alloc(len));
      if (OB_ISNULL(copy)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(copy, src, len);
        str.assign_ptr(copy, static_cast<int32_t>(len));
      }
    }
    return ret;
  }
  int convert_one_column_id_(const common::hash::ObHashMap<uint64_t, uint64_t> &column_id_map,
                             uint64_t &column_id)
  {
    int ret = common::OB_SUCCESS;
    if (0 != column_id) {
      uint64_t new_column_id = 0;
      int tmp_ret = column_id_map.get_refactored(column_id, new_column_id);
      if (common::OB_SUCCESS == tmp_ret) {
        column_id = new_column_id;
      } else if (common::OB_HASH_NOT_EXIST != tmp_ret) {
        ret = tmp_ret;
      }
    }
    return ret;
  }

private:
  common::ObIAllocator &allocator_;
  int64_t sync_mode_;
  int64_t max_retries_;
  int64_t definition_version_;
  uint64_t state_column_id_;
  uint64_t request_column_id_;
  uint64_t result_spec_column_id_;
  int64_t ai_function_;
  int64_t result_type_;
  common::ObString expr_sql_;
  common::ObString normalized_expr_;
  common::ObString model_name_;
  DependencyColumnIds dependency_column_ids_;

  DISALLOW_COPY_AND_ASSIGN(ObAiColumnInfo);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SCHEMA_OB_AI_COLUMN_INFO_H_
