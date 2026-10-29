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
#include "ob_expr_ai_embed.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase 
{
namespace sql 
{

namespace
{

// one http request carries the inputs of one model endpoint
struct ObAIEmbedBatchGroup
{
  ObString model_id_;
  int64_t dimension_ = 0;
  ObArray<int64_t> row_idxs_;
  ObArray<ObString> contents_;
};

} // anonymous namespace

ObExprAIEmbed::ObExprAIEmbed(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, 
                        T_FUN_SYS_AI_EMBED, 
                        N_AI_EMBED, 
                        MORE_THAN_ZERO,
                        NOT_VALID_FOR_GENERATED_COL, 
                        NOT_ROW_DIMENSION) 
{
}

ObExprAIEmbed::~ObExprAIEmbed() 
{
}

int ObExprAIEmbed::calc_result_typeN(ObExprResType &type,
                                     ObExprResType *types_stack,
                                     int64_t param_num,
                                     common::ObExprTypeCtx &type_ctx) const 
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 3 || param_num < 2)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    types_stack[MODEL_IDX].set_calc_type(ObVarcharType);
    types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    // text/longtext input must not be truncated by an implicit varchar cast
    if (ob_is_text_tc(types_stack[CONTENT_IDX].get_type())) {
      types_stack[CONTENT_IDX].set_calc_type(ObLongTextType);
    } else {
      types_stack[CONTENT_IDX].set_calc_type(ObVarcharType);
    }
    types_stack[CONTENT_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (param_num == 3) {
      if (ob_is_integer_type(types_stack[DIM_IDX].get_type())) {
        types_stack[DIM_IDX].set_calc_type(ObIntType);
        types_stack[DIM_IDX].set_precision(10);
        types_stack[DIM_IDX].set_scale(0);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be an integer, not a decimal or float");
      }
    }
    type.set_varchar();
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) 
{
  INIT_SUCC(ret);
  ObDatum *arg_model_id = nullptr;
  ObDatum *arg_content = nullptr;
  ObDatum *arg_dim = nullptr;
  if (expr.arg_cnt_ == 3 ? OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content, arg_dim))
                         : OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content))) {
  } else if (arg_model_id->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id is null");
    res.set_null();
  } else if (arg_content->is_null()) {
    // null input propagates a null embedding instead of failing the query
    res.set_null();
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator());
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(N_AI_EMBED));
    ObAIFuncExprInfo *info = nullptr;
    share::ObAiModelEndpointInfo resolved_endpoint;
    const share::ObAiModelEndpointInfo *endpoint_info = &resolved_endpoint;
    query::ObIAiEndpointResolver *endpoint_resolver =
        ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
    ObString model_id = arg_model_id->get_string();
    ObString content;
    if (model_id.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id is empty");
      res.set_null();
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_,
                                                                  temp_allocator,
                                                                  *arg_content,
                                                                  expr.args_[CONTENT_IDX]->datum_meta_,
                                                                  expr.args_[CONTENT_IDX]->obj_meta_.has_lob_header(),
                                                                  content))) {
    } else if (content.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, input is empty");
      res.set_null();
    }
    int64_t dim = 0;
    ObJsonInt *dim_json = nullptr;
    ObJsonObject *config = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(arg_dim) && !arg_dim->is_null()) {
      dim = arg_dim->get_int();
      if (dim <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be a positive integer");
        res.set_null();
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(temp_allocator, config))) {
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(temp_allocator, dim, dim_json))) {
      } else if (OB_FAIL(config->add("dimensions", dim_json))) {
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(temp_allocator, model_id, info))) {
    } else if (OB_ISNULL(endpoint_resolver)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(endpoint_resolver->resolve_by_model_name(
                   model_id, temp_allocator, resolved_endpoint))) {
    } else {
      ObAIFuncModel model(temp_allocator, *info, *endpoint_info);
      ObString result;
      if (OB_FAIL(model.call_dense_embedding(content, config, result))) {
      } else if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, res, result))) {
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                       const ObBitVector &skip, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObDatumVector model_datums;
  ObDatumVector content_datums;
  ObDatumVector dim_datums;
  if (expr.arg_cnt_ == 3) {
    if (OB_FAIL(expr.eval_batch_param_value(ctx, skip, size, model_datums,
                                            content_datums, dim_datums))) {
    }
  } else {
    if (OB_FAIL(expr.eval_batch_param_value(ctx, skip, size, model_datums,
                                            content_datums))) {
    }
  }

  ObDatum *res_datums = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(res_datums = expr.locate_batch_datums(ctx))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator());
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(N_AI_EMBED));
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObArray<ObAIEmbedBatchGroup *> groups;

    // group the rows of this batch by (model id, dimension); null contents are
    // skipped and propagated as null results
    for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }
      ObDatum *model_datum = model_datums.at(i);
      ObDatum *content_datum = content_datums.at(i);
      if (OB_ISNULL(model_datum) || OB_ISNULL(content_datum)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (model_datum->is_null()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id is null");
      } else if (content_datum->is_null()) {
        res_datums[i].set_null();
        eval_flags.set(i);
      } else {
        ObString model_id = model_datum->get_string();
        int64_t dim = 0;
        if (expr.arg_cnt_ == 3) {
          ObDatum *dim_datum = dim_datums.at(i);
          if (OB_NOT_NULL(dim_datum) && !dim_datum->is_null()) {
            dim = dim_datum->get_int();
          }
        }
        ObString content;
        if (model_id.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id is empty");
        } else if (dim < 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be a positive integer");
        } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_,
                                                                     temp_allocator,
                                                                     *content_datum,
                                                                     expr.args_[CONTENT_IDX]->datum_meta_,
                                                                     expr.args_[CONTENT_IDX]->obj_meta_.has_lob_header(),
                                                                     content))) {
        } else if (content.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, input is empty");
        } else {
          ObAIEmbedBatchGroup *group = nullptr;
          for (int64_t j = 0; OB_ISNULL(group) && j < groups.count(); j++) {
            ObAIEmbedBatchGroup *candidate = groups.at(j);
            if (OB_NOT_NULL(candidate) && candidate->dimension_ == dim &&
                candidate->model_id_.length() == model_id.length() &&
                0 == MEMCMP(candidate->model_id_.ptr(), model_id.ptr(), model_id.length())) {
              group = candidate;
            }
          }
          if (OB_ISNULL(group)) {
            if (OB_ISNULL(group = OB_NEWx(ObAIEmbedBatchGroup, &temp_allocator))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else if (FALSE_IT(group->model_id_ = model_id)) {
            } else if (FALSE_IT(group->dimension_ = dim)) {
            } else if (OB_FAIL(groups.push_back(group))) {
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(group->row_idxs_.push_back(i))) {
          } else if (OB_SUCC(ret) && OB_FAIL(group->contents_.push_back(content))) {
          }
        }
      }
    }

    for (int64_t g = 0; OB_SUCC(ret) && g < groups.count(); g++) {
      ObAIEmbedBatchGroup *group = groups.at(g);
      if (OB_ISNULL(group)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (group->contents_.count() == 0) {
        // nothing to do
      } else {
        ObAIFuncExprInfo *info = nullptr;
        share::ObAiModelEndpointInfo resolved_endpoint;
        const share::ObAiModelEndpointInfo *endpoint_info = &resolved_endpoint;
        query::ObIAiEndpointResolver *endpoint_resolver =
            ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
        ObJsonObject *config = nullptr;
        if (group->dimension_ > 0) {
          ObJsonInt *dim_json = nullptr;
          if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(temp_allocator, config))) {
          } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(temp_allocator, group->dimension_, dim_json))) {
          } else if (OB_FAIL(config->add("dimensions", dim_json))) {
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(temp_allocator, group->model_id_, info))) {
        } else if (OB_ISNULL(endpoint_resolver)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(endpoint_resolver->resolve_by_model_name(
                       group->model_id_, temp_allocator, resolved_endpoint))) {
        } else {
          ObAIFuncModel model(temp_allocator, *info, *endpoint_info);
          ObArray<ObString> results;
          if (OB_FAIL(model.call_dense_embedding_vector_v2(group->contents_, config, results))) {
          } else if (results.count() != group->contents_.count()) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < results.count(); i++) {
              const ObString &result = results.at(i);
              const int64_t row_idx = group->row_idxs_.at(i);
              char *result_buf = expr.get_str_res_mem(ctx, result.length(), row_idx);
              if (OB_ISNULL(result_buf)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              } else {
                MEMCPY(result_buf, result.ptr(), result.length());
                res_datums[row_idx].set_string(result_buf, result.length());
                eval_flags.set(row_idx);
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::cg_expr(ObExprCGCtx &expr_cg_ctx, 
                           const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const 
{
  int ret = OB_SUCCESS;
  // TODO: support schema version match in plan cache for ai func
  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprAIEmbed::eval_ai_embed;
    rt_expr.eval_batch_func_ = ObExprAIEmbed::eval_ai_embed_batch;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
