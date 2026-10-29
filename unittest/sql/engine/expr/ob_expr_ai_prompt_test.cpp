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

#include <gtest/gtest.h>
#include <string>
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"

using namespace oceanbase::common;

TEST(AiPromptEnhancement, repeated_placeholder_and_json_arguments)
{
  ObArenaAllocator allocator;
  ObString json(R"({"template":"{0}:{0}:{1}","args":["value",[1,2]]})");
  ObJsonObject *prompt = nullptr;
  ObString result;
  ASSERT_EQ(OB_SUCCESS, ObAIFuncJsonUtils::get_json_object_form_str(allocator, json, prompt));
  ASSERT_EQ(OB_SUCCESS, ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(allocator, prompt, result));
  std::string output(result.ptr(), result.length());
  EXPECT_EQ(0U, output.find("value:value:["));
  EXPECT_NE(std::string::npos, output.find('1'));
  EXPECT_NE(std::string::npos, output.find('2'));
}

TEST(AiPromptEnhancement, nested_prompt_is_rendered_once)
{
  ObArenaAllocator allocator;
  ObString json(R"({"template":"outer:{0}","args":[{"template":"inner:{0}","args":["{1}"]}]})");
  ObJsonObject *prompt = nullptr;
  ObString result;
  ASSERT_EQ(OB_SUCCESS, ObAIFuncJsonUtils::get_json_object_form_str(allocator, json, prompt));
  ASSERT_EQ(OB_SUCCESS, ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(allocator, prompt, result));
  EXPECT_EQ("outer:inner:{1}", std::string(result.ptr(), result.length()));
}

TEST(AiPromptEnhancement, rejects_out_of_range_and_overflowing_indices)
{
  ObArenaAllocator allocator;
  const char *inputs[] = {
    R"({"template":"{1}","args":["x"]})",
    R"({"template":"{9999999999999999999999999999999}","args":["x"]})"
  };
  for (const char *input : inputs) {
    ObString json(input);
    ObJsonObject *prompt = nullptr;
    ObString result;
    ASSERT_EQ(OB_SUCCESS, ObAIFuncJsonUtils::get_json_object_form_str(allocator, json, prompt));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(allocator, prompt, result));
  }
}
