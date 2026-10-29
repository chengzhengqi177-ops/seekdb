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
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"

using namespace oceanbase::common;

TEST(AiProviderEnhancement, restore_embedding_response_order)
{
  ObArenaAllocator allocator;
  ObString json(R"({"data":[{"index":1,"embedding":[2]},{"index":0,"embedding":[1]}]})");
  ObJsonObject *response = nullptr;
  ObIJsonBase *result = nullptr;
  ObOpenAIUtils::ObOpenAIEmbed provider;
  ASSERT_EQ(OB_SUCCESS, ObAIFuncJsonUtils::get_json_object_form_str(allocator, json, response));
  ASSERT_EQ(OB_SUCCESS, provider.parse_output(allocator, response, result));
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, result->json_type());
  ObJsonArray *array = static_cast<ObJsonArray *>(result);
  ASSERT_EQ(2U, array->element_count());
  EXPECT_EQ(1, static_cast<ObJsonArray *>(array->get_value(0))->get_value(0)->get_int());
  EXPECT_EQ(2, static_cast<ObJsonArray *>(array->get_value(1))->get_value(0)->get_int());
}

TEST(AiProviderEnhancement, reject_malformed_embedding_envelopes)
{
  ObArenaAllocator allocator;
  ObOpenAIUtils::ObOpenAIEmbed provider;
  const char *inputs[] = {
    R"({"data":{}})",
    R"({"data":[7]})",
    R"({"data":[{"index":0,"embedding":7}]})",
    R"({"data":[{"index":-1,"embedding":[1]}]})",
    R"({"data":[{"index":0,"embedding":[1]},{"index":0,"embedding":[2]}]})"
  };
  for (const char *input : inputs) {
    ObString json(input);
    ObJsonObject *response = nullptr;
    ObIJsonBase *result = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObAIFuncJsonUtils::get_json_object_form_str(allocator, json, response));
    EXPECT_EQ(OB_INVALID_DATA, provider.parse_output(allocator, response, result));
  }
}

TEST(AiProviderEnhancement, ollama_is_non_streaming_and_accepts_empty_key)
{
  ObArenaAllocator allocator;
  ObOllamaUtils::ObOllamaComplete provider;
  ObString key;
  ObArray<ObString> headers;
  EXPECT_EQ(OB_SUCCESS, provider.get_header(allocator, key, headers));
  EXPECT_GT(headers.count(), 0);
  ObString model("arbitrary-model"), system("system"), content("input");
  ObJsonObject *body = nullptr;
  ASSERT_EQ(OB_SUCCESS, provider.get_body(allocator, model, system, content, nullptr, body));
  ASSERT_NE(nullptr, body->get_value("stream"));
  EXPECT_FALSE(static_cast<ObJsonBoolean *>(body->get_value("stream"))->get_boolean());
  ASSERT_NE(nullptr, body->get_value("prompt"));
  EXPECT_EQ(content, static_cast<ObJsonString *>(body->get_value("prompt"))->get_str());
}

TEST(AiProviderEnhancement, validates_vector_shape_and_numeric_elements)
{
  ObArenaAllocator allocator;
  const char *inputs[] = {
    R"({"vectors":[[1,2],[3,4]]})",
    R"({"vectors":[[1],[2,3]]})",
    R"({"vectors":[[1,"2"],[3,4]]})",
    R"({"vectors":[[],[]]})",
    R"({"vectors":[[1,null],[3,4]]})"
  };
  for (int i = 0; i < 5; ++i) {
    ObString json(inputs[i]);
    ObJsonObject *object = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObAIFuncJsonUtils::get_json_object_form_str(allocator, json, object));
    EXPECT_EQ(i == 0 ? OB_SUCCESS : OB_INVALID_DATA,
              ObAIFuncJsonUtils::validate_embedding_array(object->get_value("vectors"), 2, 2));
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
