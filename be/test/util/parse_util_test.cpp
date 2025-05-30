// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/util/parse_util_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/parse_util.h"

#include <gtest/gtest.h>

#include <string>

#include "testutil/assert.h"

namespace starrocks {

const static int64_t test_memory_limit = 10000;

static void test_parse_mem_spec(const std::string& mem_spec_str, int64_t result) {
    ASSIGN_OR_ASSERT_FAIL(int64_t bytes, ParseUtil::parse_mem_spec(mem_spec_str, test_memory_limit));
    ASSERT_EQ(result, bytes);
}

TEST(TestParseMemSpec, Normal) {
    test_parse_mem_spec("1", 1);

    test_parse_mem_spec("100b", 100);
    test_parse_mem_spec("100B", 100);

    test_parse_mem_spec("5k", 5 * 1024L);
    test_parse_mem_spec("512K", 512 * 1024L);

    test_parse_mem_spec("4m", 4 * 1024 * 1024L);
    test_parse_mem_spec("46M", 46 * 1024 * 1024L);

    test_parse_mem_spec("8g", 8 * 1024 * 1024 * 1024L);
    test_parse_mem_spec("128G", 128 * 1024 * 1024 * 1024L);

    test_parse_mem_spec("8t", 8L * 1024 * 1024 * 1024 * 1024L);
    test_parse_mem_spec("128T", 128L * 1024 * 1024 * 1024 * 1024L);

    test_parse_mem_spec("20%", test_memory_limit * 0.2);
    test_parse_mem_spec("-1", -1);
    test_parse_mem_spec("", 0);
}

TEST(TestParseMemSpec, Bad) {
    std::vector<std::string> bad_values{{"1gib"}, {"1%b"}, {"1b%"}, {"gb"},  {"1GMb"}, {"1b1Mb"}, {"1kib"},
                                        {"1Bb"},  {"1%%"}, {"1.1"}, {"1pb"}, {"1eb"},  {"%"}};
    for (const auto& value : bad_values) {
        ASSERT_ERROR(ParseUtil::parse_mem_spec(value, test_memory_limit));
    }
}

} // namespace starrocks
