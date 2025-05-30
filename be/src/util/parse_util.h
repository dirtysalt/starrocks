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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/parse_util.h

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

#pragma once

#include <boost/cstdint.hpp>
#include <string>

#include "common/statusor.h"

namespace starrocks {

// Utility class for parsing information from strings.
class ParseUtil {
public:
    // Parses mem_spec_str and returns the memory size in bytes.
    // Accepted formats:
    // '<int>[bB]?'  -> bytes (default if no unit given)
    // '<float>[mM]' -> megabytes
    // '<float>[gG]' -> in gigabytes
    // '<int>%'      -> in percent of memory_limit
    // Return 0 if mem_spec_str is empty.
    // Return -1, means no limit and will automatically adjust
    // The caller needs to handle other legitimate negative values.
    // If parsing mem_spec_str fails, it will return an error.
    static StatusOr<int64_t> parse_mem_spec(const std::string& mem_spec_str, const int64_t memory_limit);
};

} // namespace starrocks
