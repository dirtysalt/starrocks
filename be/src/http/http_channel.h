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

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "http/http_status.h"

struct mg_connection;

namespace starrocks {

class HttpRequest;

class HttpChannel {
public:
    // Helper maybe used everywhere
    static void send_basic_challenge(HttpRequest* req, const std::string& realm);

    static void send_error(HttpRequest* request, HttpStatus status);

    // send 200(OK) reply with content
    static void send_reply(HttpRequest* request, std::string_view content) {
        send_reply(request, HttpStatus::OK, content);
    }

    static void send_reply(HttpRequest* request, HttpStatus status = HttpStatus::OK);

    static void send_reply(HttpRequest* request, HttpStatus status, std::string_view content);

    static void send_reply_json(HttpRequest* request, HttpStatus status, std::string_view content);

    static void send_reply(HttpRequest* request, HttpStatus status, std::string_view content,
                           const std::optional<std::string_view>& content_type);

    static void send_file(HttpRequest* request, int fd, size_t off, size_t size);
};

} // namespace starrocks
