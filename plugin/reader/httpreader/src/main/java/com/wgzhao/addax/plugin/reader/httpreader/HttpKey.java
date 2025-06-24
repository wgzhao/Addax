/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.plugin.reader.httpreader;

import com.wgzhao.addax.core.base.Key;

public final class HttpKey
        extends Key
{
    // the key of the JSON result
    public static final String RESULT_KEY = "resultKey";
    // connection info
    public static final String CONNECTION = "connection";
    public static final String PROXY = "proxy";
    // proxy host
    public static final String HOST = "host";
    // proxy auth, the format is username:password
    public static final String AUTH = "auth";
    public static final String URL = "url";
    // api username for auth
    public static final String USERNAME = "username";
    // api password for auth
    public static final String PASSWORD = "password";
    // or api token for auth
    public static final String TOKEN = "token";
    // api parameters
    public static final String REQUEST_PARAMETERS = "reqParams";
    // custom http headers
    public static final String HEADERS = "headers";
    // timeout in seconds
    public static final String TIMEOUT_SEC = "timeout";
    // request method, only support GET and POST
    public static final String METHOD = "method";

    public static final String IS_PAGE = "isPage";
    public static final String PAGE_PARAMS = "pageParams";
    public static final String PAGE_SIZE = "pageSize";
    public static final String PAGE_INDEX = "pageIndex";
}
