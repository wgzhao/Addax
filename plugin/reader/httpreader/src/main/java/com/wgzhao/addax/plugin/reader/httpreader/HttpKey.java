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

/** Http Key configuration keys. */
public final class HttpKey
        extends Key
{
    // the key of the JSON result
    /** Result key. */
    public static final String RESULT_KEY = "resultKey";
    // connection info
    /** Connection. */
    public static final String CONNECTION = "connection";
    /** Proxy. */
    public static final String PROXY = "proxy";
    // proxy host
    /** Host. */
    public static final String HOST = "host";
    // proxy auth, the format is username:password
    /** Auth. */
    public static final String AUTH = "auth";
    /** Url. */
    public static final String URL = "url";
    // api username for auth
    /** Username. */
    public static final String USERNAME = "username";
    // api password for auth
    /** Password. */
    public static final String PASSWORD = "password";
    // or api token for auth
    /** Token. */
    public static final String TOKEN = "token";
    // api parameters
    /** Request parameters. */
    public static final String REQUEST_PARAMETERS = "reqParams";
    // dynamic token acquisition config
    /** Auth config. */
    public static final String AUTH_CONFIG = "authConfig";
    // request header name that receives token, e.g. Authorization or X-Auth-Token
    /** Token header. */
    public static final String TOKEN_HEADER = "tokenHeader";
    // token value prefix, e.g. Bearer 
    /** Token prefix. */
    public static final String TOKEN_PREFIX = "tokenPrefix";
    // custom http headers
    /** Headers. */
    public static final String HEADERS = "headers";
    // timeout in seconds
    /** Timeout sec. */
    public static final String TIMEOUT_SEC = "timeout";
    // request method, only support GET and POST
    /** Method. */
    public static final String METHOD = "method";

    /** Is page. */
    public static final String IS_PAGE = "isPage";
    /** Page params. */
    public static final String PAGE_PARAMS = "pageParams";
    /** Page size. */
    public static final String PAGE_SIZE = "pageSize";
    /** Page index. */
    public static final String PAGE_INDEX = "pageIndex";
}
