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

public class Key
{
    // 获取返回json的那个key值
    public static final String RESULT_KEY = "resultKey";
    // 连接信息
    public static final String CONNECTION = "connection";
    // 配置连接代理
    public static final String PROXY = "proxy";
    // 代理地址
    public static final String HOST = "host";
    // 代理认证信息，格式为 username:password
    public static final String AUTH = "auth";
    // 请求的地址
    public static final String URL = "url";
    // 接口认证帐号
    public static final String USERNAME = "username";
    // 接口认证密码
    public static final String PASSWORD = "password";
    // 接口认证token
    public static final String TOKEN = "token";
    // 接口请求参数
    public static final String REQUEST_PARAMETERS = "reqParams";
    // 请求的定制头信息
    public static final String HEADERS = "headers";
    // 请求超时参数，单位为秒
    public static final String TIMEOUT_SEC = "timeout";
    // 请求方法，仅支持get，post两种模式
    public static final String METHOD = "method";
    // 结果集编码，默认为UTF8
    public static final String ENCODING = "encoding";
    // 要获取的字段，即json中的key，key值允许包含子路径，比如 info.user.age
    public static final String COLUMN = "column";
}
