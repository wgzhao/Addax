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

package com.wgzhao.datax.plugin.rdbms.writer;

public final class Key
{
    public static final String JDBC_URL = "jdbcUrl";

    public static final String USERNAME = "username";

    public static final String PASSWORD = "password";
    
    public static final String TABLE = "table";

    public static final String COLUMN = "column";

    // 可选值为：insert,replace，update, 默认为 insert
    // mysql 支持 insert, replace
    // oracle 支持insert, update
    // postgresql 支持 update
    // 其他数据库仅支持insert
    public static final String WRITE_MODE = "writeMode";

    public static final String PRE_SQL = "preSql";

    public static final String POST_SQL = "postSql";

    //默认值：2048
    public static final String BATCH_SIZE = "batchSize";

    //默认值：32m
    public static final String BATCH_BYTE_SIZE = "batchByteSize";

    public static final String EMPTY_AS_NULL = "emptyAsNull";

    public static final String DRYRUN = "dryRun";
    // 允许自定义驱动类名
    public static final String JDBC_DRIVER = "driver";

    private Key() {}
}