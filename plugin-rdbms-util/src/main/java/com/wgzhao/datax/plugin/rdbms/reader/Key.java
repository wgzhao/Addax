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

package com.wgzhao.datax.plugin.rdbms.reader;

/**
 * 编码，时区等配置，暂未定.
 */
public final class Key
{

    public static final String JDBC_URL = "jdbcUrl";
    // 自定义的驱动名字
    public static final String JDBC_DRIVER = "driver";
    public static final String USERNAME = "username";

    public static final String PASSWORD = "password";
    public static final String TABLE = "table";
    public static final String MANDATORY_ENCODING = "mandatoryEncoding";
    // 是数组配置
    public static final String COLUMN = "column";
    public static final String COLUMN_LIST = "columnList";
    public static final String WHERE = "where";
    public static final String HINT = "hint";
    public static final String SPLIT_PK = "splitPk";
//    public static final String SPLIT_MODE = "splitMode"
    public static final String SAMPLE_PERCENTAGE = "samplePercentage";
    public static final String QUERY_SQL = "querySql";
    public static final String SPLIT_PK_SQL = "splitPkSql";
    public static final String PRE_SQL = "preSql";
    public static final String POST_SQL = "postSql";
    public static final String CHECK_SLAVE = "checkSlave";
    public static final String SESSION = "session";
//    public static final String DBNAME = "dbName"
    public static final String DRYRUN = "dryRun";

    private Key() {}
}