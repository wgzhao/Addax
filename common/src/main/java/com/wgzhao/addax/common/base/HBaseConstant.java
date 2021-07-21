/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.wgzhao.addax.common.base;

public final class HBaseConstant extends Constant
{
    private HBaseConstant() {}

    public static final String RANGE = "range";
    public static final String ROWKEY_FLAG = "rowkey";
    public static final int DEFAULT_SCAN_CACHE_SIZE = 256;
    public static final int DEFAULT_SCAN_BATCH_SIZE = 100;

    public static final String DEFAULT_SERIALIZATION = "PROTOBUF";

    public static final String CONNECT_STRING_TEMPLATE = "jdbc:phoenix:thin:url=%s;serialization=%s";

    public static final String CONNECT_DRIVER_STRING = "org.apache.phoenix.queryserver.client.Driver";

    public static final String SELECT_COLUMNS_TEMPLATE = "SELECT COLUMN_NAME, COLUMN_FAMILY FROM SYSTEM.CATALOG WHERE TABLE_NAME='%s' AND COLUMN_NAME IS NOT NULL";

    public static final String QUERY_MIN_MAX_TEMPLATE = "SELECT MIN(%s),MAX(%s) FROM %s";

    public static final String QUERY_COLUMN_TYPE_TEMPLATE = "SELECT %s FROM %s LIMIT 1";

    public static final String QUERY_SQL_PER_SPLIT = "querySqlPerSplit";

    public static final String DEFAULT_NULL_MODE = "skip";
    public static final String DEFAULT_ZNODE = "/hbase";
    //    public static final boolean DEFAULT_LAST_COLUMN_IS_VERSION = false;   // 默认最后一列不是version列
    public static final int DEFAULT_BATCH_ROW_COUNT = 256;   // 默认一次写256行
    public static final boolean DEFAULT_TRUNCATE = false;    // 默认开始的时候不清空表
    public static final boolean DEFAULT_USE_THIN_CLIENT = false;    // 默认不用thin客户端
    public static final boolean DEFAULT_HAVE_KERBEROS = false; //默认不启用 kerberos
    public static final String DEFAULT_KERBEROS_KEYTAB_FILE_PATH = null;
    public static final String DEFAULT_KERBEROS_PRINCIPAL = null;

    public static final int TYPE_UNSIGNED_TINYINT = 11;
    public static final int TYPE_UNSIGNED_SMALLINT = 13;
    public static final int TYPE_UNSIGNED_INTEGER = 9;
    public static final int TYPE_UNSIGNED_LONG = 10;
    // public static final int TYPE_UNSIGNED_FLOAT = 14
    // public static final int TYPE_UNSIGNED_DOUBLE = 15
    public static final int TYPE_UNSIGNED_DATE = 19;
    public static final int TYPE_UNSIGNED_TIME = 18;
    public static final int TYPE_UNSIGNED_TIMESTAMP = 20;

    public static final long DEFAULT_WRITE_BUFFER_SIZE = 8 * 1024 * 1024L;

}
