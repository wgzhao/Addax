/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.core.base;

public final class HBaseKey
        extends Key
{

    private HBaseKey() {}

    public static final String HBASE_CONFIG = "hbaseConfig";

    // mode is one of normal, multiVersionFixedColumn, multiVersionDynamicColumn
    public static final String MODE = "mode";

    // should set up this when mode is multiVersion, default is -1 mean all, greater than 1 means the max version will read
    public static final String MAX_VERSION = "maxVersion";
    public static final String COLUMN_FAMILY = "columnFamily";
    public static final String START_ROW_KEY = "startRowkey";
    public static final String END_ROW_KEY = "endRowkey";
    public static final String IS_BINARY_ROW_KEY = "isBinaryRowkey";
    public static final String SCAN_CACHE_SIZE = "scanCacheSize";
    public static final String SCAN_BATCH_SIZE = "scanBatchSize";

    // serialization format, default is protobuf. string type
    public static final String SERIALIZATION_NAME = "serialization";
    // the schema of phoenix, default is null. string type
    public static final String SCHEMA = "schema";
    // the split key for table reading. string type
    public static final String SPLIT_KEY = "splitKey";
    // the split point for table reading. string type
    public static final String SPLIT_POINT = "splitPoint";

    // For Phoenix Query Server connection mode
    public static final String QUERY_SERVER_ADDRESS = "queryServerAddress";
    public static final String HBASE_THIN_CONNECT_URL = "hbase.thin.connect.url";
    public static final String HBASE_THIN_CONNECT_NAMESPACE = "hbase.thin.connect.namespace";
    public static final String HBASE_THIN_CONNECT_USERNAME = "hbase.thin.connect.username";
    public static final String HBASE_THIN_CONNECT_PASSWORD = "hbase.thin.connect.password";
    public static final String THIN_CLIENT = "thinClient";
    // Whether truncate table before write. boolean type
    public static final String TRUNCATE = "truncate";
    public static final String ROW_KEY_COLUMN = "rowkeyColumn";
    public static final String VERSION_COLUMN = "versionColumn";
    // the null mode, default is EMPTY_BYTES. string type
    public static final String NULL_MODE = "nullMode";
    public static final String WAL_FLAG = "walFlag";
    public static final String WRITE_BUFFER_SIZE = "writeBufferSize";
}
