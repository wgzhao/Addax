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

package com.wgzhao.addax.plugin.reader.hbase20xreader;

public final class Key
{

    public static final String HBASE_CONFIG = "hbaseConfig";

    public static final String TABLE = "table";

    /**
     * mode 可以取 normal 或者 multiVersionFixedColumn 或者 multiVersionDynamicColumn 三个值，无默认值。
     * <p>
     * normal 配合 column(Map 结构的)使用
     */
    public static final String MODE = "mode";

    /**
     * 配合 mode = multiVersion 时使用，指明需要读取的版本个数。无默认值
     * <code>-1</code> 表示去读全部版本
     * 不能为 <code>0,1</code>
     * &gt; 1 表示最多读取对应个数的版本数(不能超过 Integer 的最大值)
     */
    public static final String MAX_VERSION = "maxVersion";


    // 默认为 utf8
    public static final String ENCODING = "encoding";

    public static final String COLUMN = "column";

    public static final String COLUMN_FAMILY = "columnFamily";

    public static final String NAME = "name";

    public static final String TYPE = "type";

    public static final String FORMAT = "format";

    public static final String VALUE = "value";

    public static final String START_ROWKEY = "startRowkey";

    public static final String END_ROWKEY = "endRowkey";

    public static final String IS_BINARY_ROWKEY = "isBinaryRowkey";

    public static final String SCAN_CACHE_SIZE = "scanCacheSize";

    public static final String SCAN_BATCH_SIZE = "scanBatchSize";
}
