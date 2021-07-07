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

package com.wgzhao.addax.plugin.writer.hbase11xwriter;

public final class Key
{

    public static final String HBASE_CONFIG = "hbaseConfig";

    public static final String TABLE = "table";

    /**
     * mode 可以取 normal 或者 multiVersionFixedColumn 或者 multiVersionDynamicColumn 三个值，无默认值。
     * <p>
     * normal 配合 column(Map 结构的)使用
     * <p>
     * multiVersion
     */
    public static final String MODE = "mode";

    public static final String ROWKEY_COLUMN = "rowkeyColumn";

    public static final String VERSION_COLUMN = "versionColumn";

    /**
     * 默认为 utf8
     */
    public static final String ENCODING = "encoding";

    public static final String COLUMN = "column";

    public static final String INDEX = "index";

    public static final String NAME = "name";

    public static final String TYPE = "type";

    public static final String VALUE = "value";

//    public static final String FORMAT = "format"

    /**
     * 默认为 EMPTY_BYTES
     */
    public static final String NULL_MODE = "nullMode";

    public static final String TRUNCATE = "truncate";

//    public static final String AUTO_FLUSH = "autoFlush"

    public static final String WAL_FLAG = "walFlag";

    public static final String WRITE_BUFFER_SIZE = "writeBufferSize";

    private Key() {}
    
}
