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

package com.wgzhao.datax.plugin.writer.kuduwriter;

public class Key
{
    public final static String KUDU_CONFIG = "kuduConfig";
    public final static String KUDU_MASTER_ADDRESSES = "masterAddress";
    public final static String KUDU_ADMIN_TIMEOUT = "timeout";
    public final static String KUDU_SESSION_TIMEOUT = "sessionTimeout";

    public final static String KUDU_TABLE_NAME = "table";
    public final static String PARTITION = "partition";
    public final static String COLUMN = "column";

    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String INDEX = "index";
    public static final String PRIMARYKEY = "primaryKey";
    public static final String COMPRESSION = "compress";
    public static final String COMMENT = "comment";
    public final static String ENCODING = "encoding";

    public static final String NUM_REPLICAS = "replicas";
    public static final String HASH = "hash";
    public static final String HASH_NUM = "number";

    public static final String RANGE = "range";
    public static final String LOWER = "lower";
    public static final String UPPER = "upper";

    public static final String TRUNCATE = "truncate";

    public static final String WRITE_MODE = "writeMode";

    public static final String WRITE_BATCH_SIZE = "batchSize";

    public static final String MUTATION_BUFFER_SPACE = "bufferSize";
    public static final String SKIP_FAIL = "skipFail";
}
