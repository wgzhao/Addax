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

package com.wgzhao.addax.plugin.writer.hbase20xsqlwriter;

public final class Constant
{
    public static final String DEFAULT_NULL_MODE = "skip";
    public static final String DEFAULT_SERIALIZATION = "PROTOBUF";
    public static final int DEFAULT_BATCH_ROW_COUNT = 256;   // 默认一次写256行

    public static final int TYPE_UNSIGNED_TINYINT = 11;
    public static final int TYPE_UNSIGNED_SMALLINT = 13;
    public static final int TYPE_UNSIGNED_INTEGER = 9;
    public static final int TYPE_UNSIGNED_LONG = 10;
//    public static final int TYPE_UNSIGNED_FLOAT = 14
//    public static final int TYPE_UNSIGNED_DOUBLE = 15
    public static final int TYPE_UNSIGNED_DATE = 19;
    public static final int TYPE_UNSIGNED_TIME = 18;
    public static final int TYPE_UNSIGNED_TIMESTAMP = 20;

    private Constant() {}
}
