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

package com.wgzhao.datax.plugin.reader.hbase20xsqlreader;

public class Constant
{
    public static final String PK_TYPE = "pkType";

    public static final Object PK_TYPE_STRING = "pkTypeString";

    public static final Object PK_TYPE_LONG = "pkTypeLong";

    public static final String DEFAULT_SERIALIZATION = "PROTOBUF";

    public static final String CONNECT_STRING_TEMPLATE = "jdbc:phoenix:thin:url=%s;serialization=%s";

    public static final String CONNECT_DRIVER_STRING = "org.apache.phoenix.queryserver.client.Driver";

    public static final String SELECT_COLUMNS_TEMPLATE = "SELECT COLUMN_NAME, COLUMN_FAMILY FROM SYSTEM.CATALOG WHERE TABLE_NAME='%s' AND COLUMN_NAME IS NOT NULL";

    public static final String QUERY_SQL_TEMPLATE_WITHOUT_WHERE = "select %s from %s ";

    public static final String QUERY_SQL_TEMPLATE = "select %s from %s where (%s)";

    public static final String QUERY_MIN_MAX_TEMPLATE = "SELECT MIN(%s),MAX(%s) FROM %s";

    public static final String QUERY_COLUMN_TYPE_TEMPLATE = "SELECT %s FROM %s LIMIT 1";

    public static final String QUERY_SQL_PER_SPLIT = "querySqlPerSplit";

    private Constant() {}
}
