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

public final class Constant
{

    public static final String PK_TYPE = "pkType";
    public static final String EACH_TABLE_SPLIT_SIZE = "eachTableSplitSize";
    public static final Object PK_TYPE_STRING = "pkTypeString";
    public static final Object PK_TYPE_LONG = "pkTypeLong";
    public static final Object PK_TYPE_MONTECARLO = "pkTypeMonteCarlo";
    public static final String CONN_MARK = "connection";
    public static final String TABLE_NUMBER_MARK = "tableNumber";
    public static final String IS_TABLE_MODE = "isTableMode";
    public static final String FETCH_SIZE = "fetchSize";
    public static final String QUERY_SQL_TEMPLATE_WITHOUT_WHERE = "select %s from %s ";
    public static final String QUERY_SQL_TEMPLATE = "select %s from %s where (%s)";
    public static final String TABLE_NAME_PLACEHOLDER = "@table";

    private Constant() {}
}
