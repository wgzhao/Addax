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

package com.wgzhao.datax.plugin.reader.cassandrareader;

/**
 * Created by mazhenlin on 2019/8/19.
 */
public class Key
{
    public final static String USERNAME = "username";
    public final static String PASSWORD = "password";

    public final static String HOST = "host";
    public final static String PORT = "port";
    public final static String USESSL = "useSSL";

    public final static String KEYSPACE = "keyspace";
    public final static String TABLE = "table";
    public final static String COLUMN = "column";
    public final static String WHERE = "where";
    public final static String ALLOW_FILTERING = "allowFiltering";
    public final static String CONSITANCY_LEVEL = "consistancyLevel";
    public final static String MIN_TOKEN = "minToken";
    public final static String MAX_TOKEN = "maxToken";

    /**
     * 每个列的名字
     */
    public static final String COLUMN_NAME = "name";
    /**
     * 列分隔符
     */
    public static final String COLUMN_SPLITTER = "format";
    public static final String WRITE_TIME = "writetime(";
    public static final String ELEMENT_SPLITTER = "splitter";
    public static final String ENTRY_SPLITTER = "entrySplitter";
    public static final String KV_SPLITTER = "kvSplitter";
    public static final String ELEMENT_CONFIG = "element";
    public static final String TUPLE_CONNECTOR = "_";
    public static final String KEY_CONFIG = "key";
    public static final String VALUE_CONFIG = "value";
}
