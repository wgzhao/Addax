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

package com.wgzhao.addax.plugin.reader.cassandrareader;

import com.wgzhao.addax.core.base.Key;

/** My Key configuration keys. */
public class MyKey
        extends Key
{

    /** The Static. */
    public final static String HOST = "host";
    /** The Static. */
    public final static String PORT = "port";
    /** The Static. */
    public final static String USE_SSL = "useSSL";

    /** The Static. */
    public final static String KEYSPACE = "keyspace";
    /** The Static. */
    public final static String TABLE = "table";
    /** The Static. */
    public final static String COLUMN = "column";
    /** The Static. */
    public final static String WHERE = "where";
    /** The Static. */
    public final static String ALLOW_FILTERING = "allowFiltering";
    /** The Static. */
    public final static String CONSISTENCY_LEVEL = "consistencyLevel";
    /** The Static. */
    public final static String MIN_TOKEN = "minToken";
    /** The Static. */
    public final static String MAX_TOKEN = "maxToken";

    /** Column name. */
    public static final String COLUMN_NAME = "name";

    /** Mykey. */
    public MyKey() {super();}
}
