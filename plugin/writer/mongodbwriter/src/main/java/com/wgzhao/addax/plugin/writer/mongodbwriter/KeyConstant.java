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

package com.wgzhao.addax.plugin.writer.mongodbwriter;

import com.wgzhao.addax.core.base.Key;

/** Key Constant configuration keys. */
public class KeyConstant
        extends Key
{

    /** Mongo address. */
    public static final String MONGO_ADDRESS = "address";

    /** Mongo auth db. */
    public static final String MONGO_AUTH_DB = "authDb";

    /** Mongo collection name. */
    public static final String MONGO_COLLECTION_NAME = "collection";

    /** Column name. */
    public static final String COLUMN_NAME = "name";

    /** Column type. */
    public static final String COLUMN_TYPE = "type";

    /** Item type. */
    public static final String ITEM_TYPE = "itemtype";

    /** Column splitter. */
    public static final String COLUMN_SPLITTER = "splitter";

    private KeyConstant() {}
}
