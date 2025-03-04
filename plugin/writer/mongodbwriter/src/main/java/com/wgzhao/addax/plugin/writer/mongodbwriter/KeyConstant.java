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

import com.wgzhao.addax.common.base.Key;

public class KeyConstant
        extends Key
{

    public static final String MONGO_ADDRESS = "address";

    public static final String ARRAY_TYPE = "array";

    public static final String OBJECT_ID_TYPE = "objectid";

    public static final String MONGO_COLLECTION_NAME = "collection";

    public static final String COLUMN_NAME = "name";

    public static final String COLUMN_TYPE = "type";

    public static final String ITEM_TYPE = "itemtype";

    public static final String COLUMN_SPLITTER = "splitter";

    public static final String IS_REPLACE = "isReplace";

    public static final String UNIQUE_KEY = "replaceKey";

    private KeyConstant() {}

    public static boolean isArrayType(String type)
    {
        return ARRAY_TYPE.equals(type);
    }

    public static boolean isObjectIdType(String type)
    {
        return OBJECT_ID_TYPE.equals(type);
    }

    public static boolean isValueTrue(String value)
    {
        return "true".equals(value);
    }
}
