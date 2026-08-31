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

package com.wgzhao.addax.plugin.reader.mongodbreader;

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
    /** Mongo query. */
    public static final String MONGO_QUERY = "query";

    /** Lower bound. */
    public static final String LOWER_BOUND = "lowerBound";
    /** Upper bound. */
    public static final String UPPER_BOUND = "upperBound";
    /** Is object id. */
    public static final String IS_OBJECT_ID = "isObjectId";

    /** Mongo primary id. */
    public static final String MONGO_PRIMARY_ID = "_id";

    /** Mongo unauthorized err code. */
    public static final int MONGO_UNAUTHORIZED_ERR_CODE = 13;
    /** Mongo illegal op err code. */
    public static final int MONGO_ILLEGAL_OP_ERR_CODE = 20;
    /** Mongo command not found code. */
    public static final int MONGO_COMMAND_NOT_FOUND_CODE = 59;

    private KeyConstant() {}
}
