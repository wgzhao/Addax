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

import com.wgzhao.addax.common.base.Key;

public class KeyConstant
        extends Key
{

    /**
     * mongodb 的 host 地址
     */
    public static final String MONGO_ADDRESS = "address";

    public static final String MONGO_AUTH_DB = "authDb";
    /**
     * mongodb 集合名
     */
    public static final String MONGO_COLLECTION_NAME = "collection";
    /**
     * mongodb 查询条件
     */
    public static final String MONGO_QUERY = "query";

    public static final String LOWER_BOUND = "lowerBound";
    public static final String UPPER_BOUND = "upperBound";
    public static final String IS_OBJECT_ID = "isObjectId";


    /**
     * MongoDB的_id
     */
    public static final String MONGO_PRIMARY_ID = "_id";
    /**
     * MongoDB的错误码
     */
    public static final int MONGO_UNAUTHORIZED_ERR_CODE = 13;
    public static final int MONGO_ILLEGAL_OP_ERR_CODE = 20;
    public static final int MONGO_COMMAND_NOT_FOUND_CODE = 59;

    private KeyConstant() {}
}
