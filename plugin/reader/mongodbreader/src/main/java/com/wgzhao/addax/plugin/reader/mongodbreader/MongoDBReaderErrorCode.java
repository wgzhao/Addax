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

import com.wgzhao.addax.common.spi.ErrorCode;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 */
public enum MongoDBReaderErrorCode
        implements ErrorCode
{

    ILLEGAL_VALUE("ILLEGAL_PARAMETER_VALUE", "参数不合法"),
    ILLEGAL_ADDRESS("ILLEGAL_ADDRESS", "不合法的Mongo地址"),
    UNKNOWN_EXCEPTION("UNKNOWN_EXCEPTION", "未知异常"),
    REQUIRED_VALUE("REQUIRED_VALUE_EXCEPTION","Missing required parameters");

    private final String code;

    private final String description;

    MongoDBReaderErrorCode(String code, String description)
    {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode()
    {
        return code;
    }

    @Override
    public String getDescription()
    {
        return description;
    }
}

