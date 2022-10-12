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

package com.wgzhao.addax.common.exception;

import com.wgzhao.addax.common.spi.ErrorCode;

/**
 * general Column Error code
 */
public enum ColumnErrorCode
        implements ErrorCode
{

    ILLEGAL_VALUE("ColumnError-01", "您填写的参数值不合法."),
    MIXED_INDEX_VALUE("ColumnError-02", "您的列信息配置同时包含了index,value."),
    NO_INDEX_VALUE("ColumnError-03", "您明确的配置列信息,但未填写相应的index,value."),
    NOT_SUPPORT_TYPE("ColumnError-04", "您配置的列类型暂不支持.");

    private final String code;

    private final String describe;

    ColumnErrorCode(String code, String describe)
    {
        this.code = code;
        this.describe = describe;
    }

    @Override
    public String getCode()
    {
        return this.code;
    }

    @Override
    public String getDescription()
    {
        return this.describe;
    }

    @Override
    public String toString()
    {
        return String.format("Code:[%s], Describe:[%s]", this.code, this.describe);
    }
}
