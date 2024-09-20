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


public enum CommonErrorCode
        implements ErrorCode
{
    REQUIRED_VALUE("1001", "missing required parameters"),
    ILLEGAL_VALUE("1002", "illegal parameter value"),
    CONFIG_ERROR("1003", "configuration error"),

    PERMISSION_DENIED("2001", "permission denied"),
    CONNECT_ERROR("2002", "connection error"),
    LOGIN_ERROR("2003", "login error"),

    CONVERT_NOT_SUPPORT("3001", "unsupported data type conversion"),
    NOT_SUPPORT_TYPE("3002", "unsupported data type"),
    CONVERT_OVER_FLOW("3003", "data type conversion overflow"),
    ENCODING_ERROR("3004", "encoding error"),

    RETRY_FAIL("4001", "retry failed"),
    EXECUTE_FAIL("4002", "execution failed"),
    IO_ERROR("4003", "IO error"),

    RUNTIME_ERROR("5001", "runtime error"),
    HOOK_INTERNAL_ERROR("5002", "Hook internal error"),
    SHUT_DOWN_TASK("5003", "Task shutdown"),
    WAIT_TIME_EXCEED("5004", "Wait time exceed"),
    TASK_HUNG_EXPIRED("5005", "Task hung expired"),;

    private final String code;

    private final String describe;

    CommonErrorCode(String code, String describe)
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
