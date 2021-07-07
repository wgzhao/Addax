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

package com.wgzhao.addax.plugin.writer.streamwriter;

import com.wgzhao.addax.common.spi.ErrorCode;

public enum StreamWriterErrorCode
        implements ErrorCode
{
    RUNTIME_EXCEPTION("StreamWriter-00", "运行时异常"),
    ILLEGAL_VALUE("StreamWriter-01", "您填写的参数值不合法."),
    CONFIG_INVALID_EXCEPTION("StreamWriter-02", "您的参数配置错误."),
    SECURITY_NOT_ENOUGH("TxtFileWriter-03", "您缺少权限执行相应的文件写入操作.");

    private final String code;
    private final String description;

    StreamWriterErrorCode(String code, String description)
    {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode()
    {
        return this.code;
    }

    @Override
    public String getDescription()
    {
        return this.description;
    }

    @Override
    public String toString()
    {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
