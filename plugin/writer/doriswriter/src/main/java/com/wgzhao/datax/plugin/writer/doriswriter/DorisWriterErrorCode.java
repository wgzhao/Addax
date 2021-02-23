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

package com.wgzhao.datax.plugin.writer.doriswriter;

import com.wgzhao.datax.common.spi.ErrorCode;

public enum DorisWriterErrorCode
        implements ErrorCode
{
    REQUIRED_VALUE("DorisWriter-00", "缺失必要的值"),
    ILLEGAL_VALUE("DorisWriter-01", "值非法"),
    CONF_ERROR("DorisWriter-02", "您的配置错误."),
    CONNECT_ERROR("DorisWriter-03", "连接错误"),
    WRITER_ERROR("DorisWriter-04", "写入错误");

    private final String code;
    private final String description;

    DorisWriterErrorCode(String code, String description)
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
