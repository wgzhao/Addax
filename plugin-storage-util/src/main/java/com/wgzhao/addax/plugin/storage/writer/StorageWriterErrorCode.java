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

package com.wgzhao.addax.plugin.storage.writer;

import com.wgzhao.addax.common.spi.ErrorCode;

public enum StorageWriterErrorCode
        implements ErrorCode
{
    ILLEGAL_VALUE("UnstructuredStorageWriter-00", "您填写的参数值不合法."),
    WRITE_FILE_WITH_CHARSET_ERROR("UnstructuredStorageWriter-01", "您配置的编码未能正常写入."),
    WRITE_FILE_IO_ERROR("UnstructuredStorageWriter-02", "您配置的文件在写入时出现IO异常."),
    RUNTIME_EXCEPTION("UnstructuredStorageWriter-03", "出现运行时异常, 请联系我们"),
    REQUIRED_VALUE("UnstructuredStorageWriter-04", "您缺失了必须填写的参数值."),
    ;

    private final String code;
    private final String description;

    StorageWriterErrorCode(String code, String description)
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
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }
}
