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

package com.wgzhao.addax.plugin.writer.txtfilewriter;

import com.wgzhao.addax.common.spi.ErrorCode;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public enum TxtFileWriterErrorCode
        implements ErrorCode
{

    CONFIG_INVALID_EXCEPTION("TxtFileWriter-00", "您的参数配置错误."),
    REQUIRED_VALUE("TxtFileWriter-01", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("TxtFileWriter-02", "您填写的参数值不合法."),
    WRITE_FILE_ERROR("TxtFileWriter-03", "您配置的目标文件在写入时异常."),
    WRITE_FILE_IO_ERROR("TxtFileWriter-04", "您配置的文件在写入时出现IO异常."),
    SECURITY_NOT_ENOUGH("TxtFileWriter-05", "您缺少权限执行相应的文件写入操作."),
    PATH_NOT_VALID("TxtFileWriter-06", "配置的路径无效"),
    PAHT_NOT_DIR("TxtFileWriter-06", "您配置的路径不是文件夹.");

    private final String code;
    private final String description;

    TxtFileWriterErrorCode(String code, String description)
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
