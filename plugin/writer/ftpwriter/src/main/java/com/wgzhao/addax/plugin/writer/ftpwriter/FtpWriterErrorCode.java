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

package com.wgzhao.addax.plugin.writer.ftpwriter;

import com.wgzhao.addax.common.spi.ErrorCode;

public enum FtpWriterErrorCode
        implements ErrorCode
{

    REQUIRED_VALUE("FtpWriter-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("FtpWriter-01", "您填写的参数值不合法."),
    OPEN_FILE_ERROR("FtpWriter-06", "您配置的文件在打开时异常."),
    WRITE_FILE_IO_ERROR("FtpWriter-07", "您配置的文件在读取时出现IO异常."),
    FAIL_LOGIN("FtpWriter-12", "登录失败,无法与ftp服务器建立连接."),
    FAIL_DISCONNECT("FtpWriter-13", "关闭ftp连接失败,无法与ftp服务器断开连接."),
    COMMAND_FTP_IO_EXCEPTION("FtpWriter-14", "与ftp服务器连接异常.");

    private final String code;
    private final String description;

    FtpWriterErrorCode(String code, String description)
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

    @Override
    public String toString()
    {
        return String.format("Code:[%s], Description:[%s].", code, description);
    }
}
