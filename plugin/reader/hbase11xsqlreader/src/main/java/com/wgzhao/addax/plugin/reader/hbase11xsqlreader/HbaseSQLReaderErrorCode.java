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

package com.wgzhao.addax.plugin.reader.hbase11xsqlreader;

import com.wgzhao.addax.common.spi.ErrorCode;

public enum HbaseSQLReaderErrorCode
        implements ErrorCode
{
    REQUIRED_VALUE("HBaseReader-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("HBaseReader-01", "您填写的参数值不合法."),
    GET_PHOENIX_COLUMN_ERROR("HBaseReader-02", "获取phoenix表的列值错误"),
    GET_PHOENIX_CONNECTIONINFO_ERROR("HBaseReader-03", "获取phoenix服务的zkurl错误"),
    GET_PHOENIX_SPLITS_ERROR("HBaseReader-04", "获取phoenix的split信息错误"),
    PHOENIX_CREATEREADER_ERROR("HBaseReader-05", "获取phoenix的reader错误"),
    PHOENIX_READERINIT_ERROR("HBaseReader-06", "phoenix reader的初始化错误"),
    PHOENIX_COLUMN_TYPE_CONVERT_ERROR("HBaseReader-07", "phoenix的列类型转换错误"),
    //    PHOENIX_RECORD_READ_ERROR("HBaseReader-08", "phoenix record 读取错误"),
    PHOENIX_READER_CLOSE_ERROR("HBaseReader-09", "phoenix reader 的close错误"),
    KERBEROS_LOGIN_ERROR("HBaseReader-10", "KERBEROS认证失败"),
    HBASE_CONNECTION_ERROR("HBaseReader-11", "连接 HBase 失败");

    private final String code;
    private final String description;

    HbaseSQLReaderErrorCode(String code, String description)
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
        return String.format("Code:[%s], Description:[%s].", this.code, this.description);
    }
}
