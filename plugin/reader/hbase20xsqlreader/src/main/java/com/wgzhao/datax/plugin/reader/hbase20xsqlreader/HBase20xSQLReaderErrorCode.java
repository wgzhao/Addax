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

package com.wgzhao.datax.plugin.reader.hbase20xsqlreader;

import com.wgzhao.datax.common.spi.ErrorCode;

public enum HBase20xSQLReaderErrorCode
        implements ErrorCode
{
    REQUIRED_VALUE("Hbasewriter-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("Hbasewriter-01", "您填写的参数值不合法."),
    GET_QUERYSERVER_CONNECTION_ERROR("Hbasewriter-02", "获取QueryServer连接时出错."),
    GET_PHOENIX_TABLE_ERROR("Hbasewriter-03", "获取 Phoenix table时出错."),
    GET_TABLE_COLUMNTYPE_ERROR("Hbasewriter-05", "获取表列类型时出错."),
    CLOSE_PHOENIX_CONNECTION_ERROR("Hbasewriter-06", "关闭JDBC连接时时出错."),
    ILLEGAL_SPLIT_PK("Hbasewriter-07", "非法splitKey配置."),
    PHOENIX_COLUMN_TYPE_CONVERT_ERROR("Hbasewriter-08", "phoenix的列类型转换错误."),
    QUERY_DATA_ERROR("Hbasewriter-09", "truncate hbase表时发生异常."),
    ;

    private final String code;
    private final String description;

    HBase20xSQLReaderErrorCode(String code, String description)
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
