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

package com.wgzhao.addax.plugin.writer.hbase11xsqlwriter;

import com.wgzhao.addax.common.spi.ErrorCode;

public enum HbaseSQLWriterErrorCode
        implements ErrorCode
{
    REQUIRED_VALUE("Hbasewriter-00", "Missing mandatory configuration item."),
    ILLEGAL_VALUE("Hbasewriter-01", "Illegal configuration value."),
    GET_HBASE_CONNECTION_ERROR("Hbasewriter-02", "Failed to connect HBase cluster."),
//    GET_HBASE_TABLE_ERROR("Hbasewriter-03", "获取 Hbase table时出错."),
//    CLOSE_HBASE_CONNECTION_ERROR("Hbasewriter-04", "关闭Hbase连接时出错."),
    CLOSE_HBASE_AMIN_ERROR("Hbasewriter-05", "Failed to close HBase admin handler."),
//    CLOSE_HBASE_TABLE_ERROR("Hbasewriter-06", "关闭Hbase table时时出错."),
    PUT_HBASE_ERROR("Hbasewriter-07", "IO exception occurred while writing to HBase."),
//    DELETE_HBASE_ERROR("Hbasewriter-08", "delete hbase表时发生异常."),
    TRUNCATE_HBASE_ERROR("Hbasewriter-09", "Exception occurred while truncating HBase table."),
    KERBEROS_LOGIN_ERROR("HbaseWriter-10", "Kerberos authentication failed.");

    private final String code;
    private final String description;

    HbaseSQLWriterErrorCode(String code, String description)
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
