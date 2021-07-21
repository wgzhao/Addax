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

package com.wgzhao.addax.plugin.writer.hbase11xwriter;

import com.wgzhao.addax.common.spi.ErrorCode;

/**
 * Hbase11xWriterErrorCode
 * Created by shf on 16/3/8.
 */
public enum Hbase11xWriterErrorCode
        implements ErrorCode
{
    REQUIRED_VALUE("Hbasewriter-00", "The mandatory parameter are missing"),
    ILLEGAL_VALUE("Hbasewriter-01", "The value of parameter is illegal"),
    HBASE_CONNECTION_ERROR("Hbasewriter-02", "Failed to connect to HBase cluster"),
    HBASE_TABLE_ERROR("Hbasewriter-03", "Failed to access HBase table"),
    CLOSE_HBASE_CONNECTION_ERROR("Hbasewriter-04", "Failed to close connection"),
    CLOSE_HBASE_AMIN_ERROR("Hbasewriter-05", "Failed to close HBase admin handler"),
    CLOSE_HBASE_TABLE_ERROR("Hbasewriter-06", "Failed to close HBase table handler"),
    PUT_HBASE_ERROR("Hbasewriter-07", "IO Exception occurred while putting"),
    TRUNCATE_HBASE_ERROR("Hbasewriter-09", "Exception occurred while truncating table"),
    CONSTRUCT_ROWKEY_ERROR("Hbasewriter-10", "Failed to create rowkey."),
    CONSTRUCT_VERSION_ERROR("Hbasewriter-11", "Failed to put version."),
    GET_HBASE_BUFFERED_MUTATOR_ERROR("Hbasewriter-12", "Failed to get the BufferedMutator."),
    CLOSE_HBASE_BUFFERED_MUTATOR_ERROR("Hbasewriter-13", "Failed to close BufferedMutator handler."),
    KERBEROS_LOGIN_ERROR("Hbasewriter-14", "Kerberos authentication failed.")
    ;
    private final String code;
    private final String description;

    Hbase11xWriterErrorCode(String code, String description)
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
        return String.format("Error Code:[%s], Description:[%s].", this.code, this.description);
    }
}
