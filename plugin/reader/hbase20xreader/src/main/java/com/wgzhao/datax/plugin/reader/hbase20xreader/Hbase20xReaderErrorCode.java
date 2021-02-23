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

package com.wgzhao.datax.plugin.reader.hbase20xreader;

import com.wgzhao.datax.common.spi.ErrorCode;

/**
 * Created by shf on 16/3/8.
 */
public enum Hbase20xReaderErrorCode
        implements ErrorCode
{
    REQUIRED_VALUE("Hbase20xReader-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("Hbase20xReader-01", "您填写的参数值不合法."),
    PREPAR_READ_ERROR("HbaseReader-02", "准备读取 Hbase 时出错."),
    SPLIT_ERROR("HbaseReader-03", "切分 Hbase 表时出错."),
    GET_HBASE_CONNECTION_ERROR("HbaseReader-04", "获取Hbase连接时出错."),
    GET_HBASE_TABLE_ERROR("HbaseReader-05", "初始化 Hbase 抽取表时出错."),
    GET_HBASE_REGINLOCTOR_ERROR("HbaseReader-06", "获取 Hbase RegionLocator时出错."),
    CLOSE_HBASE_CONNECTION_ERROR("HbaseReader-07", "关闭Hbase连接时出错."),
    CLOSE_HBASE_TABLE_ERROR("HbaseReader-08", "关闭Hbase 抽取表时出错."),
    CLOSE_HBASE_REGINLOCTOR_ERROR("HbaseReader-09", "关闭 Hbase RegionLocator时出错."),
    CLOSE_HBASE_ADMIN_ERROR("HbaseReader-10", "关闭 Hbase admin时出错.");

    private final String code;
    private final String description;

    Hbase20xReaderErrorCode(String code, String description)
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
