/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.wgzhao.addax.rdbms.util;

import com.wgzhao.addax.common.spi.ErrorCode;

public enum DBUtilErrorCode
        implements ErrorCode
{
    JDBC_NULL("db-1001", "JDBC URL为空，请检查配置"),
    CONF_ERROR("db-1002", "您的配置错误."),
    CONN_DB_ERROR("db-1003", "Failed to connect the database."),
    GET_COLUMN_INFO_FAILED("db-2001", "获取表字段相关信息失败."),
    UNSUPPORTED_TYPE("db-3001", "不支持的数据库类型. 请注意查看已经支持的数据库类型以及数据库版本."),
    COLUMN_SPLIT_ERROR("db-4001", "根据主键进行切分失败."),
    SET_SESSION_ERROR("db-5001", "设置 session 失败."),
    RS_ASYNC_ERROR("db-5002", "异步获取ResultSet next失败."),

    REQUIRED_VALUE("db-0001", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("db-0002", "您填写的参数值不合法."),
    ILLEGAL_SPLIT_PK("DBUtilErrorCode-04", "您填写的主键列不合法, 仅支持切分主键为一个,并且类型为整数或者字符串类型."),
    SPLIT_FAILED_ILLEGAL_SQL("DBUtilErrorCode-15", "尝试切分表时, 执行数据库 Sql 失败. 请检查您的配置 table/splitPk/where 并作出修改."),
    SQL_EXECUTE_FAIL("DBUtilErrorCode-06", "执行数据库 Sql 失败, 请检查您的配置的 column/table/where/querySql或者向 DBA 寻求帮助."),

    // only for reader
    READ_RECORD_FAIL("DBUtilErrorCode-07", "读取数据库数据失败. 请检查您的配置的 column/table/where/querySql或者向 DBA 寻求帮助."),
    TABLE_QUERY_SQL_MIXED("DBUtilErrorCode-08", "您配置凌乱了. 不能同时既配置table又配置querySql"),
    TABLE_QUERY_SQL_MISSING("DBUtilErrorCode-09", "您配置错误. table和querySql 应该并且只能配置一个."),

    // only for writer
    WRITE_DATA_ERROR("DBUtilErrorCode-05", "往您配置的写入表中写入数据时失败."),
    NO_INSERT_PRIVILEGE("DBUtilErrorCode-11", "数据库没有写权限，请联系DBA"),
    NO_DELETE_PRIVILEGE("DBUtilErrorCode-16", "数据库没有DELETE权限，请联系DBA"),
    ;

    private final String code;

    private final String description;

    DBUtilErrorCode(String code, String description)
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
        return String.format("error code: %s", this.code);
    }
}
