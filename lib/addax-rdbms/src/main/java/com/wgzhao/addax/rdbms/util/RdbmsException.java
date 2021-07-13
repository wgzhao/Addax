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

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.spi.ErrorCode;

/**
 * Created by judy.lt on 2015/6/5.
 */
public class RdbmsException
        extends AddaxException
{
    public RdbmsException(ErrorCode errorCode, String message)
    {
        super(errorCode, message);
    }

    public static AddaxException asConnException(DataBaseType dataBaseType, Exception e, String userName, String dbName)
    {
        if (dataBaseType == DataBaseType.MySql) {
            DBUtilErrorCode dbUtilErrorCode = mySqlConnectionErrorAna(e.getMessage());
            if (dbUtilErrorCode == DBUtilErrorCode.MYSQL_CONN_DB_ERROR && dbName != null) {
                return asAddaxException(dbUtilErrorCode, "该数据库名称为：" + dbName + " 具体错误信息为：" + e);
            }
            if (dbUtilErrorCode == DBUtilErrorCode.MYSQL_CONN_USERPWD_ERROR) {
                return asAddaxException(dbUtilErrorCode, "该数据库用户名为：" + userName + " 具体错误信息为：" + e);
            }
            return asAddaxException(dbUtilErrorCode, " 具体错误信息为：" + e);
        }

        if (dataBaseType == DataBaseType.Oracle) {
            DBUtilErrorCode dbUtilErrorCode = oracleConnectionErrorAna(e.getMessage());
            if (dbUtilErrorCode == DBUtilErrorCode.ORACLE_CONN_DB_ERROR && dbName != null) {
                return asAddaxException(dbUtilErrorCode, "该数据库名称为：" + dbName + " 具体错误信息为：" + e);
            }
            if (dbUtilErrorCode == DBUtilErrorCode.ORACLE_CONN_USERPWD_ERROR) {
                return asAddaxException(dbUtilErrorCode, "该数据库用户名为：" + userName + " 具体错误信息为：" + e);
            }
            return asAddaxException(dbUtilErrorCode, " 具体错误信息为：" + e);
        }
        return asAddaxException(DBUtilErrorCode.CONN_DB_ERROR, " 具体错误信息为：" + e);
    }

    public static DBUtilErrorCode mySqlConnectionErrorAna(String e)
    {
        if (e.contains(Constant.MYSQL_DATABASE)) {
            return DBUtilErrorCode.MYSQL_CONN_DB_ERROR;
        }

        if (e.contains(Constant.MYSQL_CONNEXP)) {
            return DBUtilErrorCode.MYSQL_CONN_IPPORT_ERROR;
        }

        if (e.contains(Constant.MYSQL_ACCDENIED)) {
            return DBUtilErrorCode.MYSQL_CONN_USERPWD_ERROR;
        }

        return DBUtilErrorCode.CONN_DB_ERROR;
    }

    public static DBUtilErrorCode oracleConnectionErrorAna(String e)
    {
        if (e.contains(Constant.ORACLE_DATABASE)) {
            return DBUtilErrorCode.ORACLE_CONN_DB_ERROR;
        }

        if (e.contains(Constant.ORACLE_CONNEXP)) {
            return DBUtilErrorCode.ORACLE_CONN_IPPORT_ERROR;
        }

        if (e.contains(Constant.ORACLE_ACCDENIED)) {
            return DBUtilErrorCode.ORACLE_CONN_USERPWD_ERROR;
        }

        return DBUtilErrorCode.CONN_DB_ERROR;
    }

    public static AddaxException asQueryException(DataBaseType dataBaseType, Exception e, String querySql, String table, String userName)
    {
        if (dataBaseType == DataBaseType.MySql) {
            DBUtilErrorCode dbUtilErrorCode = mySqlQueryErrorAna(e.getMessage());
            if (dbUtilErrorCode == DBUtilErrorCode.MYSQL_QUERY_TABLE_NAME_ERROR && table != null) {
                return asAddaxException(dbUtilErrorCode, "表名为：" + table + " 执行的SQL为:" + querySql + " 具体错误信息为：" + e);
            }
            if (dbUtilErrorCode == DBUtilErrorCode.MYSQL_QUERY_SELECT_PRI_ERROR && userName != null) {
                return asAddaxException(dbUtilErrorCode, "用户名为：" + userName + " 具体错误信息为：" + e);
            }

            return asAddaxException(dbUtilErrorCode, "执行的SQL为: " + querySql + " 具体错误信息为：" + e);
        }

        if (dataBaseType == DataBaseType.Oracle) {
            DBUtilErrorCode dbUtilErrorCode = oracleQueryErrorAna(e.getMessage());
            if (dbUtilErrorCode == DBUtilErrorCode.ORACLE_QUERY_TABLE_NAME_ERROR && table != null) {
                return asAddaxException(dbUtilErrorCode, "表名为：" + table + " 执行的SQL为:" + querySql + " 具体错误信息为：" + e);
            }
            if (dbUtilErrorCode == DBUtilErrorCode.ORACLE_QUERY_SELECT_PRI_ERROR) {
                return asAddaxException(dbUtilErrorCode, "用户名为：" + userName + " 具体错误信息为：" + e);
            }

            return asAddaxException(dbUtilErrorCode, "执行的SQL为: " + querySql + " 具体错误信息为：" + e);
        }

        return asAddaxException(DBUtilErrorCode.SQL_EXECUTE_FAIL, "执行的SQL为: " + querySql + " 具体错误信息为：" + e);
    }

    public static DBUtilErrorCode mySqlQueryErrorAna(String e)
    {
        if (e.contains(Constant.MYSQL_TABLE_NAME_ERR1) && e.contains(Constant.MYSQL_TABLE_NAME_ERR2)) {
            return DBUtilErrorCode.MYSQL_QUERY_TABLE_NAME_ERROR;
        }
        else if (e.contains(Constant.MYSQL_SELECT_PRI)) {
            return DBUtilErrorCode.MYSQL_QUERY_SELECT_PRI_ERROR;
        }
        else if (e.contains(Constant.MYSQL_COLUMN1) && e.contains(Constant.MYSQL_COLUMN2)) {
            return DBUtilErrorCode.MYSQL_QUERY_COLUMN_ERROR;
        }
        else if (e.contains(Constant.MYSQL_WHERE)) {
            return DBUtilErrorCode.MYSQL_QUERY_SQL_ERROR;
        }
        return DBUtilErrorCode.READ_RECORD_FAIL;
    }

    public static DBUtilErrorCode oracleQueryErrorAna(String e)
    {
        if (e.contains(Constant.ORACLE_TABLE_NAME)) {
            return DBUtilErrorCode.ORACLE_QUERY_TABLE_NAME_ERROR;
        }
        else if (e.contains(Constant.ORACLE_SQL)) {
            return DBUtilErrorCode.ORACLE_QUERY_SQL_ERROR;
        }
        else if (e.contains(Constant.ORACLE_SELECT_PRI)) {
            return DBUtilErrorCode.ORACLE_QUERY_SELECT_PRI_ERROR;
        }
        return DBUtilErrorCode.READ_RECORD_FAIL;
    }

    public static AddaxException asSqlParserException(DataBaseType dataBaseType, Exception e, String querySql)
    {
        if (dataBaseType == DataBaseType.MySql) {
            throw asAddaxException(DBUtilErrorCode.MYSQL_QUERY_SQL_PARSER_ERROR, "执行的SQL为:" + querySql + " 具体错误信息为：" + e);
        }
        if (dataBaseType == DataBaseType.Oracle) {
            throw asAddaxException(DBUtilErrorCode.ORACLE_QUERY_SQL_PARSER_ERROR, "执行的SQL为:" + querySql + " 具体错误信息为：" + e);
        }
        throw asAddaxException(DBUtilErrorCode.READ_RECORD_FAIL, "执行的SQL为:" + querySql + " 具体错误信息为：" + e);
    }

    public static AddaxException asPreSQLParserException(DataBaseType dataBaseType, Exception e, String querySql)
    {
        if (dataBaseType == DataBaseType.MySql) {
            throw asAddaxException(DBUtilErrorCode.MYSQL_PRE_SQL_ERROR, "执行的SQL为:" + querySql + " 具体错误信息为：" + e);
        }

        if (dataBaseType == DataBaseType.Oracle) {
            throw asAddaxException(DBUtilErrorCode.ORACLE_PRE_SQL_ERROR, "执行的SQL为:" + querySql + " 具体错误信息为：" + e);
        }
        throw asAddaxException(DBUtilErrorCode.READ_RECORD_FAIL, "执行的SQL为:" + querySql + " 具体错误信息为：" + e);
    }

    public static AddaxException asPostSQLParserException(DataBaseType dataBaseType, Exception e, String querySql)
    {
        if (dataBaseType == DataBaseType.MySql) {
            throw asAddaxException(DBUtilErrorCode.MYSQL_POST_SQL_ERROR, "执行的SQL为:" + querySql + " 具体错误信息为：" + e);
        }

        if (dataBaseType == DataBaseType.Oracle) {
            throw asAddaxException(DBUtilErrorCode.ORACLE_POST_SQL_ERROR, "执行的SQL为:" + querySql + " 具体错误信息为：" + e);
        }
        throw asAddaxException(DBUtilErrorCode.READ_RECORD_FAIL, "执行的SQL为:" + querySql + " 具体错误信息为：" + e);
    }

    public static AddaxException asInsertPriException(DataBaseType dataBaseType, String userName, String jdbcUrl)
    {
        if (dataBaseType == DataBaseType.MySql) {
            throw asAddaxException(DBUtilErrorCode.MYSQL_INSERT_ERROR, "用户名为:" + userName + " jdbcURL为：" + jdbcUrl);
        }

        if (dataBaseType == DataBaseType.Oracle) {
            throw asAddaxException(DBUtilErrorCode.ORACLE_INSERT_ERROR, "用户名为:" + userName + " jdbcURL为：" + jdbcUrl);
        }
        throw asAddaxException(DBUtilErrorCode.NO_INSERT_PRIVILEGE, "用户名为:" + userName + " jdbcURL为：" + jdbcUrl);
    }

    public static AddaxException asDeletePriException(DataBaseType dataBaseType, String userName, String jdbcUrl)
    {
        if (dataBaseType == DataBaseType.MySql) {
            throw asAddaxException(DBUtilErrorCode.MYSQL_DELETE_ERROR, "用户名为:" + userName + " jdbcURL为：" + jdbcUrl);
        }

        if (dataBaseType == DataBaseType.Oracle) {
            throw asAddaxException(DBUtilErrorCode.ORACLE_DELETE_ERROR, "用户名为:" + userName + " jdbcURL为：" + jdbcUrl);
        }
        throw asAddaxException(DBUtilErrorCode.NO_DELETE_PRIVILEGE, "用户名为:" + userName + " jdbcURL为：" + jdbcUrl);
    }

    public static AddaxException asSplitPKException(DataBaseType dataBaseType, Exception e, String splitSql, String splitPkID)
    {
        if (dataBaseType == DataBaseType.MySql) {

            return asAddaxException(DBUtilErrorCode.MYSQL_SPLIT_PK_ERROR, "配置的SplitPK为: " + splitPkID + ", 执行的SQL为: " + splitSql + " 具体错误信息为：" + e);
        }

        if (dataBaseType == DataBaseType.Oracle) {
            return asAddaxException(DBUtilErrorCode.ORACLE_SPLIT_PK_ERROR, "配置的SplitPK为: " + splitPkID + ", 执行的SQL为: " + splitSql + " 具体错误信息为：" + e);
        }

        return asAddaxException(DBUtilErrorCode.READ_RECORD_FAIL, splitSql + e);
    }
}
