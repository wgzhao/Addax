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

    public static AddaxException asConnException(Exception e)
    {
        return asAddaxException(DBUtilErrorCode.CONN_DB_ERROR, " 具体错误信息为：" + e);
    }

    public static AddaxException asQueryException(Exception e, String querySql)
    {
        return asAddaxException(DBUtilErrorCode.SQL_EXECUTE_FAIL, "执行的SQL为: " + querySql + " 具体错误信息为：" + e);
    }

    public static AddaxException asSqlParserException(Exception e, String querySql)
    {
        throw asAddaxException(DBUtilErrorCode.READ_RECORD_FAIL, "执行的SQL为:" + querySql + " 具体错误信息为：" + e);
    }

    public static AddaxException asPreSQLParserException(Exception e, String querySql)
    {
        throw asAddaxException(DBUtilErrorCode.READ_RECORD_FAIL, "执行的SQL为:" + querySql + " 具体错误信息为：" + e);
    }

    public static AddaxException asPostSQLParserException(Exception e, String querySql)
    {
        throw asAddaxException(DBUtilErrorCode.READ_RECORD_FAIL, "执行的SQL为:" + querySql + " 具体错误信息为：" + e);
    }

    public static AddaxException asInsertPriException(String userName, String jdbcUrl)
    {
        throw asAddaxException(DBUtilErrorCode.NO_INSERT_PRIVILEGE, "用户名为:" + userName + " jdbcURL为：" + jdbcUrl);
    }

    public static AddaxException asDeletePriException(String userName, String jdbcUrl)
    {
        throw asAddaxException(DBUtilErrorCode.NO_DELETE_PRIVILEGE, "用户名为:" + userName + " jdbcURL为：" + jdbcUrl);
    }

    public static AddaxException asSplitPKException(Exception e, String splitSql, String splitPkID)
    {
        return asAddaxException(DBUtilErrorCode.READ_RECORD_FAIL, "配置的SplitPK为: " + splitPkID + ", 执行的SQL为: " + splitSql + " 具体错误信息为：" + e);
    }
}
