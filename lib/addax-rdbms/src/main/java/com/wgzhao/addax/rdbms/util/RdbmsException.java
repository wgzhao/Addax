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

import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.spi.ErrorCode;

import static com.wgzhao.addax.core.spi.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.PERMISSION_ERROR;


public class RdbmsException
        extends AddaxException
{
    public RdbmsException(ErrorCode errorCode, String message)
    {
        super(errorCode, message);
    }

    public static AddaxException asConnException(Exception e)
    {
        return asAddaxException(CONNECT_ERROR, e.getMessage());
    }

    public static AddaxException asQueryException(Exception e, String querySql)
    {
        return asAddaxException(EXECUTE_FAIL, e.getMessage());
    }

    public static AddaxException asSqlParserException(Exception e, String querySql)
    {
        throw asAddaxException(EXECUTE_FAIL, e.getMessage());
    }

    public static AddaxException asPreSQLParserException(Exception e, String querySql)
    {
        throw asAddaxException(EXECUTE_FAIL, e.getMessage());
    }

    public static AddaxException asPostSQLParserException(Exception e, String querySql)
    {
        throw asAddaxException(EXECUTE_FAIL, e.getMessage());
    }

    public static AddaxException asInsertPriException(String userName, String jdbcUrl)
    {
        throw asAddaxException(PERMISSION_ERROR, "");
    }

    public static AddaxException asDeletePriException(String userName, String jdbcUrl)
    {
        throw asAddaxException(PERMISSION_ERROR, "");
    }

    public static AddaxException asSplitPKException(Exception e, String splitSql, String splitPkID)
    {
        return asAddaxException(EXECUTE_FAIL, e.getMessage());
    }
}
