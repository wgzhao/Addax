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


/**
 * Custom exception class for RDBMS-related errors in Addax.
 * Extends AddaxException with specific error handling for database operations.
 */
public class RdbmsException
        extends AddaxException
{
    /**
     * Constructs a new RdbmsException with the specified error code and message.
     *
     * @param errorCode The error code indicating the type of error
     * @param message The detail message explaining the error
     */
    public RdbmsException(ErrorCode errorCode, String message)
    {
        super(errorCode, message);
    }

    /**
     * Creates an AddaxException for database connection errors.
     *
     * @param e The underlying exception that caused the connection failure
     * @return AddaxException with CONNECTION_ERROR code
     */
    public static AddaxException asConnException(Exception e)
    {
        return asAddaxException(CONNECT_ERROR, e.getMessage());
    }

    /**
     * Creates an AddaxException for SQL query execution errors.
     *
     * @param e The underlying exception that caused the query failure
     * @param querySql The SQL query that failed (currently unused but kept for API compatibility)
     * @return AddaxException with EXECUTE_FAIL code
     */
    public static AddaxException asQueryException(Exception e, String querySql)
    {
        return asAddaxException(EXECUTE_FAIL, e.getMessage());
    }

    /**
     * Creates an AddaxException for SQL parser errors.
     *
     * @param e The underlying exception that caused the parsing failure
     * @param querySql The SQL query that failed to parse (currently unused but kept for API compatibility)
     * @return AddaxException with EXECUTE_FAIL code
     * @throws AddaxException Always throws the created exception
     */
    public static AddaxException asSqlParserException(Exception e, String querySql)
    {
        throw asAddaxException(EXECUTE_FAIL, e.getMessage());
    }

    /**
     * Creates an AddaxException for pre-SQL parser errors.
     *
     * @param e The underlying exception that caused the parsing failure
     * @param querySql The pre-SQL query that failed to parse (currently unused but kept for API compatibility)
     * @return AddaxException with EXECUTE_FAIL code
     * @throws AddaxException Always throws the created exception
     */
    public static AddaxException asPreSQLParserException(Exception e, String querySql)
    {
        throw asAddaxException(EXECUTE_FAIL, e.getMessage());
    }

    /**
     * Creates an AddaxException for post-SQL parser errors.
     *
     * @param e The underlying exception that caused the parsing failure
     * @param querySql The post-SQL query that failed to parse (currently unused but kept for API compatibility)
     * @return AddaxException with EXECUTE_FAIL code
     * @throws AddaxException Always throws the created exception
     */
    public static AddaxException asPostSQLParserException(Exception e, String querySql)
    {
        throw asAddaxException(EXECUTE_FAIL, e.getMessage());
    }

    /**
     * Creates an AddaxException for INSERT privilege errors.
     *
     * @param userName The username that lacks INSERT privileges
     * @param jdbcUrl The JDBC URL where the privilege check failed
     * @return AddaxException with PERMISSION_ERROR code
     * @throws AddaxException Always throws the created exception
     */
    public static AddaxException asInsertPriException(String userName, String jdbcUrl)
    {
        throw asAddaxException(PERMISSION_ERROR, "User " + userName + " lacks INSERT privilege on " + jdbcUrl);
    }

    /**
     * Creates an AddaxException for DELETE privilege errors.
     *
     * @param userName The username that lacks DELETE privileges
     * @param jdbcUrl The JDBC URL where the privilege check failed
     * @return AddaxException with PERMISSION_ERROR code
     * @throws AddaxException Always throws the created exception
     */
    public static AddaxException asDeletePriException(String userName, String jdbcUrl)
    {
        throw asAddaxException(PERMISSION_ERROR, "User " + userName + " lacks DELETE privilege on " + jdbcUrl);
    }

    /**
     * Creates an AddaxException for primary key split errors.
     *
     * @param e The underlying exception that caused the split failure
     * @param splitSql The SQL used for splitting (currently unused but kept for API compatibility)
     * @param splitPkID The primary key used for splitting (currently unused but kept for API compatibility)
     * @return AddaxException with EXECUTE_FAIL code
     */
    public static AddaxException asSplitPKException(Exception e, String splitSql, String splitPkID)
    {
        return asAddaxException(EXECUTE_FAIL, e.getMessage());
    }
}
