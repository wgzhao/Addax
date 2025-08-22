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

import java.sql.Connection;

/**
 * Factory interface for creating database connections.
 * Provides methods to obtain connections with different retry behaviors.
 */
public interface ConnectionFactory
{
    /**
     * Gets a database connection with retry mechanism enabled.
     *
     * @return A database connection
     */
    Connection getConnection();

    /**
     * Gets a database connection without retry mechanism.
     * Fails immediately if connection cannot be established.
     *
     * @return A database connection
     */
    Connection getConnectionWithoutRetry();

    /**
     * Gets connection information string for logging and debugging purposes.
     *
     * @return Connection information as a string
     */
    String getConnectionInfo();
}
