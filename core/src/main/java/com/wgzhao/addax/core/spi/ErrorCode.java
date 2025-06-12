/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.core.spi;


/**
 * Enum representing common error codes and their descriptions.
 */
public enum ErrorCode
{
    // Required and Illegal Values
    REQUIRED_VALUE("1001", "Missing required parameters"),
    ILLEGAL_VALUE("1002", "Illegal parameter value"),
    CONFIG_ERROR("1003", "Configuration error"),

    // Permission and Connection Errors
    PERMISSION_ERROR("2001", "Permission denied"),
    CONNECT_ERROR("2002", "Connection error"),
    LOGIN_ERROR("2003", "Login error"),

    // Data Conversion Errors
    CONVERT_NOT_SUPPORT("3001", "Unsupported data type conversion"),
    NOT_SUPPORT_TYPE("3002", "Unsupported data type"),
    CONVERT_OVER_FLOW("3003", "Data type conversion overflow"),
    ENCODING_ERROR("3004", "Encoding error"),

    // Execution Errors
    RETRY_FAIL("4001", "Retry failed"),
    EXECUTE_FAIL("4002", "Execution failed"),
    IO_ERROR("4003", "IO error"),

    // Runtime Errors
    RUNTIME_ERROR("5001", "Runtime error"),
    HOOK_INTERNAL_ERROR("5002", "Hook internal error"),
    SHUT_DOWN_TASK("5003", "Task shutdown"),
    WAIT_TIME_EXCEED("5004", "Wait time exceeded"),
    TASK_HUNG_EXPIRED("5005", "Task hung expired"),

    // Plugin Errors
    PLUGIN_INSTALL_ERROR("6001", "Plugins installation error"),
    PLUGIN_INIT_ERROR("6002", "Plugins initialization error"),
    OVER_LIMIT_ERROR("6003", "Over limit error");

    private final String code;

    private final String description;

    ErrorCode(String code, String description)
    {
        if (code == null || code.isEmpty()) {
            throw new IllegalArgumentException("Error code cannot be null or empty");
        }
        if (description == null || description.isEmpty()) {
            throw new IllegalArgumentException("Error description cannot be null or empty");
        }
        this.code = code;
        this.description = description;
    }

    public String getCode()
    {
        return this.code;
    }

    public String getDescription()
    {
        return this.description;
    }

    public String toString()
    {
        return String.format("Error code: %s, Description: %s", this.code, this.description);
    }
}
