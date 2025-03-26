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

package com.wgzhao.addax.core.exception;

import com.wgzhao.addax.core.spi.ErrorCode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;

public class AddaxException
        extends RuntimeException
{
    private static final Logger logger = LoggerFactory.getLogger(AddaxException.class);
    private static final long serialVersionUID = 1L;

    private final transient ErrorCode errorCode;

    public AddaxException(ErrorCode errorCode, String errorMessage)
    {
        super(errorMessage);
        this.errorCode = Objects.requireNonNull(errorCode, "ErrorCode cannot be null");
//        logger.error("Error Code: {}: {}", errorCode.getCode(), errorMessage);
    }

    private AddaxException(ErrorCode errorCode, String errorMessage, Throwable cause)
    {
        super(errorMessage, cause);
        this.errorCode = Objects.requireNonNull(errorCode, "ErrorCode cannot be null");
//        logger.error("Error Code: {}: {}", errorCode.getCode(), errorMessage, cause);
    }

    public static AddaxException asAddaxException(ErrorCode errorCode, String message)
    {
        String errorMessage = StringUtils.isBlank(message) ? errorCode.getDescription() : message;
        return new AddaxException(errorCode, errorMessage);
    }

    public static AddaxException asAddaxException(ErrorCode errorCode, String message, Throwable cause)
    {
        if (cause instanceof AddaxException) {
            return (AddaxException) cause;
        }
        return new AddaxException(errorCode, message, cause);
    }

    public static AddaxException asAddaxException(ErrorCode errorCode, Throwable cause)
    {
        if (cause instanceof AddaxException) {
            return (AddaxException) cause;
        }
        return new AddaxException(errorCode, formatCauseMessage(cause), cause);
    }

    public static AddaxException illegalConfigValue(String configName, Object configValue)
    {
        return new AddaxException(ErrorCode.ILLEGAL_VALUE,
                String.format("The configuration value for '%s' is illegal or unsupported: '%s'", configName, configValue));
    }

    public static AddaxException illegalConfigValue(String configName, Object configValue, String message)
    {
        return new AddaxException(ErrorCode.ILLEGAL_VALUE,
                String.format("The configuration value for '%s' is illegal or unsupported: '%s': %s", configName, configValue, message));
    }

    public static AddaxException missingConfig(String configName)
    {
        return new AddaxException(ErrorCode.REQUIRED_VALUE,
                "The configuration for '" + configName + "' is required but not provided");
    }

    private static String formatCauseMessage(Throwable cause)
    {
        if (cause == null) {
            return "";
        }

        // For exceptions, use stack trace
        StringWriter str = new StringWriter();
        PrintWriter pw = new PrintWriter(str);
        cause.printStackTrace(pw);
        return str.toString();
    }

    public ErrorCode getErrorCode()
    {
        return this.errorCode;
    }
}