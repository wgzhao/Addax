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

package com.wgzhao.addax.common.exception;

import com.wgzhao.addax.common.spi.ErrorCode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

public class AddaxException
        extends RuntimeException
{
    private static final Logger logger = LoggerFactory.getLogger(AddaxException.class);
    private static final long serialVersionUID = 1L;

    private final transient ErrorCode errorCode;

    public AddaxException(ErrorCode errorCode, String errorMessage)
    {
        super(errorMessage);
        logger.error("Error Code: {}: {}", errorCode.getCode(), errorMessage);
        this.errorCode = errorCode;
    }

    private AddaxException(ErrorCode errorCode, String errorMessage, Throwable cause)
    {
        super(errorMessage, cause);

        this.errorCode = errorCode;
    }

    public static AddaxException asAddaxException(ErrorCode errorCode, String message)
    {
        if (StringUtils.isBlank(message)) {
            message = errorCode.getDescription();
        }
        throw new RuntimeException(message);
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
        return new AddaxException(errorCode, getMessage(cause), cause);
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

    private static String getMessage(Object obj)
    {
        if (obj == null) {
            return "";
        }

        if (obj instanceof Throwable) {
            StringWriter str = new StringWriter();
            PrintWriter pw = new PrintWriter(str);
            ((Throwable) obj).printStackTrace(pw);
            return str.toString();
        }
        else {
            return obj.toString();
        }
    }

    public ErrorCode getErrorCode()
    {
        return this.errorCode;
    }
}
