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

import java.io.PrintWriter;
import java.io.StringWriter;

public class AddaxException
        extends RuntimeException
{

    private static final long serialVersionUID = 1L;

    private final transient ErrorCode errorCode;

    public AddaxException(ErrorCode errorCode, String errorMessage)
    {
        super(errorMessage);
        this.errorCode = errorCode;
    }

    private AddaxException(ErrorCode errorCode, String errorMessage, Throwable cause)
    {
        super(errorMessage, cause);

        this.errorCode = errorCode;
    }

    public static AddaxException asAddaxException(ErrorCode errorCode, String message)
    {
        return new AddaxException(errorCode, message);
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
