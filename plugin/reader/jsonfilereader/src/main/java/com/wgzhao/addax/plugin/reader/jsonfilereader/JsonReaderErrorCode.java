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

package com.wgzhao.addax.plugin.reader.jsonfilereader;

import com.wgzhao.addax.common.spi.ErrorCode;

public enum JsonReaderErrorCode
        implements ErrorCode
{
    REQUIRED_VALUE("JsonFilereader-00", "Missing required value"),
    ILLEGAL_VALUE("JsonFilereader-01", "Illegal value"),
    MIXED_INDEX_VALUE("JsonFilereader-02", "Both configure index and value."),
    NO_INDEX_VALUE("JsonFilereader-03", "You specify columns, but not configure index and value "),
    FILE_NOT_EXISTS("JsonFilereader-04", "Directory not exists."),
    OPEN_FILE_ERROR("JsonFilereader-06", "Failed to open the file."),
    READ_FILE_IO_ERROR("JsonFilereader-07", "IOException occurred when open file"),
    SECURITY_NOT_ENOUGH("JsonFilereader-08", "Permission denied"),
    CONFIG_INVALID_EXCEPTION("JsonFilereader-09", "incorrect configure."),
    NOT_SUPPORT_TYPE("JsonFilereader-10", "The type is unsupported."),
    EMPTY_DIR_EXCEPTION("JsonFilereader-11", "Empty directory"),
    ;

    private final String code;
    private final String description;

    JsonReaderErrorCode(String code, String description)
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
        return String.format("Code:[%s], Description:[%s].", this.code, this.description);
    }
}



