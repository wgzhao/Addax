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

package com.wgzhao.addax.plugin.writer.txtfilewriter;

import com.wgzhao.addax.common.spi.ErrorCode;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public enum TxtFileWriterErrorCode
        implements ErrorCode
{

    CONFIG_INVALID_EXCEPTION("TxtFileWriter-00", "error value."),
    REQUIRED_VALUE("TxtFileWriter-01", "missing mandatory parameters"),
    ILLEGAL_VALUE("TxtFileWriter-02", "illegal value"),
    WRITE_FILE_ERROR("TxtFileWriter-03", "failed to write file."),
    WRITE_FILE_IO_ERROR("TxtFileWriter-04", "IOException occurred when writing the file."),
    SECURITY_NOT_ENOUGH("TxtFileWriter-05", "permission denied"),
    PATH_NOT_VALID("TxtFileWriter-06", "invalid path"),
    PAHT_NOT_DIR("TxtFileWriter-06", "the path is not directory.");

    private final String code;
    private final String description;

    TxtFileWriterErrorCode(String code, String description)
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
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }
}
