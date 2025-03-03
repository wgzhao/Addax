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

package com.wgzhao.addax.plugin.reader.hbase20xreader;

import com.wgzhao.addax.common.exception.AddaxException;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

import static com.wgzhao.addax.common.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;

public enum ColumnType
{
    BOOLEAN("boolean"),
    SHORT("short"),
    INT("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double"),
    DATE("date"),
    STRING("string"),
    BINARY_STRING("binarystring");

    private final String typeName;

    ColumnType(String typeName)
    {
        this.typeName = typeName;
    }

    public static ColumnType getByTypeName(String typeName)
    {
        if (StringUtils.isBlank(typeName)) {
            throw AddaxException.asAddaxException(REQUIRED_VALUE, "The configuration item type is required ");
        }
        for (ColumnType columnType : values()) {
            if (StringUtils.equalsIgnoreCase(columnType.typeName, typeName.trim())) {
                return columnType;
            }
        }

        throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE, "The type '" + typeName + "' is not supported");
    }

    @Override
    public String toString()
    {
        return this.typeName;
    }
}
