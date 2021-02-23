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

package com.wgzhao.datax.plugin.writer.kuduwriter;

import com.wgzhao.datax.common.exception.DataXException;

import java.util.Arrays;

public enum ColumnType
{
    INT("int"),
    FLOAT("float"),
    STRING("string"),
    BIGINT("bigint"),
    DOUBLE("double"),
    BOOLEAN("boolean"),
    LONG("long");
    private final String mode;

    ColumnType(String mode)
    {
        this.mode = mode.toLowerCase();
    }

    public static ColumnType getByTypeName(String modeName)
    {
        for (ColumnType modeType : values()) {
            if (modeType.mode.equalsIgnoreCase(modeName)) {
                return modeType;
            }
        }
        throw DataXException.asDataXException(KuduWriterErrorCode.ILLEGAL_VALUE,
                String.format("Kuduwriter does not support the type:%s, currently supported types are:%s",
                        modeName, Arrays.asList(values())));
    }

    public String getMode()
    {
        return mode;
    }
}
