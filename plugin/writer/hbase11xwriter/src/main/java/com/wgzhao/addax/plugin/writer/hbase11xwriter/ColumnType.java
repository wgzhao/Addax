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

package com.wgzhao.addax.plugin.writer.hbase11xwriter;

import com.wgzhao.addax.common.exception.DataXException;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;

/**
 * 只对 normal 模式读取时有用，多版本读取时，不存在列类型的
 */
public enum ColumnType
{
    STRING("string"),
    BOOLEAN("boolean"),
    SHORT("short"),
    INT("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double");

    private final String typeName;

    ColumnType(String typeName)
    {
        this.typeName = typeName;
    }

    public static ColumnType getByTypeName(String typeName)
    {
        if (StringUtils.isBlank(typeName)) {
            throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE,
                    String.format("Hbasewriter 不支持该类型:%s, 目前支持的类型是:%s", typeName, Arrays.asList(values())));
        }
        for (ColumnType columnType : values()) {
            if (StringUtils.equalsIgnoreCase(columnType.typeName, typeName.trim())) {
                return columnType;
            }
        }

        throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE,
                String.format("Hbasewriter 不支持该类型:%s, 目前支持的类型是:%s", typeName, Arrays.asList(values())));
    }

    @Override
    public String toString()
    {
        return this.typeName;
    }
}
