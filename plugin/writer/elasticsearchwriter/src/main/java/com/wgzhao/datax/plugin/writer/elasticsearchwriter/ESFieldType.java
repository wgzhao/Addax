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

package com.wgzhao.datax.plugin.writer.elasticsearchwriter;

/**
 * Created by xiongfeng.bxf on 17/3/1.
 */
public enum ESFieldType
{
    ID,
    STRING,
    TEXT,
    KEYWORD,
    LONG,
    INTEGER,
    SHORT,
    BYTE,
    DOUBLE,
    FLOAT,
    DATE,
    BOOLEAN,
    BINARY,
    INTEGER_RANGE,
    FLOAT_RANGE,
    LONG_RANGE,
    DOUBLE_RANGE,
    DATE_RANGE,
    GEO_POINT,
    GEO_SHAPE,

    IP,
    COMPLETION,
    TOKEN_COUNT,

    ARRAY,
    OBJECT,
    FLATTENED,
    NESTED;

    public static ESFieldType getESFieldType(String type)
    {
        if (type == null) {
            return null;
        }
        for (ESFieldType f : ESFieldType.values()) {
            if (f.name().compareTo(type.toUpperCase()) == 0) {
                return f;
            }
        }
        return null;
    }
}
