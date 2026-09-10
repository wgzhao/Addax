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

package com.wgzhao.addax.plugin.writer.hdfswriter;

import java.util.Locale;

/** Support Hive Data Type. */
public enum SupportHiveDataType
{
    TINYINT,
    SMALLINT,
    INT,
    INTEGER,
    BIGINT,
    FLOAT,
    DOUBLE,
    TIMESTAMP,
    DATE,
    DECIMAL,
    STRING,
    VARCHAR,
    CHAR,
    LONG,
    BOOLEAN,
    BINARY,
    MAP,
    ARRAY;

    /**
     * Resolve a configured type name to its constant.
     * <p>
     * The configuration is written in the SQL/Hive spelling, which {@link #valueOf} cannot read:
     * it carries aliases ({@code integer} for {@code int}, {@code long} for {@code bigint}) and
     * parameters ({@code varchar(10)}, {@code char(3)}, {@code decimal(10,2)}). Calling
     * {@code valueOf} on the raw string threw {@link IllegalArgumentException} deep inside a task,
     * after part of the data had already been read.
     *
     * @param type the configured type, for example {@code "varchar(10)"}
     * @return the matching constant
     * @throws IllegalArgumentException if the type is not supported
     */
    public static SupportHiveDataType of(String type)
    {
        String name = type.trim().toUpperCase(Locale.ROOT);
        int parameters = name.indexOf('(');
        if (parameters >= 0) {
            name = name.substring(0, parameters).trim();
        }

        return switch (name) {
            case "INTEGER" -> INT;
            case "LONG" -> BIGINT;
            default -> valueOf(name);
        };
    }
}
