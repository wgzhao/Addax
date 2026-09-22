/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.wgzhao.addax.plugin.reader.hdfsreader;

import java.util.Locale;

/** Java Type. */
public enum JavaType {
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
    ARRAY,
    MAP;

    /**
     * Resolve a configured column type to its constant.
     * <p>
     * The job configuration carries the same spellings the writer side accepts, which
     * {@link #valueOf} cannot read: aliases ({@code integer}, {@code long}, {@code byte}),
     * parameters ({@code varchar(10)}, {@code decimal(10,2)}) and the collection generics
     * ({@code array<string>}, {@code map<string,int>}). {@code valueOf} threw on every one of
     * them, inside the read loop, where the failure showed up as a dirty record per row (or as
     * a job that died on the first record) instead of as a configuration error.
     *
     * @param type the configured type, for example {@code "array<string>"}
     * @return the matching constant
     * @throws IllegalArgumentException if the type is not supported
     */
    public static JavaType of(String type)
    {
        String name = type.trim().toUpperCase(Locale.ROOT);
        if (name.startsWith("ARRAY<")) {
            return ARRAY;
        }
        if (name.startsWith("MAP<")) {
            return MAP;
        }
        int parameters = name.indexOf('(');
        if (parameters >= 0) {
            // varchar(10), decimal(10,2): the parameters do not change the java type
            name = name.substring(0, parameters).trim();
        }
        return switch (name) {
            case "BYTE" -> TINYINT;
            case "SHORT" -> SMALLINT;
            case "INTEGER" -> INT;
            case "LONG" -> BIGINT;
            default -> valueOf(name);
        };
    }
}
