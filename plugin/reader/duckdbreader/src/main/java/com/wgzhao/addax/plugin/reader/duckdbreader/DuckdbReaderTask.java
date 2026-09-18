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

package com.wgzhao.addax.plugin.reader.duckdbreader;

import com.alibaba.fastjson2.JSON;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.element.TimestampColumn;
import com.wgzhao.addax.rdbms.reader.CommonRdbmsReader;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import org.duckdb.DuckDBStruct;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * DuckDB specific column conversion.
 *
 * <p>Most DuckDB types map onto standard JDBC ones and are left to the shared reader. The
 * types below need to be taken over here because the shared mapping either loses precision,
 * loses the structure of a value, or fails outright.
 */
class DuckdbReaderTask
        extends CommonRdbmsReader.Task
{
    DuckdbReaderTask(DataBaseType dataBaseType, int taskGroupId, int taskId)
    {
        super(dataBaseType, taskGroupId, taskId);
    }

    // the shared reader keeps its own metadata cache private, so this task needs its own
    private ResultSetMetaData cachedMetaData;
    private String[] cachedTypeNames;

    @Override
    protected Column createColumn(ResultSet rs, ResultSetMetaData metaData, int i)
            throws SQLException, UnsupportedEncodingException
    {
        if (cachedMetaData != metaData) {
            int columnCount = metaData.getColumnCount();
            String[] typeNames = new String[columnCount + 1];
            for (int k = 1; k <= columnCount; k++) {
                typeNames[k] = metaData.getColumnTypeName(k);
            }
            cachedTypeNames = typeNames;
            cachedMetaData = metaData;
        }

        switch (baseTypeName(cachedTypeNames[i])) {
            case "BIT":
                // DuckDB's BIT is a bit string of arbitrary width and its driver rejects both
                // getBoolean and getBytes for it, which would drop the whole row as a dirty
                // record; the printable 0/1 form is the only lossless representation
                return new StringColumn(rs.getString(i));

            case "DECIMAL":
            case "HUGEINT":
            case "UHUGEINT":
                // 128-bit and fixed-point values exceed what a double column can hold exactly,
                // so they travel as text and are parsed back by a numeric target column
                return new StringColumn(rs.getString(i));

            case "JSON": {
                // JsonNode.toString() returns the raw JSON source, unlike getString which the
                // driver does not guarantee for this type
                Object value = rs.getObject(i);
                return new StringColumn(Objects.isNull(value) ? null : value.toString());
            }

            case "MAP":
            case "STRUCT":
            case "LIST":
            case "ARRAY": {
                // the shared fallback stringifies these with java.util.Object semantics, which
                // produces an unparseable "{a=1}" or "[1, 2]"; JSON keeps the nesting readable
                Object value = rs.getObject(i);
                return new StringColumn(Objects.isNull(value) ? null : JSON.toJSONString(normalize(value)));
            }

            case "TIMESTAMP WITH TIME ZONE": {
                // collapsing the offset onto the instant keeps a database to database copy
                // consistent, matching how a PostgreSQL timestamptz is handled
                OffsetDateTime value = rs.getObject(i, OffsetDateTime.class);
                return new TimestampColumn(value == null ? null : Timestamp.from(value.toInstant()));
            }

            default:
                return super.createColumn(rs, metaData, i);
        }
    }

    /**
     * Reduces a DuckDB type name to its base name.
     * The driver reports parameterised types as {@code DECIMAL(10,2)}, nested ones as
     * {@code STRUCT(a INTEGER)} and lists as either {@code INTEGER[]} or {@code INTEGER[3]}.
     */
    private static String baseTypeName(String typeName)
    {
        if (typeName == null) {
            return "";
        }
        if (typeName.endsWith("[]")) {
            return "LIST";
        }
        if (typeName.endsWith("]")) {
            return "ARRAY";
        }
        int parenthesis = typeName.indexOf('(');
        return parenthesis < 0 ? typeName.trim() : typeName.substring(0, parenthesis).trim();
    }

    /**
     * Converts the driver's composite values into plain Java containers so that a nested value
     * serializes as real JSON rather than through reflection over driver internals.
     */
    private static Object normalize(Object value)
            throws SQLException
    {
        if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean) {
            return value;
        }
        if (value instanceof DuckDBStruct struct) {
            // getMap keeps the field names, which getAttributes alone would lose
            Map<String, Object> fields = new LinkedHashMap<>();
            for (Map.Entry<String, Object> field : struct.getMap().entrySet()) {
                fields.put(field.getKey(), normalize(field.getValue()));
            }
            return fields;
        }
        if (value instanceof java.sql.Array array) {
            value = array.getArray();
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> fields = new LinkedHashMap<>();
            for (Map.Entry<?, ?> field : map.entrySet()) {
                fields.put(String.valueOf(field.getKey()), normalize(field.getValue()));
            }
            return fields;
        }
        if (value instanceof Object[] elements) {
            List<Object> normalized = new ArrayList<>(elements.length);
            for (Object element : elements) {
                normalized.add(normalize(element));
            }
            return normalized;
        }
        return value;
    }
}
