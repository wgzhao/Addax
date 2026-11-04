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

package com.wgzhao.addax.plugin.reader.sqlitereader;

import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.rdbms.reader.CommonRdbmsReader;
import com.wgzhao.addax.rdbms.util.DataBaseType;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Locale;

/**
 * SQLite specific column conversion.
 *
 * <p>SQLite columns are dynamically typed: the JDBC driver derives a column's type
 * from the storage class of the value it is currently positioned on. A DECIMAL
 * column whose first row holds an integral value is therefore reported as INTEGER,
 * and the shared reader, which caches the column types once per result set, reads
 * every later fractional value through asLong() and drops the fraction.
 */
class SqliteReaderTask
        extends CommonRdbmsReader.Task
{
    SqliteReaderTask(DataBaseType dataBaseType, int taskGroupId, int taskId)
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

        String typeName = baseTypeName(cachedTypeNames[i]);
        if ("DECIMAL".equals(typeName) || "NUMERIC".equals(typeName)) {
            // the declared type is authoritative; the driver's JDBC type follows the
            // first row's storage class and cannot be trusted for this column
            return new DoubleColumn(rs.getString(i));
        }
        return super.createColumn(rs, metaData, i);
    }

    /** Reduces a driver type name such as {@code DECIMAL(12,3)} to its base name. */
    private static String baseTypeName(String typeName)
    {
        if (typeName == null) {
            return "";
        }
        int parenthesis = typeName.indexOf('(');
        return (parenthesis < 0 ? typeName : typeName.substring(0, parenthesis)).trim().toUpperCase(Locale.ENGLISH);
    }
}
