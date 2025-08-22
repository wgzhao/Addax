/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.wgzhao.addax.rdbms.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for expanding table name patterns into concrete table names.
 * Supports pattern-based table name expansion with range syntax like table[0-32].
 */
public final class TableExpandUtil
{
    // Groups: 1=schema, 2=table, 3=start, 4=end, 5=suffix
    public static final Pattern PATTERN = Pattern.compile("(\\w+\\.)?(\\w+)\\[(\\d+)-(\\d+)\\](.*)");

    private TableExpandUtil()
    {
        // Private constructor to prevent instantiation
    }

    /**
     * Splits table string containing table names with range patterns into a list of concrete table names.
     * <p>
     * Example: "table[0-32]" will be split into "table0", "table1", "table2", ..., "table32"
     * </p>
     *
     * @param tables A string containing table names (one or many, comma-separated)
     * @return A list of expanded table names
     */
    public static List<String> splitTables(String tables)
    {
        List<String> splitTables = new ArrayList<>();

        String[] tableArrays = tables.split(",");

        String tableName;
        for (String tableArray : tableArrays) {
            Matcher matcher = PATTERN.matcher(tableArray.trim());
            if (!matcher.matches()) {
                tableName = tableArray.trim();
                splitTables.add(tableName);
            }
            else {
                String start = matcher.group(3).trim();
                String end = matcher.group(4).trim();
                
                // Ensure start <= end
                if (Integer.parseInt(start) > Integer.parseInt(end)) {
                    String temp = start;
                    start = end;
                    end = temp;
                }
                
                int paddingLength = start.length();
                String schema = (matcher.group(1) == null) ? "" : matcher.group(1).trim();
                
                for (int k = Integer.parseInt(start); k <= Integer.parseInt(end); k++) {
                    if (start.startsWith("0")) {
                        // Preserve zero-padding format
                        tableName = schema + matcher.group(2).trim()
                                + String.format("%0" + paddingLength + "d", k)
                                + matcher.group(5).trim();
                    }
                    else {
                        tableName = schema + matcher.group(2).trim()
                                + k
                                + matcher.group(5).trim();
                    }
                    splitTables.add(tableName);
                }
            }
        }
        return splitTables;
    }

    /**
     * Expands table configurations based on database type and table patterns.
     * Handles special cases like SQL Server's bracket notation for table names containing commas.
     *
     * @param dataBaseType The database type
     * @param tables List of table configurations to expand
     * @return List of expanded table names
     */
    public static List<String> expandTableConf(DataBaseType dataBaseType, List<String> tables)
    {
        List<String> parsedTables = new ArrayList<>();
        for (String table : tables) {
            if (table.startsWith("[") && dataBaseType == DataBaseType.SQLServer) {
                // SQL Server allows table or column names to include commas, quoted with brackets
                parsedTables.add(table);
            } 
            else {
                List<String> expandedTables = splitTables(table);
                parsedTables.addAll(expandedTables);
            }
        }

        return parsedTables;
    }
}
