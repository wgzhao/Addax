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

package com.wgzhao.addax.plugin.rdbms.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TableExpandUtil
{

    // schema.table[0-2]more
    // 1 2 3 4 5
    public static Pattern pattern = Pattern
            .compile("(\\w+\\.)?(\\w+)\\[(\\d+)-(\\d+)\\](.*)");

    private TableExpandUtil()
    {
    }

    /**
     * Split the table string(Usually contains names of some tables) to a List
     * that is formated. example: table[0-32] will be splitted into `table0`,
     * `table1`, `table2`, ... ,`table32` in {@link List}
     *
     * @param tables a string contains table name(one or many).
     * @return a split result of table name.
     */
    public static List<String> splitTables(String tables)
    {
        List<String> splittedTables = new ArrayList<>();

        String[] tableArrays = tables.split(",");

        String tableName;
        for (String tableArray : tableArrays) {
            Matcher matcher = pattern.matcher(tableArray.trim());
            if (!matcher.matches()) {
                tableName = tableArray.trim();
                splittedTables.add(tableName);
            }
            else {
                String start = matcher.group(3).trim();
                String end = matcher.group(4).trim();
                String tmp;
                if (Integer.parseInt(start) > Integer.parseInt(end)) {
                    tmp = start;
                    start = end;
                    end = tmp;
                }
                int len = start.length();
                String schema;
                for (int k = Integer.parseInt(start); k <= Integer.parseInt(end); k++) {
                    schema = (null == matcher.group(1)) ? "" : matcher.group(1)
                            .trim();
                    if (start.startsWith("0")) {
                        tableName = schema + matcher.group(2).trim()
                                + String.format("%0" + len + "d", k)
                                + matcher.group(5).trim();
                    }
                    else {
                        tableName = schema + matcher.group(2).trim()
                                + String.format("%d", k)
                                + matcher.group(5).trim();
                    }
                    splittedTables.add(tableName);
                }
            }
        }
        return splittedTables;
    }

    public static List<String> expandTableConf(List<String> tables)
    {
        List<String> parsedTables = new ArrayList<>();
        for (String table : tables) {
            List<String> splittedTables = splitTables(table);
            parsedTables.addAll(splittedTables);
        }

        return parsedTables;
    }
}
