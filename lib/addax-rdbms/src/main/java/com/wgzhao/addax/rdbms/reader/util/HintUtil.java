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

package com.wgzhao.addax.rdbms.reader.util;

import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.base.Key;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for handling database hints, particularly Oracle hints.
 * Provides functionality to apply table-specific hints to SQL queries based on configured patterns.
 */
public class HintUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(HintUtil.class);

    private static DataBaseType dataBaseType;
    private static Pattern tablePattern;
    private static String hintExpression;

    private HintUtil()
    {
        // Private constructor to prevent instantiation
    }

    /**
     * Initializes hint configuration from the provided configuration.
     * Parses hint expression and table pattern for later use in query building.
     *
     * @param type The database type
     * @param configuration Configuration containing hint settings
     */
    public static void initHintConf(DataBaseType type, Configuration configuration)
    {
        dataBaseType = type;

        String hint = configuration.getString(Key.HINT);
        if (StringUtils.isNotBlank(hint)) {
            String[] tablePatternAndHint = hint.split("#");
            if (tablePatternAndHint.length == 1) {
                tablePattern = Pattern.compile(".*");
                hintExpression = tablePatternAndHint[0];
            }
            else {
                tablePattern = Pattern.compile(tablePatternAndHint[0]);
                hintExpression = tablePatternAndHint[1];
            }
        }
    }

    /**
     * Builds a query column specification with optional database hints.
     * Currently, supports Oracle hints applied to tables matching the configured pattern.
     *
     * @param table The table name to check against hint patterns
     * @param column The column specification to potentially enhance with hints
     * @return The column specification, possibly enhanced with database hints
     */
    public static String buildQueryColumn(String table, String column)
    {
        try {
            if (tablePattern != null && DataBaseType.Oracle == dataBaseType) {
                Matcher m = tablePattern.matcher(table);
                if (m.find()) {
                    String[] tableStr = table.split("\\.");
                    String tableWithoutSchema = tableStr[tableStr.length - 1];
                    String finalHint = hintExpression.replaceAll(Constant.TABLE_NAME_PLACEHOLDER, tableWithoutSchema);
                    LOG.info("table:{} use hint:{}.", table, finalHint);
                    return finalHint + column;
                }
            }
        }
        catch (Exception e) {
            LOG.warn("match hint exception, will not use hint", e);
        }
        return column;
    }
}
