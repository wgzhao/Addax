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
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by liuyi on 15/9/18.
 */
public class HintUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(HintUtil.class);

    private static DataBaseType dataBaseType;
    private static Pattern tablePattern;
    private static String hintExpression;

    private HintUtil() {}

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
