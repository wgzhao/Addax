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

package com.wgzhao.addax.plugin.writer.databendwriter.util;

import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.StringJoiner;

public final class DatabendWriterUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DatabendWriterUtil.class);

    public final static String ONCONFLICT_COLUMN = "onConflictColumn";

    private DatabendWriterUtil() {
    }

    public static void dealWriteMode(Configuration originalConfig) {
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
        List<String> onConflictColumns = originalConfig.getList(ONCONFLICT_COLUMN, String.class);
        StringBuilder writeDataSqlTemplate = new StringBuilder();

        String jdbcUrl = originalConfig.getString(String.format("%s.%s",
                Key.CONNECTION, Key.JDBC_URL));

        String writeMode = originalConfig.getString(Key.WRITE_MODE, "INSERT");
        LOG.info("write mode is {}", writeMode);
        if (writeMode.toLowerCase().contains("replace")) {
            if (onConflictColumns == null || onConflictColumns.isEmpty()) {
                LOG.error("Replace mode must has onConflictColumn conf");
                return;
            }
            // for databend if you want to use replace mode, the writeMode should be:  "writeMode": "replace"
            writeDataSqlTemplate.append("REPLACE INTO  (")
                    .append(StringUtils.join(columns, ",")).append(") ").append(onConFlictDoString(onConflictColumns))
                    .append(" VALUES");

            LOG.info("Replace data [{}], which jdbcUrl like:[{}]", writeDataSqlTemplate, jdbcUrl);
            originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, writeDataSqlTemplate);
        } else {
            writeDataSqlTemplate.append("INSERT INTO %s");
            StringJoiner columnString = new StringJoiner(",");

            for (String column : columns) {
                columnString.add(column);
            }
            writeDataSqlTemplate.append(String.format("(%s)", columnString));
            writeDataSqlTemplate.append(" VALUES");

            LOG.info("Insert data [{}], which jdbcUrl like:[{}]", writeDataSqlTemplate, jdbcUrl);

            originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, writeDataSqlTemplate);
        }
    }

    public static String onConFlictDoString(List<String> conflictColumns) {
        return " ON " +
                "(" +
                StringUtils.join(conflictColumns, ",") + ") ";
    }
}