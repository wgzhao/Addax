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

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.base.Constant.LOAD_BALANCE_RESOURCE_MARK;

public final class ReaderSplitUtil
{

    private ReaderSplitUtil() {}

    public static List<Configuration> doSplit(DataBaseType dataBaseType, Configuration originalSliceConfig, int adviceNumber)
    {
        boolean isTableMode = originalSliceConfig.getBool(Key.IS_TABLE_MODE);
        boolean isUserSpecifyEachTableSplitSize = originalSliceConfig.getInt(Key.EACH_TABLE_SPLIT_SIZE, -1) != -1;
        int eachTableShouldSplitNumber = -1;
        if (isTableMode) {
            // the adviceNumber is the number of channel, i.e., the number of concurrent tasks in addax
            // eachTableShouldSplitNumber is the number of splits that a single table should be split into,
            // and the rounding up may not have a proportional relationship with adviceNumber
            if (!isUserSpecifyEachTableSplitSize) {
                // eachTableShouldSplitNumber = eachTableShouldSplitNumber * 2 + 1;
                eachTableShouldSplitNumber = calculateEachTableShouldSplitNumber(
                        adviceNumber, originalSliceConfig.getInt(Key.TABLE_NUMBER));
            }
            else {
                eachTableShouldSplitNumber = originalSliceConfig.getInt(Key.EACH_TABLE_SPLIT_SIZE, -1);
            }
        }

        String column = originalSliceConfig.getString(Key.COLUMN);
        String where = originalSliceConfig.getString(Key.WHERE, null);

        Configuration connConf = originalSliceConfig.getConfiguration(Key.CONNECTION);

        List<Configuration> splitConfigs = new ArrayList<>();

        Configuration sliceConfig = originalSliceConfig.clone();

        String jdbcUrl = connConf.getString(Key.JDBC_URL);
        sliceConfig.set(Key.JDBC_URL, jdbcUrl);

        // mark resource
        sliceConfig.set(LOAD_BALANCE_RESOURCE_MARK, DataBaseType.parseIpFromJdbcUrl(jdbcUrl));

        sliceConfig.remove(Key.CONNECTION);
        int tableSplitNumber = eachTableShouldSplitNumber;
        Configuration tempSlice;

        if (isTableMode) {
            List<String> tables = connConf.getList(Key.TABLE, String.class);

            Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");

            String splitPk = originalSliceConfig.getString(Key.SPLIT_PK, null);
            boolean needSplitTable = tableSplitNumber > 0 && StringUtils.isNotBlank(splitPk);
            if (needSplitTable) {
                if (tables.size() == 1 && !isUserSpecifyEachTableSplitSize) {
                    tableSplitNumber = tableSplitNumber * 5;
                }
                // try to split each table into eachTableShouldSplitNumber splits
                for (String table : tables) {
                    tempSlice = sliceConfig.clone();
                    tempSlice.set(Key.TABLE, table);

                    List<Configuration> splitSlices = SingleTableSplitUtil.splitSingleTable(dataBaseType, tempSlice, tableSplitNumber);

                    splitConfigs.addAll(splitSlices);
                }
            }
            else {
                for (String table : tables) {
                    tempSlice = sliceConfig.clone();
                    tempSlice.set(Key.TABLE, table);
                    String queryColumn = HintUtil.buildQueryColumn(table, column);
                    tempSlice.set(Key.QUERY_SQL, SingleTableSplitUtil.buildQuerySql(queryColumn, table, where));
                    splitConfigs.add(tempSlice);
                }
            }
        }
        else {
            // querySql mode
            List<String> sqls = connConf.getList(Key.QUERY_SQL, String.class);

            for (String querySql : sqls) {
                tempSlice = sliceConfig.clone();
                tempSlice.set(Key.QUERY_SQL, querySql);
                splitConfigs.add(tempSlice);
            }
        }

        return splitConfigs;
    }

    public static Configuration doPreCheckSplit(Configuration originalSliceConfig)
    {
        Configuration queryConfig = originalSliceConfig.clone();
        boolean isTableMode = originalSliceConfig.getBool(Key.IS_TABLE_MODE);

        String splitPK = originalSliceConfig.getString(Key.SPLIT_PK);
        String column = originalSliceConfig.getString(Key.COLUMN);
        String where = originalSliceConfig.getString(Key.WHERE, null);

        Configuration connConf = queryConfig.getConfiguration(Key.CONNECTION);

        List<String> queries = new ArrayList<>();
        List<String> splitPkQueries = new ArrayList<>();
        // table mode
        if (isTableMode) {
            List<String> tables = connConf.getList(Key.TABLE, String.class);
            Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");
            for (String table : tables) {
                queries.add(SingleTableSplitUtil.buildQuerySql(column, table, where));
                if (splitPK != null && !splitPK.isEmpty()) {
                    splitPkQueries.add(SingleTableSplitUtil.genPKSql(splitPK.trim(), table, where));
                }
            }
            if (!splitPkQueries.isEmpty()) {
                connConf.set(Key.SPLIT_PK_SQL, splitPkQueries);
            }
            connConf.set(Key.QUERY_SQL, queries);
            queryConfig.set(Key.CONNECTION, connConf);
        }
        else {
            List<String> sqls = connConf.getList(Key.QUERY_SQL, String.class);
            queries.addAll(sqls);
            connConf.set(Key.QUERY_SQL, queries);
            queryConfig.set(Key.CONNECTION, connConf);
        }

        return queryConfig;
    }

    private static int calculateEachTableShouldSplitNumber(int adviceNumber, int tableNumber)
    {
        double tempNum = 1.0 * adviceNumber / tableNumber;

        return (int) Math.ceil(tempNum);
    }
}
