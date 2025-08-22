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

import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.core.base.Constant.LOAD_BALANCE_RESOURCE_MARK;

/**
 * Utility class for splitting database reader tasks across multiple parallel threads.
 * Handles both table-mode and query-mode splitting strategies with optional primary key splitting.
 */
public final class ReaderSplitUtil
{
    private ReaderSplitUtil()
    {
        // Private constructor to prevent instantiation
    }

    /**
     * Splits the original reader configuration into multiple slice configurations for parallel execution.
     * Supports two modes: table-mode (splits tables) and query-mode (uses pre-defined queries).
     *
     * @param dataBaseType The database type being read from
     * @param originalSliceConfig The original configuration to split
     * @param adviceNumber The recommended number of parallel tasks (channels)
     * @return List of split configurations, one for each parallel task
     */
    public static List<Configuration> doSplit(DataBaseType dataBaseType, Configuration originalSliceConfig, int adviceNumber)
    {
        boolean isTableMode = originalSliceConfig.getBool(Key.IS_TABLE_MODE);
        boolean isUserSpecifyEachTableSplitSize = originalSliceConfig.getInt(Key.EACH_TABLE_SPLIT_SIZE, -1) != -1;
        int eachTableShouldSplitNumber = -1;
        if (isTableMode) {
            // The adviceNumber is the number of channels, i.e., the number of concurrent tasks in Addax
            // eachTableShouldSplitNumber is the number of splits that a single table should be split into,
            // and the rounding up may not have a proportional relationship with adviceNumber
            if (!isUserSpecifyEachTableSplitSize) {
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

        // mark resource for load balancing
        sliceConfig.set(LOAD_BALANCE_RESOURCE_MARK, DataBaseType.parseIpFromJdbcUrl(jdbcUrl));

        sliceConfig.remove(Key.CONNECTION);
        int tableSplitNumber = eachTableShouldSplitNumber;
        Configuration tempSlice;

        if (isTableMode) {
            List<String> tables = connConf.getList(Key.TABLE, String.class);

            Validate.isTrue(null != tables && !tables.isEmpty(), "Tables list cannot be null or empty");

            String splitPk = originalSliceConfig.getString(Key.SPLIT_PK, null);
            boolean needSplitTable = tableSplitNumber > 0 && StringUtils.isNotBlank(splitPk);
            if (needSplitTable) {
                // For single table scenarios, increase split multiplier for better parallelism
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
                // No primary key splitting, create one slice per table
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
            // querySql mode - use pre-defined SQL queries
            List<String> sqls = connConf.getList(Key.QUERY_SQL, String.class);

            for (String querySql : sqls) {
                tempSlice = sliceConfig.clone();
                tempSlice.set(Key.QUERY_SQL, querySql);
                splitConfigs.add(tempSlice);
            }
        }

        return splitConfigs;
    }

    /**
     * Performs pre-check configuration splitting for validation purposes.
     * Generates the necessary SQL queries for table validation and split key analysis.
     *
     * @param originalSliceConfig The original configuration to prepare for pre-check
     * @return Configuration with generated SQL queries for validation
     */
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
            Validate.isTrue(null != tables && !tables.isEmpty(), "Failed to get tables from connection.");
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
            // Query SQL mode - use provided queries as-is
            List<String> sqls = connConf.getList(Key.QUERY_SQL, String.class);
            queries.addAll(sqls);
            connConf.set(Key.QUERY_SQL, queries);
            queryConfig.set(Key.CONNECTION, connConf);
        }

        return queryConfig;
    }

    /**
     * Calculates the optimal number of splits per table based on total channel count and table count.
     * Uses ceiling division to ensure all channels are utilized effectively.
     *
     * @param adviceNumber The total number of recommended parallel channels
     * @param tableNumber The number of tables to be processed
     * @return The number of splits each table should be divided into
     */
    private static int calculateEachTableShouldSplitNumber(int adviceNumber, int tableNumber)
    {
        double tempNum = 1.0 * adviceNumber / tableNumber;
        return (int) Math.ceil(tempNum);
    }
}
