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

import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.common.constant.CommonConstant;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.List;

public final class ReaderSplitUtil
{

    private ReaderSplitUtil() {}
    
    public static List<Configuration> doSplit(
            Configuration originalSliceConfig, int adviceNumber)
    {
        boolean isTableMode = originalSliceConfig.getBool(Key.IS_TABLE_MODE);
        boolean isUserSpecifyEachTableSplitSize = originalSliceConfig.getInt(Key.EACH_TABLE_SPLIT_SIZE, -1) != -1;
        int eachTableShouldSplittedNumber = -1;
        if (isTableMode) {
            // adviceNumber这里是channel数量大小, 即addax并发task数量
            // eachTableShouldSplittedNumber是单表应该切分的份数, 向上取整可能和adviceNumber没有比例关系了已经
            if (!isUserSpecifyEachTableSplitSize) {
                // adviceNumber这里是channel数量大小, 即addax并发task数量
                // eachTableShouldSplittedNumber是单表应该切分的份数, 向上取整可能和adviceNumber没有比例关系了已经
                eachTableShouldSplittedNumber = calculateEachTableShouldSplittedNumber(
                        adviceNumber, originalSliceConfig.getInt(Constant.TABLE_NUMBER_MARK));
            }
            else {
                eachTableShouldSplittedNumber = originalSliceConfig.getInt(Key.EACH_TABLE_SPLIT_SIZE, -1);
            }
        }

        String column = originalSliceConfig.getString(Key.COLUMN);
        String where = originalSliceConfig.getString(Key.WHERE, null);

        List<Object> conns = originalSliceConfig.getList(Key.CONNECTION, Object.class);

        List<Configuration> splittedConfigs = new ArrayList<>();

        for (Object conn : conns) {
            Configuration sliceConfig = originalSliceConfig.clone();

            Configuration connConf = Configuration.from(conn.toString());
            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            sliceConfig.set(Key.JDBC_URL, jdbcUrl);

            // 抽取 jdbcUrl 中的 ip/port 进行资源使用的打标，以提供给 core 做有意义的 shuffle 操作
            sliceConfig.set(CommonConstant.LOAD_BALANCE_RESOURCE_MARK, DataBaseType.parseIpFromJdbcUrl(jdbcUrl));

            sliceConfig.remove(Key.CONNECTION);
            int tableSplitNumber = eachTableShouldSplittedNumber;
            Configuration tempSlice;

            // 说明是配置的 table 方式
            if (isTableMode) {
                // 已在之前进行了扩展和`处理，可以直接使用
                List<String> tables = connConf.getList(Key.TABLE, String.class);

                Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");

                String splitPk = originalSliceConfig.getString(Key.SPLIT_PK, null);
                //最终切分份数不一定等于 eachTableShouldSplittedNumber
                boolean needSplitTable = tableSplitNumber > 1
                        && StringUtils.isNotBlank(splitPk);
                if (needSplitTable) {
                    if (tables.size() == 1 && !isUserSpecifyEachTableSplitSize) {
                        //原来:如果是单表的，主键切分num=num*2+1
                        // splitPk is null这类的情况的数据量本身就比真实数据量少很多, 和channel大小比率关系时，不建议考虑
                        //eachTableShouldSplittedNumber = eachTableShouldSplittedNumber * 2 + 1;// 不应该加1导致长尾

                        //考虑其他比率数字?(splitPk is null, 忽略此长尾)
                        tableSplitNumber = tableSplitNumber * 5;
                    }
                    // 尝试对每个表，切分为eachTableShouldSplittedNumber 份
                    for (String table : tables) {
                        tempSlice = sliceConfig.clone();
                        tempSlice.set(Key.TABLE, table);

                        List<Configuration> splittedSlices = SingleTableSplitUtil
                                .splitSingleTable(tempSlice, tableSplitNumber);

                        splittedConfigs.addAll(splittedSlices);
                    }
                }
                else {
                    for (String table : tables) {
                        tempSlice = sliceConfig.clone();
                        tempSlice.set(Key.TABLE, table);
                        String queryColumn = HintUtil.buildQueryColumn(table, column);
                        tempSlice.set(Key.QUERY_SQL, SingleTableSplitUtil.buildQuerySql(queryColumn, table, where));
                        splittedConfigs.add(tempSlice);
                    }
                }
            }
            else {
                // 说明是配置的 querySql 方式
                List<String> sqls = connConf.getList(Key.QUERY_SQL, String.class);

                for (String querySql : sqls) {
                    tempSlice = sliceConfig.clone();
                    tempSlice.set(Key.QUERY_SQL, querySql);
                    splittedConfigs.add(tempSlice);
                }
            }
        }

        return splittedConfigs;
    }

    public static Configuration doPreCheckSplit(Configuration originalSliceConfig)
    {
        Configuration queryConfig = originalSliceConfig.clone();
        boolean isTableMode = originalSliceConfig.getBool(Key.IS_TABLE_MODE);

        String splitPK = originalSliceConfig.getString(Key.SPLIT_PK);
        String column = originalSliceConfig.getString(Key.COLUMN);
        String where = originalSliceConfig.getString(Key.WHERE, null);

        List<Object> conns = queryConfig.getList(Key.CONNECTION, Object.class);

        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration connConf = Configuration.from(conns.get(i).toString());
            List<String> querys = new ArrayList<>();
            List<String> splitPkQuerys = new ArrayList<>();
            String connPath = String.format("connection[%d]", i);
            // 说明是配置的 table 方式
            if (isTableMode) {
                // 已在之前进行了扩展和`处理，可以直接使用
                List<String> tables = connConf.getList(Key.TABLE, String.class);
                Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");
                for (String table : tables) {
                    querys.add(SingleTableSplitUtil.buildQuerySql(column, table, where));
                    if (splitPK != null && !splitPK.isEmpty()) {
                        splitPkQuerys.add(SingleTableSplitUtil.genPKSql(splitPK.trim(), table, where));
                    }
                }
                if (!splitPkQuerys.isEmpty()) {
                    connConf.set(Key.SPLIT_PK_SQL, splitPkQuerys);
                }
                connConf.set(Key.QUERY_SQL, querys);
                queryConfig.set(connPath, connConf);
            }
            else {
                // 说明是配置的 querySql 方式
                List<String> sqls = connConf.getList(Key.QUERY_SQL,
                        String.class);
                querys.addAll(sqls);
                connConf.set(Key.QUERY_SQL, querys);
                queryConfig.set(connPath, connConf);
            }
        }
        return queryConfig;
    }

    private static int calculateEachTableShouldSplittedNumber(int adviceNumber,
            int tableNumber)
    {
        double tempNum = 1.0 * adviceNumber / tableNumber;

        return (int) Math.ceil(tempNum);
    }
}
