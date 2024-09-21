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

package com.wgzhao.addax.plugin.reader.kudureader;

import com.google.common.collect.ImmutableMap;
import com.wgzhao.addax.common.element.BoolColumn;
import com.wgzhao.addax.common.element.BytesColumn;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.wgzhao.addax.common.base.Key.COLUMN;
import static com.wgzhao.addax.common.base.Key.WHERE;
import static com.wgzhao.addax.common.exception.CommonErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.exception.CommonErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.common.exception.CommonErrorCode.RUNTIME_ERROR;

/**
 * Kudu reader plugin
 */
public class KuduReader
        extends Reader
{

    public static class Job
            extends Reader.Job
    {
        private Configuration originalConfig = null;

        private String splitKey;

        private String lowerBound;

        private String upperBound;
        // match where clause such as age > 18
        private static final String PATTERN_FOR_WHERE = "^(\\w+)\\s+(=|>|>=|<|<=)\\s+(.*)$";
        private static final Pattern pattern = Pattern.compile(PATTERN_FOR_WHERE);
        private static final Map<String, KuduPredicate.ComparisonOp> KUDU_OPERATORS = ImmutableMap.of(
                "=", KuduPredicate.ComparisonOp.EQUAL,
                ">", KuduPredicate.ComparisonOp.GREATER,
                ">=", KuduPredicate.ComparisonOp.GREATER_EQUAL,
                "<", KuduPredicate.ComparisonOp.LESS,
                "<=", KuduPredicate.ComparisonOp.LESS_EQUAL
        );

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            List<Configuration> confList = new ArrayList<>();

            if ((splitKey != null) && (!"min".equals(lowerBound)) && (!"max".equals(upperBound))) {
                int iLowerBound = Integer.parseInt(this.lowerBound);
                int iUpperBound = Integer.parseInt(this.upperBound);
                int range = (iUpperBound - iLowerBound) + 1;
                int limit = (int) Math.ceil((double) range / (double) adviceNumber);
                int offset;
                for (int page = 0; page < adviceNumber; ++page) {
                    offset = page * limit;
                    Configuration conf = originalConfig.clone();
                    int possibleLowerBound = (iLowerBound + offset);
                    int possibleUpperBound = (iLowerBound + offset + limit - 1);
                    if (possibleLowerBound > iUpperBound) {
                        possibleLowerBound = 0;
                        possibleUpperBound = 0;
                    }
                    else {
                        possibleUpperBound = Math.min(possibleUpperBound, iUpperBound);
                    }
                    conf.set(KuduKey.SPLIT_LOWER_BOUND, String.valueOf(possibleLowerBound));
                    conf.set(KuduKey.SPLIT_UPPER_BOUND, String.valueOf(possibleUpperBound));
                    confList.add(conf);
                }
            }
            else {
                Configuration conf = originalConfig.clone();
                conf.set(KuduKey.SPLIT_LOWER_BOUND, "min");
                conf.set(KuduKey.SPLIT_UPPER_BOUND, "max");
                confList.add(conf);
            }

            return confList;
        }

        @Override
        public void prepare()
        {
            List<String> where = this.originalConfig.getList(WHERE, String.class);
            if (where != null && !where.isEmpty()) {
                List<Configuration> result = new ArrayList<>();
                Matcher matcher;

                for (String w : where) {
                    matcher = pattern.matcher(w);
                    while (matcher.find()) {
                        if (matcher.groupCount() == 3) {
                            if (KUDU_OPERATORS.containsKey(matcher.group(2))) {
                                Configuration conf = Configuration.from(
                                        String.format("{\"field\": \"%s\", \"op\": \"%s\", \"value\": \"%s\"}",
                                                matcher.group(1).trim(), matcher.group(2).trim(), matcher.group(3).trim()));
                                result.add(conf);
                            }
                            else {
                                throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                                        "operator '" + matcher.group(2) + "' is unsupported");
                            }
                        }
                        else {
                            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                                    "Illegal where clause: " + w);
                        }
                    }
                }

                if (!result.isEmpty()) {
                    this.originalConfig.set(WHERE, result);
                }
            }
        }

        @Override
        public void init()
        {
            originalConfig = super.getPluginJobConf();
            splitKey = originalConfig.getString(KuduKey.SPLIT_KEY);
            lowerBound = originalConfig.getString(KuduKey.LOWER_BOUND);
            upperBound = originalConfig.getString(KuduKey.UPPER_BOUND);
        }

        @Override
        public void destroy()
        {
            //
        }
    }

    public static class Task
            extends Reader.Task
    {

        private KuduClient kuduClient;

        private String tableName = null;

        private String splitKey;

        private String lowerBound;

        private String upperBound;

        private Long scanRequestTimeout;
        private List<String> columns;
        private boolean specifyColumn = false;
        List<KuduPredicate> customPredicate;
        private static final Map<String, KuduPredicate.ComparisonOp> KUDU_OPERATORS = ImmutableMap.of(
                "=", KuduPredicate.ComparisonOp.EQUAL,
                ">", KuduPredicate.ComparisonOp.GREATER,
                ">=", KuduPredicate.ComparisonOp.GREATER_EQUAL,
                "<", KuduPredicate.ComparisonOp.LESS,
                "<=", KuduPredicate.ComparisonOp.LESS_EQUAL
        );

        List<Configuration> where;

        @Override
        public void startRead(RecordSender recordSender)
        {
            KuduTable kuduTable;
            try {
                kuduTable = kuduClient.openTable(tableName);
            }
            catch (KuduException ex) {
                throw AddaxException.asAddaxException(
                        RUNTIME_ERROR,
                        ex.getMessage()
                );
            }

            Schema schema = kuduTable.getSchema();

            KuduScanner.KuduScannerBuilder kuduScannerBuilder = kuduClient.newScannerBuilder(kuduTable);
            if (scanRequestTimeout != null) {
                kuduScannerBuilder.scanRequestTimeout(scanRequestTimeout);
            }
            KuduScanner kuduScanner;

            if ((splitKey != null) && (!"min".equals(lowerBound)) && (!"max".equals(upperBound))) {
                KuduPredicate lowerBoundPredicate = KuduPredicate.newComparisonPredicate(
                        schema.getColumn(splitKey),
                        KuduPredicate.ComparisonOp.GREATER_EQUAL,
                        Integer.parseInt(lowerBound)
                );
                KuduPredicate upperBoundPredicate = KuduPredicate.newComparisonPredicate(
                        schema.getColumn(splitKey),
                        KuduPredicate.ComparisonOp.LESS_EQUAL,
                        Integer.parseInt(upperBound)
                );
                kuduScannerBuilder
                        .addPredicate(lowerBoundPredicate)
                        .addPredicate(upperBoundPredicate);
                if (specifyColumn) {
                    // judge specific column exists or not

                    for (String column : columns) {
                        if (!schema.hasColumn(column)) {
                            throw AddaxException.asAddaxException(
                                    ILLEGAL_VALUE,
                                    "column '" + column + "' does not exists in the table '" + tableName + "'"
                            );
                        }
                    }
                    kuduScannerBuilder.setProjectedColumnNames(columns);
                }
            }

            if (!where.isEmpty()) {
                List<KuduPredicate> customPredicate = processWhere(where, schema);
                for (KuduPredicate p : customPredicate) {
                    kuduScannerBuilder.addPredicate(p);
                }
            }

            kuduScanner = kuduScannerBuilder.build();

            List<ColumnSchema> columnSchemas = kuduScanner.getProjectionSchema().getColumns();

            while (kuduScanner.hasMoreRows()) {
                RowResultIterator rows;
                try {
                    rows = kuduScanner.nextRows();
                }
                catch (KuduException ex) {
                    throw AddaxException.asAddaxException(
                            RUNTIME_ERROR,
                            ex.getMessage()
                    );
                }
                while (rows.hasNext()) {
                    RowResult result = rows.next();

                    Record record = recordSender.createRecord();

                    boolean isDirtyRecord = false;

                    for (ColumnSchema columnSchema : columnSchemas) {
                        if (result.isNull(columnSchema.getName())) {
                            record.addColumn(new StringColumn());
                            continue;
                        }

                        Type columnType = columnSchema.getType();
                        switch (columnType) {
                            case INT8:
                                record.addColumn(new LongColumn(Long.valueOf(result.getByte(columnSchema.getName()))));
                                break;
                            case INT16:
                                record.addColumn(new LongColumn(Long.valueOf(result.getShort(columnSchema.getName()))));
                                break;
                            case INT32:
                                record.addColumn(new LongColumn(Long.valueOf(result.getInt(columnSchema.getName()))));
                                break;
                            case INT64:
                                record.addColumn(new LongColumn(result.getLong(columnSchema.getName())));
                                break;
                            case BINARY:
                                record.addColumn(new BytesColumn(result.getString(columnSchema.getName()).getBytes(StandardCharsets.UTF_8)));
                                break;
                            case STRING:
                                record.addColumn(new StringColumn(result.getString(columnSchema.getName())));
                                break;
                            case BOOL:
                                record.addColumn(new BoolColumn(result.getBoolean(columnSchema.getName())));
                                break;
                            case FLOAT:
                                record.addColumn(new DoubleColumn(result.getFloat(columnSchema.getName())));
                                break;
                            case DOUBLE:
                                record.addColumn(new DoubleColumn(result.getDouble(columnSchema.getName())));
                                break;
                            case UNIXTIME_MICROS:
                                record.addColumn(new DateColumn(result.getTimestamp(columnSchema.getName())));
                                break;
                            case DECIMAL:
                                record.addColumn(new DoubleColumn(result.getDecimal(columnSchema.getName())));
                                break;
                            default:
                                isDirtyRecord = true;
                                getTaskPluginCollector().collectDirtyRecord(
                                        record,
                                        "Invalid kudu data type: " + columnType.getName()
                                );
                                break;
                        }
                        if (isDirtyRecord) {
                            break;
                        }
                    }
                    if (!isDirtyRecord) {
                        recordSender.sendToWriter(record);
                    }
                }
            }

            try {
                kuduScanner.close();
            }
            catch (KuduException ex) {
                throw AddaxException.asAddaxException(
                        RUNTIME_ERROR,
                        ex.getMessage()
                );
            }
        }

        @Override
        public void init()
        {
            Configuration readerSliceConfig = super.getPluginJobConf();
            String masterAddresses = readerSliceConfig.getString(KuduKey.KUDU_MASTER_ADDRESSES);
            tableName = readerSliceConfig.getString(KuduKey.KUDU_TABlE_NAME);
            long socketReadTimeoutMs = readerSliceConfig.getLong(KuduKey.SOCKET_READ_TIMEOUT, 10) * 1000L;
            scanRequestTimeout = readerSliceConfig.getLong(KuduKey.SCAN_REQUEST_TIMEOUT, 20L) * 1000L;
            KuduClient.KuduClientBuilder kuduClientBuilder = (new KuduClient.KuduClientBuilder(masterAddresses));
            kuduClientBuilder.defaultOperationTimeoutMs(socketReadTimeoutMs);

            kuduClient = kuduClientBuilder.build();
            lowerBound = readerSliceConfig.getString(KuduKey.SPLIT_LOWER_BOUND);
            upperBound = readerSliceConfig.getString(KuduKey.SPLIT_UPPER_BOUND);
            splitKey = readerSliceConfig.getString(KuduKey.SPLIT_KEY);
            columns = readerSliceConfig.getList(COLUMN, String.class);
            if (!columns.isEmpty()) {
                specifyColumn = columns.size() != 1 || (!"*".equals(columns.get(0)) && !"\"*\"".equals(columns.get(0)));
            }
            where = readerSliceConfig.getListConfiguration(WHERE);
//            processWhere(where);
        }

        private void processWhere(List<Configuration> where)
        {
            String field;
            KuduPredicate.ComparisonOp op;
            KuduPredicate predicate;
            for (Configuration conf : where) {
                field = conf.getString("field");
                op = KUDU_OPERATORS.get(conf.getString("op"));
                ColumnSchema column;
                String value = conf.getString("value");
                if (value.startsWith("'")) {
                    column = new ColumnSchema.ColumnSchemaBuilder(field, Type.VARCHAR).build();
                    predicate = KuduPredicate.newComparisonPredicate(column, op, value);
                }
                else if (value.indexOf('.') > 0) {
                    column = new ColumnSchema.ColumnSchemaBuilder(field, Type.DOUBLE).build();
                    predicate = KuduPredicate.newComparisonPredicate(column, op, Double.valueOf(value));
                }
                else {
                    column = new ColumnSchema.ColumnSchemaBuilder(field, Type.INT32).build();
                    predicate = KuduPredicate.newComparisonPredicate(column, op, Long.parseLong(value));
                }
                this.customPredicate.add(predicate);
            }
        }

        /**
         * convert sql-format where to kudu {@link KuduPredicate} format
         * "age &gt; 1" as assumed where clause , it will be convert into
         * <pre>
         *      KuduPredicate.newComparisonPredicate("age", KuduPredicate.ComparisonOp.GREATER, 1);
         * </pre>
         *
         * @param where List of configuration, each element like <code>{"field":"age", "op": "&gt;", "value": 1}</code>
         * @param schema kudu schema
         * @return list of {@link KuduPredicate}
         */
        private List<KuduPredicate> processWhere(List<Configuration> where, Schema schema)
        {
            List<KuduPredicate> customPredicate = new ArrayList<>();
            String field;
            KuduPredicate.ComparisonOp op;
            KuduPredicate predicate;
            for (Configuration conf : where) {
                field = conf.getString("field");
                op = KUDU_OPERATORS.get(conf.getString("op"));
                if (!schema.hasColumn(field)) {
                    throw AddaxException.asAddaxException(
                            ILLEGAL_VALUE,
                            "column '" + field + "' in where clause does not exists in the table '" + tableName + "'"
                    );
                }
                ColumnSchema column = schema.getColumn(field);
                String value = conf.getString("value");

                switch (column.getType()) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                        predicate = KuduPredicate.newComparisonPredicate(column, op, Long.parseLong(value));
                        break;
                    case BOOL:
                        predicate = KuduPredicate.newComparisonPredicate(column, op, Boolean.valueOf(value));
                        break;
                    case STRING:
                    case VARCHAR:
                        predicate = KuduPredicate.newComparisonPredicate(column, op, value);
                        break;
                    case DATE:
                        predicate = KuduPredicate.newComparisonPredicate(column, op, Date.valueOf(value));
                        break;
                    case FLOAT:
                        predicate = KuduPredicate.newComparisonPredicate(column, op, Float.valueOf(value));
                        break;
                    case DOUBLE:
                        predicate = KuduPredicate.newComparisonPredicate(column, op, Double.valueOf(value));
                        break;
                    case DECIMAL:
                        predicate = KuduPredicate.newComparisonPredicate(column, op, new BigDecimal(value));
                        break;
                    case BINARY:
                        predicate = KuduPredicate.newComparisonPredicate(column, op, value.getBytes(StandardCharsets.UTF_8));
                        break;
                    case UNIXTIME_MICROS:
                        predicate = KuduPredicate.newComparisonPredicate(column, op, Timestamp.valueOf(value));
                        break;
                    default:
                        throw new IllegalStateException("Unexpected type: " + column.getType());
                }
                customPredicate.add(predicate);
            }
            return customPredicate;
        }

        @Override
        public void destroy()
        {
            try {
                kuduClient.close();
            }
            catch (KuduException ex) {
                throw AddaxException.asAddaxException(
                        RUNTIME_ERROR,
                        ex.getMessage()
                );
            }
        }
    }
}
