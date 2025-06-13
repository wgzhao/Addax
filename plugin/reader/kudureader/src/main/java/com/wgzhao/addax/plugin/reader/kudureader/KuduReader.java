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

import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.BytesColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.element.TimestampColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.wgzhao.addax.core.base.Constant.DEFAULT_DATE_FORMAT;
import static com.wgzhao.addax.core.base.Key.COLUMN;
import static com.wgzhao.addax.core.base.Key.HAVE_KERBEROS;
import static com.wgzhao.addax.core.base.Key.KERBEROS_KEYTAB_FILE_PATH;
import static com.wgzhao.addax.core.base.Key.KERBEROS_PRINCIPAL;
import static com.wgzhao.addax.core.base.Key.WHERE;
import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;
import static com.wgzhao.addax.plugin.reader.kudureader.KuduKey.KUDU_OPERATORS;

/**
 * Kudu reader plugin
 */
public class KuduReader
        extends Reader
{

    public static class Job
            extends Reader.Job
    {
        private record WhereClause(String field, String operator, String value) {}

        private Configuration originalConfig = null;
        private String splitKey;
        private String lowerBound;
        private String upperBound;
        // match where clause such as age > 18
        private static final String PATTERN_FOR_WHERE = "^\\s*(\\w+)\\s*(>=|<|<=|!=|=|>)\\s*(.*)$";
        private static final Pattern pattern = Pattern.compile(PATTERN_FOR_WHERE);

        @Override
        public void init()
        {
            originalConfig = super.getPluginJobConf();
            splitKey = originalConfig.getString(KuduKey.SPLIT_PK);
            lowerBound = originalConfig.getString(KuduKey.LOWER_BOUND, "min");
            upperBound = originalConfig.getString(KuduKey.UPPER_BOUND, "max");
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            List<Configuration> confList = new ArrayList<>();

            if (shouldSplit()) {
                int iLowerBound = Integer.parseInt(this.lowerBound);
                int iUpperBound = Integer.parseInt(this.upperBound);
                int range = (iUpperBound - iLowerBound) + 1;
                int limit = (int) Math.ceil((double) range / (double) adviceNumber);

                for (int page = 0; page < adviceNumber; ++page) {
                    int offset = page * limit;
                    Configuration conf = this.originalConfig.clone();
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
                Configuration conf = this.originalConfig.clone();
                conf.set(KuduKey.SPLIT_LOWER_BOUND, "min");
                conf.set(KuduKey.SPLIT_UPPER_BOUND, "max");
                confList.add(conf);
            }

            return confList;
        }

        private boolean shouldSplit()
        {
            return (this.splitKey != null) && (!"min".equals(this.lowerBound)) && (!"max".equals(this.upperBound));
        }

        @Override
        public void prepare()
        {
            List<String> where = this.originalConfig.getList(WHERE, String.class);
            if (where != null && !where.isEmpty()) {
                List<Configuration> result = new ArrayList<>();

                for (String w : where) {
                    WhereClause whereClause = parseWhereClause(w);
                    if (KUDU_OPERATORS.containsKey(whereClause.operator())) {
                        Configuration conf = Configuration.from("""
                                {
                                    "field": "%s",
                                    "op": "%s",
                                    "value": "%s"
                                }""".formatted(whereClause.field(), whereClause.operator(), whereClause.value()));
                        result.add(conf);
                    }
                    else {
                        throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                                "Operator '%s' is unsupported".formatted(whereClause.operator()));
                    }
                }

                if (!result.isEmpty()) {
                    this.originalConfig.set(WHERE, result);
                }
            }
        }

        private WhereClause parseWhereClause(String whereClause)
        {
            Matcher matcher = pattern.matcher(whereClause);
            if (matcher.find() && matcher.groupCount() == 3) {
                return new WhereClause(
                        matcher.group(1).trim(),
                        matcher.group(2).trim(),
                        matcher.group(3).trim()
                );
            }
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "Illegal where clause: " + whereClause);
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
        private String tableName;
        private String splitKey;
        private String lowerBound;
        private String upperBound;
        private Long scanRequestTimeout;
        private List<String> columns;
        private boolean specifyColumn = false;
        private List<KuduPredicate> customPredicate;
        private List<Configuration> where;

        @Override
        public void init()
        {
            Configuration readerSliceConfig = super.getPluginJobConf();
            String masterAddresses = readerSliceConfig.getString(KuduKey.KUDU_MASTER_ADDRESSES);
            this.tableName = readerSliceConfig.getString(KuduKey.TABLE);
            long socketReadTimeoutMs = readerSliceConfig.getLong(KuduKey.SOCKET_READ_TIMEOUT, 10) * 1000L;
            this.scanRequestTimeout = readerSliceConfig.getLong(KuduKey.SCAN_REQUEST_TIMEOUT, 20L) * 1000L;

            boolean haveKerberos = readerSliceConfig.getBool(HAVE_KERBEROS, false);

            if (!haveKerberos) {
                this.kuduClient = new KuduClient.KuduClientBuilder(masterAddresses)
                        .defaultOperationTimeoutMs(socketReadTimeoutMs)
                        .build();
            }
            else {
                org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
                UserGroupInformation.setConfiguration(configuration);

                String kerberosKeytabFilePath = readerSliceConfig.getString(KERBEROS_KEYTAB_FILE_PATH);
                String kerberosPrincipal = readerSliceConfig.getString(KERBEROS_PRINCIPAL);
                try {
                    UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
                    this.kuduClient = UserGroupInformation.getLoginUser().doAs(
                            (PrivilegedExceptionAction<KuduClient>) () ->
                                    new KuduClient.KuduClientBuilder(masterAddresses).defaultOperationTimeoutMs(socketReadTimeoutMs).build());
                }
                catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            this.lowerBound = readerSliceConfig.getString(KuduKey.SPLIT_LOWER_BOUND);
            this.upperBound = readerSliceConfig.getString(KuduKey.SPLIT_UPPER_BOUND);
            this.splitKey = readerSliceConfig.getString(KuduKey.SPLIT_PK);
            this.columns = readerSliceConfig.getList(COLUMN, String.class);

            if (!this.columns.isEmpty()) {
                this.specifyColumn = !isWildcardColumn(this.columns.get(0));
            }

            this.where = readerSliceConfig.getListConfiguration(WHERE);
        }

        private boolean isWildcardColumn(String column)
        {
            return "*".equals(column) || "\"*\"".equals(column);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            KuduTable kuduTable;
            try {
                kuduTable = this.kuduClient.openTable(this.tableName);
                Schema schema = kuduTable.getSchema();
                KuduScanner scanner = buildScanner(kuduTable, schema);
                processRows(scanner, recordSender);
            }
            catch (KuduException ex) {
                throw AddaxException.asAddaxException(RUNTIME_ERROR, ex.getMessage());
            }
        }

        private KuduScanner buildScanner(KuduTable kuduTable, Schema schema)
        {
            KuduScanner.KuduScannerBuilder builder = this.kuduClient.newScannerBuilder(kuduTable);

            if (this.scanRequestTimeout != null) {
                builder.scanRequestTimeout(this.scanRequestTimeout);
            }

            if (shouldApplySplitPredicates()) {
                applySplitPredicates(builder, schema);
            }

            if (!this.where.isEmpty()) {
                List<KuduPredicate> predicates = processWhere(this.where, schema);
                predicates.forEach(builder::addPredicate);
            }

            return builder.build();
        }

        private boolean shouldApplySplitPredicates()
        {
            return (this.splitKey != null) && (!"min".equals(this.lowerBound)) && (!"max".equals(this.upperBound));
        }

        private void applySplitPredicates(KuduScanner.KuduScannerBuilder builder, Schema schema)
        {
            builder.addPredicate(KuduPredicate.newComparisonPredicate(
                            schema.getColumn(this.splitKey),
                            KuduPredicate.ComparisonOp.GREATER_EQUAL,
                            Integer.parseInt(this.lowerBound)))
                    .addPredicate(KuduPredicate.newComparisonPredicate(
                            schema.getColumn(this.splitKey),
                            KuduPredicate.ComparisonOp.LESS_EQUAL,
                            Integer.parseInt(this.upperBound)));

            if (this.specifyColumn) {
                validateAndSetProjectedColumns(builder, schema);
            }
        }

        private void validateAndSetProjectedColumns(KuduScanner.KuduScannerBuilder builder, Schema schema)
        {
            for (String column : this.columns) {
                if (!schema.hasColumn(column)) {
                    throw AddaxException.asAddaxException(
                            ILLEGAL_VALUE,
                            "Column '%s' does not exist in table '%s'".formatted(column, this.tableName)
                    );
                }
            }
            builder.setProjectedColumnNames(this.columns);
        }

        private void processRows(KuduScanner scanner, RecordSender recordSender)
                throws KuduException
        {
            List<ColumnSchema> columnSchemas = scanner.getProjectionSchema().getColumns();

            while (scanner.hasMoreRows()) {
                RowResultIterator rows = scanner.nextRows();
                while (rows.hasNext()) {
                    processRow(rows.next(), columnSchemas, recordSender);
                }
            }
        }

        private void processRow(RowResult result, List<ColumnSchema> columnSchemas, RecordSender recordSender)
        {
            Record record = recordSender.createRecord();
            boolean isDirtyRecord = false;

            for (ColumnSchema columnSchema : columnSchemas) {
                if (result.isNull(columnSchema.getName())) {
                    record.addColumn(new StringColumn());
                    continue;
                }

                try {
                    addColumnToRecord(record, result, columnSchema);
                }
                catch (Exception e) {
                    isDirtyRecord = true;
                    getTaskPluginCollector().collectDirtyRecord(record,
                            "Invalid kudu data type: " + columnSchema.getType().getName());
                    break;
                }
            }

            if (!isDirtyRecord) {
                recordSender.sendToWriter(record);
            }
        }

        private void addColumnToRecord(Record record, RowResult result, ColumnSchema columnSchema)
        {
            String columnName = columnSchema.getName();
            Type columnType = columnSchema.getType();

            switch (columnType) {
                case INT8 -> record.addColumn(new LongColumn(Long.valueOf(result.getByte(columnName))));
                case INT16 -> record.addColumn(new LongColumn(Long.valueOf(result.getShort(columnName))));
                case INT32 -> record.addColumn(new LongColumn(result.getInt(columnName)));
                case INT64 -> record.addColumn(new LongColumn(result.getLong(columnName)));
                case BINARY -> record.addColumn(new BytesColumn(result.getString(columnName).getBytes(StandardCharsets.UTF_8)));
                case STRING -> record.addColumn(new StringColumn(result.getString(columnName)));
                case BOOL -> record.addColumn(new BoolColumn(result.getBoolean(columnName)));
                case FLOAT -> record.addColumn(new DoubleColumn(result.getFloat(columnName)));
                case DOUBLE -> record.addColumn(new DoubleColumn(result.getDouble(columnName)));
                case UNIXTIME_MICROS -> {
                    int offsetSecs = ZonedDateTime.now(ZoneId.systemDefault()).getOffset().getTotalSeconds();
                    long ts = result.getLong(columnName) / 1_000L - offsetSecs * 1_000L;
                    record.addColumn(new TimestampColumn(ts));
                }
                case DECIMAL -> record.addColumn(new DoubleColumn(result.getDecimal(columnName)));
                default -> throw new IllegalStateException("Unexpected column type: " + columnType);
            }
        }

        private List<KuduPredicate> processWhere(List<Configuration> where, Schema schema)
        {
            List<KuduPredicate> predicates = new ArrayList<>();

            for (Configuration conf : where) {
                String field = conf.getString("field");
                if (!schema.hasColumn(field)) {
                    throw AddaxException.asAddaxException(
                            ILLEGAL_VALUE,
                            "Column '%s' in where clause does not exist in table '%s'".formatted(field, this.tableName)
                    );
                }

                KuduPredicate.ComparisonOp op = KUDU_OPERATORS.get(conf.getString("op"));
                ColumnSchema column = schema.getColumn(field);
                String value = conf.getString("value");

                predicates.add(createPredicate(column, op, value));
            }

            return predicates;
        }

        private KuduPredicate createPredicate(ColumnSchema column, KuduPredicate.ComparisonOp op, String value)
        {
            return switch (column.getType()) {
                case INT8, INT16, INT32, INT64 -> KuduPredicate.newComparisonPredicate(column, op, Long.parseLong(value));
                case BOOL -> KuduPredicate.newComparisonPredicate(column, op, Boolean.parseBoolean(value));
                case STRING, VARCHAR -> KuduPredicate.newComparisonPredicate(column, op, value);
                case DATE -> KuduPredicate.newComparisonPredicate(column, op, Date.valueOf(value));
                case FLOAT -> KuduPredicate.newComparisonPredicate(column, op, Float.parseFloat(value));
                case DOUBLE -> KuduPredicate.newComparisonPredicate(column, op, Double.parseDouble(value));
                case DECIMAL -> KuduPredicate.newComparisonPredicate(column, op, new BigDecimal(value));
                case BINARY -> KuduPredicate.newComparisonPredicate(column, op, value.getBytes(StandardCharsets.UTF_8));
                case UNIXTIME_MICROS -> {
                    SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
                    try {
                        java.util.Date date = sdf.parse(value);
                        int offsetSecs = ZonedDateTime.now(ZoneId.systemDefault()).getOffset().getTotalSeconds();
                        long ts = date.getTime() * 1_000L + offsetSecs * 1_000_000L;
                        yield KuduPredicate.newComparisonPredicate(column, op, ts);
                    }
                    catch (ParseException e) {
                        throw AddaxException.asAddaxException(CONFIG_ERROR, "Cannot parse date: " + value);
                    }
                }
                default -> throw new IllegalStateException("Unexpected type: " + column.getType());
            };
        }

        @Override
        public void destroy()
        {
            try {
                if (this.kuduClient != null) {
                    this.kuduClient.close();
                }
            }
            catch (KuduException ex) {
                throw AddaxException.asAddaxException(RUNTIME_ERROR, ex.getMessage());
            }
        }
    }
}
