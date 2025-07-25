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

package com.wgzhao.addax.plugin.writer.paimonwriter;

import com.alibaba.fastjson2.JSON;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.*;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.core.base.Key.KERBEROS_KEYTAB_FILE_PATH;
import static com.wgzhao.addax.core.base.Key.KERBEROS_PRINCIPAL;

public class PaimonWriter
        extends Writer
{
    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration conf = null;
        private BatchWriteBuilder writeBuilder = null;

        @Override
        public void init()
        {
            this.conf = this.getPluginJobConf();

            Options options = PaimonHelper.getOptions(this.conf);
            CatalogContext context = PaimonHelper.getCatalogContext(options);

            if ("kerberos".equals(options.get("hadoop.security.authentication"))) {
                String kerberosKeytabFilePath = options.get(KERBEROS_KEYTAB_FILE_PATH);
                String kerberosPrincipal = options.get(KERBEROS_PRINCIPAL);
                try {
                    PaimonHelper.kerberosAuthentication(context.hadoopConf(), kerberosPrincipal, kerberosKeytabFilePath);
                    LOG.info("kerberos Authentication success");

                    FileSystem fs = FileSystem.get(context.hadoopConf());
                    fs.getStatus().getCapacity();
                }
                catch (Exception e) {
                    LOG.error("kerberos Authentication error", e);
                    throw new RuntimeException(e);
                }
            }
            try (Catalog catalog = CatalogFactory.createCatalog(context)) {

                String dbName = this.conf.getString("dbName");
                String tableName = this.conf.getString("tableName");
                Identifier identifier = Identifier.create(dbName, tableName);

                Table table = catalog.getTable(identifier);

                writeBuilder = table.newBatchWriteBuilder();
            }
            catch (Exception e) {
                LOG.error("init paimon error", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            List<Configuration> configurations = new ArrayList<>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(conf);
            }
            return configurations;
        }

        @Override
        public void prepare()
        {
            String writeMode = this.conf.getString("writeMode");
            if ("truncate".equalsIgnoreCase(writeMode)) {
                if (writeBuilder != null) {
                    LOG.info("You specify truncate writeMode, begin to clean history data.");
                    BatchTableWrite write = writeBuilder.withOverwrite().newWrite();
                    BatchTableCommit commit = writeBuilder.newCommit();
                    commit.commit(new ArrayList<>());
                    try {
                        write.close();
                    }
                    catch (Exception e) {
                        LOG.error("close paimon write error", e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        @Override
        public void destroy()
        {

        }
    }

    public static class Task
            extends Writer.Task
    {

        private static final Logger log = LoggerFactory.getLogger(Task.class);
        private BatchWriteBuilder writeBuilder = null;
        private Integer batchSize = 1000;
        private List<DataField> columnList = new ArrayList<>();
        private List<DataType> typeList = new ArrayList<>();
        private boolean isDynamicBucketMode;
        private int bucketNum = 0;

        @Override
        public void startWrite(RecordReceiver recordReceiver)
        {

            List<Record> writerBuffer = new ArrayList<>(this.batchSize);
            Record record;
            long total = 0;
            while ((record = recordReceiver.getFromReader()) != null) {
                writerBuffer.add(record);
                if (writerBuffer.size() >= this.batchSize) {
                    total += doBatchInsert(writerBuffer);
                    writerBuffer.clear();
                }
            }

            if (!writerBuffer.isEmpty()) {
                total += doBatchInsert(writerBuffer);
                writerBuffer.clear();
            }

            String msg = String.format("task end, write size :%d", total);
            getTaskPluginCollector().collectMessage("writeSize", String.valueOf(total));
            log.info(msg);
        }

        @Override
        public void init()
        {
            Configuration conf = super.getPluginJobConf();

            batchSize = conf.getInt("batchSize", 1000);

            Options options = PaimonHelper.getOptions(conf);
            CatalogContext context = PaimonHelper.getCatalogContext(options);

            if ("kerberos".equals(options.get("hadoop.security.authentication"))) {
                String kerberosKeytabFilePath = options.get(KERBEROS_KEYTAB_FILE_PATH);
                String kerberosPrincipal = options.get(KERBEROS_PRINCIPAL);
                try {
                    PaimonHelper.kerberosAuthentication(context.hadoopConf(), kerberosPrincipal, kerberosKeytabFilePath);
                    log.info("kerberos Authentication success");
                }
                catch (Exception e) {
                    log.error("kerberos Authentication error", e);
                    throw new RuntimeException(e);
                }
            }

            try (Catalog catalog = CatalogFactory.createCatalog(context)) {

                String dbName = conf.getString("dbName");
                String tableName = conf.getString("tableName");
                Identifier identifier = Identifier.create(dbName, tableName);

                Table table = catalog.getTable(identifier);

                // check the table has dynamic bucket
                Map<String, String> tableOptions = table.options();
                String bucketMode = tableOptions.getOrDefault("bucket-mode", "dynamic");
                this.isDynamicBucketMode = "dynamic".equalsIgnoreCase(bucketMode);
                String bucketNumStr = tableOptions.getOrDefault("bucket", "32");
                if (bucketNumStr != null && !bucketNumStr.isEmpty()) {
                    try {
                        this.bucketNum = Integer.parseInt(bucketNumStr);
                    } catch (NumberFormatException e) {
                        log.warn("Can not parse the bucket number: {}", bucketNumStr);
                    }
                }

                columnList = table.rowType().getFields();
                typeList = table.rowType().getFieldTypes();
                writeBuilder = table.newBatchWriteBuilder().withOverwrite();
            }
            catch (Exception e) {
                log.error("init paimon error", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public void destroy()
        {

        }

        private long doBatchInsert(final List<Record> writerBuffer)
        {
            BatchTableWrite write = writeBuilder.newWrite();
            GenericRow data;
            for (Record record : writerBuffer) {
                data = new GenericRow(columnList.size());
                StringBuilder id = new StringBuilder();
                for (int i = 0; i < record.getColumnNumber(); i++) {
                    Column column = record.getColumn(i);
                    if (column == null) {
                        continue;
                    }
                    if (i >= columnList.size()) {
                        throw new RuntimeException("columnList size is " + columnList.size() + ", but record column number is " + record.getColumnNumber());
                    }
                    String columnName = columnList.get(i).name();
                    DataType columnType = typeList.get(i);
                    if (columnType.getTypeRoot().equals(DataTypeRoot.ARRAY)) {
                        if (null == column.asString()) {
                            data.setField(i, null);
                        }
                        else {
                            String[] dataList = column.asString().split(",");
                            data.setField(i, new GenericArray(dataList));
                        }
                    }
                    else {
                        switch (columnType.getTypeRoot()) {

                            case DATE:
                            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                            case TIMESTAMP_WITHOUT_TIME_ZONE:
                                try {
                                    if (column.asLong() != null) {
                                        data.setField(i, Timestamp.fromSQLTimestamp(column.asTimestamp()));
                                    }
                                    else {
                                        data.setField(i, null);
                                    }
                                }
                                catch (Exception e) {
                                    getTaskPluginCollector().collectDirtyRecord(record, String.format("时间类型解析失败 [%s:%s] exception: %s", columnName, column.toString(), e));
                                }
                                break;
                            case CHAR:
                            case VARCHAR:
                                data.setField(i, BinaryString.fromString(column.asString()));
                                break;
                            case BOOLEAN:
                                data.setField(i, column.asBoolean());
                                break;
                            case VARBINARY:
                            case BINARY:
                                data.setField(i, column.asBytes());
                                break;
                            case BIGINT:
                                data.setField(i, column.asLong());
                                break;
                            case INTEGER:
                            case SMALLINT:
                            case TINYINT:
                                data.setField(i, column.asBigInteger() == null ? null : column.asBigInteger().intValue());
                                break;
                            case FLOAT:
                            case DOUBLE:

                                data.setField(i, column.asDouble());
                                break;
                            case DECIMAL:
                                if (column.asBigDecimal() != null) {
                                    data.setField(i, Decimal.fromBigDecimal(column.asBigDecimal(), ((DecimalType) columnType).getPrecision(), ((DecimalType) columnType).getScale()));
                                }
                                else {
                                    data.setField(i, null);
                                }
                                break;
                            case MAP:
                                try {
                                    data.setField(i, new GenericMap(JSON.parseObject(column.asString(), Map.class)));
                                }
                                catch (Exception e) {
                                    getTaskPluginCollector().collectDirtyRecord(record, "failed to parse the '" + column.asString() + "' to map: " + e);
                                }
                                break;
                            default:
                                getTaskPluginCollector().collectDirtyRecord(record, "The column type is not supported: " + columnType.getTypeRoot());
                        }
                    }
                }

                try {
                    if (isDynamicBucketMode) {
                        int bucketId = Math.abs(data.hashCode() % bucketNum);
                        write.write(data, bucketId);
                    } else {
                        write.write(data);
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            List<CommitMessage> messages = null;
            try {
                messages = write.prepareCommit();
                BatchTableCommit commit = writeBuilder.newCommit();
                commit.commit(messages);

                write.close();

                return messages.size();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
