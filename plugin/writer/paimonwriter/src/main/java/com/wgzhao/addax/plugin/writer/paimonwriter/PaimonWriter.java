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
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.wgzhao.addax.core.base.Key.KERBEROS_KEYTAB_FILE_PATH;
import static com.wgzhao.addax.core.base.Key.KERBEROS_PRINCIPAL;
import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;

/** Paimon Writer. */
public class PaimonWriter
        extends Writer
{
    /** Job. */
    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        /** Write modes this writer honours, everything else is refused instead of appended. */
        private static final Set<String> WRITE_MODES = Set.of("append", "insert", "truncate");
        private Configuration conf = null;
        private BatchWriteBuilder writeBuilder = null;
        private String writeMode = null;

        @Override
        public void init()
        {
            this.conf = this.getPluginJobConf();

            Options options = PaimonHelper.getOptions(this.conf);
            CatalogContext context = PaimonHelper.getCatalogContext(options);

            if (PaimonHelper.isKerberos(options)) {
                String kerberosKeytabFilePath = options.get(KERBEROS_KEYTAB_FILE_PATH);
                String kerberosPrincipal = options.get(KERBEROS_PRINCIPAL);
                try {
                    PaimonHelper.kerberosAuthentication(context.hadoopConf(), kerberosPrincipal, kerberosKeytabFilePath);
                    LOG.info("kerberos Authentication success");
                }
                catch (AddaxException e) {
                    throw e;
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
                PaimonHelper.validateBucketMode(table);
                this.writeMode = writeMode();

                writeBuilder = table.newBatchWriteBuilder();
            }
            catch (AddaxException e) {
                throw e;
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
            if ("truncate".equals(this.writeMode) && writeBuilder != null) {
                LOG.info("You specify truncate writeMode, begin to clean history data.");
                BatchTableCommit commit = writeBuilder.newCommit();
                try {
                    commit.truncateTable();
                }
                catch (Exception e) {
                    LOG.error("Failed to truncate table ", e);
                    throw new RuntimeException(e);
                }
            }
        }

        /** The writeMode the job asked for, checked against the ones this writer honours. */
        private String writeMode()
        {
            String writeMode = StringUtils.defaultIfBlank(this.conf.getString("writeMode"), "append").toLowerCase();
            if (!WRITE_MODES.contains(writeMode)) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, String.format(
                        "writeMode [%s] is not supported, use one of %s", writeMode, WRITE_MODES));
            }
            return writeMode;
        }

        @Override
        public void destroy()
        {

        }
    }

    /** Task. */
    public static class Task
            extends Writer.Task
    {

        private static final Logger log = LoggerFactory.getLogger(Task.class);
        /** Buffer one task may fill before it writes a data file, see {@link #boundedWriteBuffer}. */
        private static final String DEFAULT_WRITE_BUFFER_SIZE = "64 mb";
        private BatchWriteBuilder writeBuilder = null;
        private DataField[] columns = new DataField[0];
        private DataType[] types = new DataType[0];
        /** For reader column i the table column it feeds, or null when both are in the same order. */
        private int[] projection = null;

        @Override
        public void startWrite(RecordReceiver recordReceiver)
        {
            long total = 0;
            // a BatchTableWrite accepts exactly one commit, so a task writes through a single
            // writer and commits once: one snapshot per task instead of one per buffered batch
            BatchTableWrite write = writeBuilder.newWrite();
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    GenericRow data = convert(record);
                    if (data == null) {
                        // the record was collected as dirty, writing it would put a
                        // half-filled row into the table
                        continue;
                    }
                    // the bucket is left to Paimon, see PaimonHelper.validateBucketMode
                    write.write(data);
                    total++;
                }

                List<CommitMessage> messages = write.prepareCommit();
                BatchTableCommit commit = writeBuilder.newCommit();
                commit.commit(messages);

                log.info("task end, write size :{}, commit messages :{}", total, messages.size());
                getTaskPluginCollector().collectMessage("writeSize", String.valueOf(total));
            }
            catch (AddaxException e) {
                throw e;
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            finally {
                try {
                    write.close();
                }
                catch (Exception e) {
                    log.warn("failed to close the paimon writer", e);
                }
            }
        }

        @Override
        public void init()
        {
            Configuration conf = super.getPluginJobConf();

            Options options = PaimonHelper.getOptions(conf);
            CatalogContext context = PaimonHelper.getCatalogContext(options);

            if (PaimonHelper.isKerberos(options)) {
                String kerberosKeytabFilePath = options.get(KERBEROS_KEYTAB_FILE_PATH);
                String kerberosPrincipal = options.get(KERBEROS_PRINCIPAL);
                try {
                    PaimonHelper.kerberosAuthentication(context.hadoopConf(), kerberosPrincipal, kerberosKeytabFilePath);
                    log.info("kerberos Authentication success");
                }
                catch (AddaxException e) {
                    throw e;
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
                PaimonHelper.validateBucketMode(table);
                table = boundedWriteBuffer(table, conf);

                RowType rowType = table.rowType();
                columns = rowType.getFields().toArray(new DataField[0]);
                types = rowType.getFieldTypes().toArray(new DataType[0]);
                projection = projection(conf, tableName);

                writeBuilder = table.newBatchWriteBuilder();
            }
            catch (AddaxException e) {
                throw e;
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

        /**
         * Caps how much data one task buffers before it writes a data file.
         * <p>
         * A task keeps a single writer for its whole run, so it also holds that writer's
         * buffer. Paimon defaults it to 256 mb per write, which is what all channels
         * together would hold in the 1 GB heap the launcher gives the job by default.
         * The job parameter, or an explicit table option, wins over the default here.
         */
        private Table boundedWriteBuffer(Table table, Configuration conf)
        {
            String size = conf.getString("writeBufferSize");
            if (StringUtils.isBlank(size) && !table.options().containsKey(CoreOptions.WRITE_BUFFER_SIZE.key())) {
                size = DEFAULT_WRITE_BUFFER_SIZE;
            }
            if (StringUtils.isBlank(size)) {
                return table;
            }
            log.info("write buffer size is {}", size);
            return table.copy(Map.of(CoreOptions.WRITE_BUFFER_SIZE.key(), size));
        }

        /**
         * Reads the optional column list, which names the table columns in the order the reader
         * produces them. Without it the two are taken to be in the same order.
         *
         * @return the table column each reader column feeds, or null for positional mapping
         */
        private int[] projection(Configuration conf, String tableName)
        {
            List<String> names = conf.getList("column", String.class);
            if (names == null || names.isEmpty()) {
                return null;
            }
            Map<String, Integer> byName = new HashMap<>();
            for (int i = 0; i < columns.length; i++) {
                byName.put(columns[i].name().toLowerCase(), i);
            }
            int[] mapping = new int[names.size()];
            for (int i = 0; i < names.size(); i++) {
                Integer target = byName.get(names.get(i).trim().toLowerCase());
                if (target == null) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR, String.format(
                            "column [%s] of the column list does not exist in table %s", names.get(i), tableName));
                }
                mapping[i] = target;
            }
            return mapping;
        }

        /**
         * Converts a record into a Paimon row.
         *
         * @return the row, or null when a column could not be converted: such a record is
         * reported as dirty and must not be written
         */
        private GenericRow convert(Record record)
        {
            int produced = record.getColumnNumber();
            if (projection == null ? produced > columns.length : produced != projection.length) {
                throw AddaxException.asAddaxException(CONFIG_ERROR, String.format(
                        "the table has %d columns%s, but the reader produced %d: check that the reader "
                                + "columns match the table schema",
                        columns.length,
                        projection == null ? "" : " and the column list names " + projection.length,
                        produced));
            }

            GenericRow data = new GenericRow(columns.length);
            for (int i = 0; i < produced; i++) {
                Column column = record.getColumn(i);
                if (column == null) {
                    continue;
                }
                int target = projection == null ? i : projection[i];
                try {
                    data.setField(target, toFieldValue(column, types[target]));
                }
                catch (Exception e) {
                    getTaskPluginCollector().collectDirtyRecord(record, String.format(
                            "failed to convert column [%s] to %s: %s", columns[target].name(), types[target], e));
                    return null;
                }
            }
            return data;
        }

        /** Converts a column to the object layout Paimon keeps in memory for that type. */
        private Object toFieldValue(Column column, DataType type)
        {
            switch (type.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return BinaryString.fromString(column.asString());
                case BOOLEAN:
                    return column.asBoolean();
                case TINYINT:
                    return column.asBigInteger() == null ? null : column.asBigInteger().byteValue();
                case SMALLINT:
                    return column.asBigInteger() == null ? null : column.asBigInteger().shortValue();
                case INTEGER:
                    return column.asBigInteger() == null ? null : column.asBigInteger().intValue();
                case BIGINT:
                    return column.asLong();
                case FLOAT:
                    return column.asDouble() == null ? null : column.asDouble().floatValue();
                case DOUBLE:
                    return column.asDouble();
                case DECIMAL:
                    if (column.asBigDecimal() == null) {
                        return null;
                    }
                    DecimalType decimalType = (DecimalType) type;
                    return Decimal.fromBigDecimal(column.asBigDecimal(),
                            decimalType.getPrecision(), decimalType.getScale());
                case DATE:
                    // Paimon keeps DATE as an int holding days since epoch, not as a timestamp
                    java.util.Date date = column.asDate();
                    return date == null ? null : (int) new java.sql.Date(date.getTime()).toLocalDate().toEpochDay();
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    return Timestamp.fromSQLTimestamp(column.asTimestamp());
                case VARBINARY:
                case BINARY:
                    return column.asBytes();
                case ARRAY:
                    return toArray(column.asString(), (ArrayType) type);
                case MAP:
                    return toMap(column.asString(), (MapType) type);
                default:
                    throw new UnsupportedOperationException("unsupported column type " + type);
            }
        }

        private GenericArray toArray(String text, ArrayType type)
        {
            if (text == null) {
                return null;
            }
            String[] parts = text.split(",");
            Object[] elements = new Object[parts.length];
            for (int i = 0; i < parts.length; i++) {
                elements[i] = toElementValue(parts[i].trim(), type.getElementType());
            }
            return new GenericArray(elements);
        }

        private GenericMap toMap(String text, MapType type)
        {
            if (text == null) {
                return null;
            }
            Map<Object, Object> entries = new LinkedHashMap<>();
            Map<String, Object> raw = JSON.parseObject(text, Map.class);
            for (Map.Entry<String, Object> entry : raw.entrySet()) {
                entries.put(toElementValue(entry.getKey(), type.getKeyType()),
                        toElementValue(entry.getValue(), type.getValueType()));
            }
            return new GenericMap(entries);
        }

        /**
         * Converts one array element or one map key/value, both of which reach us as the
         * text they had in the source record, into the object Paimon keeps in memory.
         */
        private Object toElementValue(Object value, DataType type)
        {
            if (value == null) {
                return null;
            }
            String text = String.valueOf(value);
            switch (type.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return BinaryString.fromString(text);
                case BOOLEAN:
                    return Boolean.valueOf(text);
                case TINYINT:
                    return Byte.valueOf(text);
                case SMALLINT:
                    return Short.valueOf(text);
                case INTEGER:
                    return Integer.valueOf(text);
                case BIGINT:
                    return Long.valueOf(text);
                case FLOAT:
                    return Float.valueOf(text);
                case DOUBLE:
                    return Double.valueOf(text);
                case DECIMAL:
                    DecimalType decimalType = (DecimalType) type;
                    return Decimal.fromBigDecimal(new BigDecimal(text),
                            decimalType.getPrecision(), decimalType.getScale());
                case DATE:
                    return (int) java.sql.Date.valueOf(text).toLocalDate().toEpochDay();
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    return Timestamp.fromSQLTimestamp(java.sql.Timestamp.valueOf(text));
                default:
                    throw new UnsupportedOperationException("unsupported element type " + type);
            }
        }
    }
}
