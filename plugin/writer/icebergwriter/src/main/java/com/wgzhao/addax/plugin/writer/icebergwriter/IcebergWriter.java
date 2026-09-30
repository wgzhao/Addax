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

package com.wgzhao.addax.plugin.writer.icebergwriter;

import com.alibaba.fastjson2.JSON;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;

/** Iceberg Writer. */
public class IcebergWriter
        extends Writer
{
    /** Job. */
    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration conf = null;
        private Catalog catalog = null;
        private String tableName = null;

        @Override
        public void init()
        {
            this.conf = this.getPluginJobConf();
            try {
                this.catalog = IcebergHelper.getCatalog(conf);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }

            tableName = IcebergHelper.getTableName(conf);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            List<Configuration> configurations = new ArrayList<>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                // every task gets its own copy, so a task that adjusts its configuration cannot
                // change the configuration of the tasks next to it
                configurations.add(conf.clone());
            }
            return configurations;
        }

        @Override
        public void prepare()
        {
            String writeMode = this.conf.getString("writeMode");
            if ("truncate".equalsIgnoreCase(writeMode)) {
                Table table = catalog.loadTable(TableIdentifier.of(tableName.split("\\.")));
                table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();
                LOG.info("table [{}] is truncated", tableName);
            }
            else if (writeMode != null && !"append".equalsIgnoreCase(writeMode)) {
                // only truncate and append exist: the writer always appends, and a job asking for
                // something else would silently be an append
                LOG.warn("writeMode [{}] is not supported by icebergwriter, the rows are appended", writeMode);
            }
        }

        @Override
        public void destroy()
        {
            IcebergHelper.closeCatalog(catalog);
        }
    }

    /** Task. */
    public static class Task
            extends Writer.Task
    {
        private static final Logger log = LoggerFactory.getLogger(Task.class);

        private Catalog catalog = null;
        private String tableName = null;
        private Table table = null;
        private Schema schema = null;
        private FileFormat fileFormat = FileFormat.PARQUET;
        private FileAppenderFactory<org.apache.iceberg.data.Record> appenderFactory = null;
        private OutputFileFactory outputFileFactory = null;
        private PartitionKey partitionKey = null;
        private long targetFileSize = 0;
        private List<FieldBinding> bindings = null;
        // the reader's column count is only known from the first record on, and the check for it is
        // worth doing once: a column type this writer cannot write would otherwise be a dirty record
        // for every single row
        private boolean bindingsChecked = false;

        @Override
        public void startWrite(RecordReceiver recordReceiver)
        {
            long total = 0;
            PartitionedFanoutWriter<org.apache.iceberg.data.Record> writer = newPartitionedWriter();
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    GenericRecord row = toGenericRecord(record);
                    if (row == null) {
                        // the row was collected as dirty, one of its columns could not be converted
                        continue;
                    }
                    writer.write(row);
                    total++;
                }

                // one commit for the whole task. A commit per batch leaves a file and a snapshot for
                // every batch worth of rows, which is a fraction of the target file size, and a task
                // that fails after a batch has already committed rows that a rerun writes again
                WriteResult writeResult = writer.complete();
                if (writeResult.dataFiles().length > 0) {
                    // a task that wrote nothing (an empty reader, or every row dirty) has no file to
                    // point at, and an empty snapshot is one a reader has to plan around for nothing
                    AppendFiles appends = table.newAppend();
                    for (DataFile dataFile : writeResult.dataFiles()) {
                        appends.appendFile(dataFile);
                    }
                    appends.commit();
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            finally {
                closeQuietly(writer);
            }

            getTaskPluginCollector().collectMessage("writeSize", String.valueOf(total));
            log.info("task end, write size :{}", total);
        }

        @Override
        public void init()
        {
            Configuration conf = super.getPluginJobConf();

            try {
                this.catalog = IcebergHelper.getCatalog(conf);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }

            this.tableName = IcebergHelper.getTableName(conf);
            this.table = catalog.loadTable(TableIdentifier.of(tableName.split("\\.")));
            this.schema = table.schema();
            this.bindings = buildBindings(schema);
            this.fileFormat = resolveFileFormat(table);
            this.targetFileSize = PropertyUtil.propertyAsLong(
                    table.properties(),
                    TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
                    TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
            this.appenderFactory = newAppenderFactory(table);
            // the ids only name the files iceberg writes, they are not part of the commit
            this.outputFileFactory = OutputFileFactory.builderFor(table, getTaskGroupId(), getTaskId())
                    .format(fileFormat)
                    .build();
            this.partitionKey = new PartitionKey(table.spec(), table.spec().schema());

            log.info("writing to table [{}] as {} with {} column(s)", tableName, fileFormat, bindings.size());
        }

        @Override
        public void destroy()
        {
            IcebergHelper.closeCatalog(catalog);
        }

        private PartitionedFanoutWriter<org.apache.iceberg.data.Record> newPartitionedWriter()
        {
            return new PartitionedFanoutWriter<org.apache.iceberg.data.Record>(table.spec(), fileFormat, appenderFactory, outputFileFactory, table.io(), targetFileSize)
            {
                @Override
                protected PartitionKey partition(org.apache.iceberg.data.Record record)
                {
                    // the key is filled in and handed back, which is what this writer expects: it copies
                    // every key it keeps, so no row can end up in another partition's file
                    partitionKey.partition(record);
                    return partitionKey;
                }
            };
        }

        private GenericRecord toGenericRecord(Record record)
        {
            checkBindings(record.getColumnNumber());

            GenericRecord row = GenericRecord.create(schema);
            int columns = Math.min(record.getColumnNumber(), bindings.size());
            for (int i = 0; i < columns; i++) {
                Column column = record.getColumn(i);
                if (column == null) {
                    continue;
                }
                FieldBinding binding = bindings.get(i);
                try {
                    row.set(binding.position(), binding.converter().toValue(column));
                }
                catch (Exception e) {
                    // a value the declared type cannot hold makes the row dirty instead of writing a
                    // null where the source had a value
                    getTaskPluginCollector().collectDirtyRecord(record, String.format(
                            "failed to convert column [%s] to %s: %s", binding.name(), binding.type(), e));
                    return null;
                }
            }
            return row;
        }

        private void checkBindings(int recordColumns)
        {
            if (bindingsChecked) {
                return;
            }
            bindingsChecked = true;

            if (recordColumns > bindings.size()) {
                throw AddaxException.asAddaxException(CONFIG_ERROR, String.format(
                        "the reader produces %d column(s) but table [%s] has %d", recordColumns, tableName, bindings.size()));
            }
            for (int i = 0; i < recordColumns; i++) {
                FieldBinding binding = bindings.get(i);
                if (binding.converter() == null) {
                    throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE, String.format(
                            "column [%s] of type %s is not supported by icebergwriter", binding.name(), binding.type()));
                }
            }
        }

        private static List<FieldBinding> buildBindings(Schema schema)
        {
            // the reader and the table are matched by position: the record column at an index fills the
            // table column at the same index, which is also the order GenericRecord keeps its values in
            List<Types.NestedField> fields = schema.columns();
            List<FieldBinding> bindings = new ArrayList<>(fields.size());
            for (int i = 0; i < fields.size(); i++) {
                Types.NestedField field = fields.get(i);
                bindings.add(new FieldBinding(i, field.name(), field.type(), converterFor(field.type())));
            }
            return bindings;
        }

        private static ValueConverter converterFor(Type type)
        {
            if (type.isPrimitiveType()) {
                return scalarConverter(type);
            }
            return switch (type.typeId()) {
                case LIST -> listConverter((Types.ListType) type);
                case MAP -> mapConverter((Types.MapType) type);
                default -> null;
            };
        }

        private static ValueConverter scalarConverter(Type type)
        {
            return switch (type.typeId()) {
                case BOOLEAN -> Column::asBoolean;
                case INTEGER -> IcebergWriter.Task::toInteger;
                case LONG -> Column::asLong;
                case FLOAT -> IcebergWriter.Task::toFloat;
                case DOUBLE -> Column::asDouble;
                case DATE -> IcebergWriter.Task::toDate;
                case TIME -> IcebergWriter.Task::toTime;
                case TIMESTAMP, TIMESTAMP_NANO -> IcebergWriter.Task::toTimestamp;
                case STRING -> Column::asString;
                case UUID -> IcebergWriter.Task::toUuid;
                // fixed is written from a byte array, binary is written from a byte buffer
                case FIXED -> Column::asBytes;
                case BINARY -> IcebergWriter.Task::toBinary;
                case DECIMAL -> column -> toDecimal(column, ((Types.DecimalType) type).scale());
                default -> null;
            };
        }

        private static ValueConverter listConverter(Types.ListType listType)
        {
            if (!listType.elementType().isPrimitiveType()) {
                return null;
            }
            ValueConverter element = scalarConverter(listType.elementType());
            if (element == null) {
                return null;
            }
            return column -> {
                String text = column.asString();
                if (text == null) {
                    return null;
                }
                if (text.isEmpty()) {
                    return List.of();
                }
                // the input is the comma separated string this plugin has always taken
                String[] tokens = text.split(",", -1);
                List<Object> values = new ArrayList<>(tokens.length);
                for (String token : tokens) {
                    values.add(element.toValue(new StringColumn(token.trim())));
                }
                return values;
            };
        }

        private static ValueConverter mapConverter(Types.MapType mapType)
        {
            if (!mapType.keyType().isPrimitiveType() || !mapType.valueType().isPrimitiveType()) {
                return null;
            }
            ValueConverter key = scalarConverter(mapType.keyType());
            ValueConverter value = scalarConverter(mapType.valueType());
            if (key == null || value == null) {
                return null;
            }
            return column -> {
                String text = column.asString();
                if (text == null) {
                    return null;
                }
                if (text.isEmpty()) {
                    return Map.of();
                }
                Map<String, Object> entries = JSON.parseObject(text);
                if (entries == null) {
                    return null;
                }
                Map<Object, Object> values = new LinkedHashMap<>(entries.size());
                for (Map.Entry<String, Object> entry : entries.entrySet()) {
                    values.put(key.toValue(new StringColumn(entry.getKey())), value.toValue(asColumn(entry.getValue())));
                }
                return values;
            };
        }

        private static Integer toInteger(Column column)
        {
            BigInteger value = column.asBigInteger();
            return value == null ? null : value.intValue();
        }

        private static Float toFloat(Column column)
        {
            Double value = column.asDouble();
            return value == null ? null : value.floatValue();
        }

        private static LocalDate toDate(Column column)
        {
            return column.asLong() == null ? null : column.asTimestamp().toLocalDateTime().toLocalDate();
        }

        private static LocalTime toTime(Column column)
        {
            return column.asLong() == null ? null : column.asTimestamp().toLocalDateTime().toLocalTime();
        }

        private static LocalDateTime toTimestamp(Column column)
        {
            return column.asLong() == null ? null : column.asTimestamp().toLocalDateTime();
        }

        private static UUID toUuid(Column column)
        {
            String value = column.asString();
            return value == null ? null : UUID.fromString(value.trim());
        }

        private static ByteBuffer toBinary(Column column)
        {
            byte[] value = column.asBytes();
            return value == null ? null : ByteBuffer.wrap(value);
        }

        private static BigDecimal toDecimal(Column column, int scale)
        {
            BigDecimal value = column.asBigDecimal();
            if (value == null) {
                return null;
            }
            // iceberg refuses a decimal whose scale is not the one the column was declared with, and
            // rounding a value into the declared scale would write something the source never had
            return value.setScale(scale, RoundingMode.UNNECESSARY);
        }

        /** Wraps a value read from json so that the converters above can turn it into its declared type. */
        private static Column asColumn(Object jsonValue)
        {
            return new StringColumn(jsonValue == null ? null : String.valueOf(jsonValue));
        }

        private static FileFormat resolveFileFormat(Table table)
        {
            String configured = table.properties().get(TableProperties.DEFAULT_FILE_FORMAT);
            FileFormat format = configured == null || configured.isBlank()
                    ? FileFormat.PARQUET
                    : FileFormat.fromString(configured.trim());
            if (format != FileFormat.PARQUET && format != FileFormat.ORC) {
                throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE, String.format(
                        "table property %s is [%s], icebergwriter writes parquet and orc only",
                        TableProperties.DEFAULT_FILE_FORMAT, format));
            }
            return format;
        }

        private static FileAppenderFactory<org.apache.iceberg.data.Record> newAppenderFactory(Table table)
        {
            Map<String, String> tableProps = new HashMap<>(table.properties());
            Set<Integer> identifierFieldIds = table.schema().identifierFieldIds();
            if (identifierFieldIds == null || identifierFieldIds.isEmpty()) {
                return new GenericAppenderFactory(table, table.schema(), table.spec(), tableProps, null, null);
            }

            int[] equalityFieldIds = new int[identifierFieldIds.size()];
            int i = 0;
            for (Integer fieldId : identifierFieldIds) {
                equalityFieldIds[i++] = fieldId;
            }
            return new GenericAppenderFactory(table, table.schema(), table.spec(), tableProps, equalityFieldIds,
                    TypeUtil.select(table.schema(), new HashSet<>(identifierFieldIds)));
        }

        private static void closeQuietly(PartitionedFanoutWriter<org.apache.iceberg.data.Record> writer)
        {
            try {
                writer.close();
            }
            catch (IOException e) {
                log.warn("failed to close the iceberg writer", e);
            }
        }

        /** Turns one addax column into the value iceberg keeps for a field. */
        @FunctionalInterface
        private interface ValueConverter
        {
            /** Converts the column, a null return writes a null into the field. */
            Object toValue(Column column);
        }

        /** A table column together with the record column it is filled from. */
        private record FieldBinding(int position, String name, Type type, ValueConverter converter) {}
    }
}
