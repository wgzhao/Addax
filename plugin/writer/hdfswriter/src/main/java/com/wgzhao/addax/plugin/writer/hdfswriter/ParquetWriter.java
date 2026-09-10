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

package com.wgzhao.addax.plugin.writer.hdfswriter;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;

/** Parquet Writer. */
public class ParquetWriter
        extends HdfsHelper
        implements IHDFSWriter
{
    private static final Logger logger = LoggerFactory.getLogger(ParquetWriter.class.getName());
    private static final int DECIMAL_BYTE_LENGTH = 16;
    private static final int PAGE_SIZE = 1024 * 1024;
    private static final int DICTIONARY_PAGE_SIZE = 512 * 1024;
    private static final String WRITER_TIME_ZONE = "writer.time.zone";
    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;

    /** Whether a configured column holds a single value, an array or a map. */
    private enum ColumnKind
    {
        SCALAR, ARRAY, MAP
    }

    /**
     * The resolved shape of one configured column.
     * <p>
     * Resolved once before the records are read so that the per-field path does not re-read and
     * re-parse the configuration for every field of every record: each lookup walks the
     * configuration path and allocates several short-lived objects on the way.
     *
     * @param name the field name
     * @param declaredType the type as configured, for diagnostics
     * @param kind whether the column holds a single value, an array or a map
     * @param type the resolved type of a scalar column
     * @param scale the decimal scale
     */
    private record ColumnPlan(String name, String declaredType, ColumnKind kind,
            SupportHiveDataType type, int scale)
    {
    }

    /** Parquetwriter. */
    public ParquetWriter(Configuration conf)
    {
        super();
        getFileSystem(conf);
    }

    /*
     * the schema of a parquet file is as follows:
     * {
     *    "type":	"record",
     *    "name":	"testFile",
     *    "doc":	"test records",
     *    "fields":
     *      [{
     *        "name":	"id",
     *        "type":	["null", "int"]
     *
     *      },
     *      {
     *        "name":	"empName",
     *        "type":	"string"
     *      }
     *    ]
     *  }
     * "null" indicates that the field is optional
     */
    @Override
    public void write(RecordReceiver lineReceiver, Configuration config, String fileName, TaskPluginCollector taskPluginCollector)
    {
        List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, "UNCOMPRESSED").toUpperCase().trim();
        CompressionCodecName codecName = CompressionCodecName.fromConf(compress.equals("NONE") ? "UNCOMPRESSED" : compress);

        // Construct parquet schema, which also validates the configured types
        MessageType schema = generateParquetSchema(columns);
        List<ColumnPlan> plans = resolveColumns(columns);
        Path path = new Path(fileName);
        logger.info("Begin to write parquet file [{}]", fileName);

        // Configure Hadoop and Parquet settings
        setupHadoopConfiguration(schema);

        try (org.apache.parquet.hadoop.ParquetWriter<Group> writer = createParquetWriter(path, codecName, schema)) {
            writeRecords(lineReceiver, plans, taskPluginCollector, writer, schema);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to write Parquet file: " + fileName, e);
        }
    }

    private void setupHadoopConfiguration(MessageType schema)
    {
        hadoopConf.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);
        hadoopConf.setBoolean(AvroWriteSupport.WRITE_FIXED_AS_INT96, true);
        GroupWriteSupport.setSchema(schema, hadoopConf);
    }

    private org.apache.parquet.hadoop.ParquetWriter<Group> createParquetWriter(
            Path path, CompressionCodecName codecName, MessageType schema)
            throws IOException
    {

        Map<String, String> extraMeta = new HashMap<>();
        // Hive needs timezone info to handle timestamp
        extraMeta.put(WRITER_TIME_ZONE, ZoneId.systemDefault().toString());

        return ExampleParquetWriter.builder(HadoopOutputFile.fromPath(path, hadoopConf))
                .withCompressionCodec(codecName)
                .withConf(hadoopConf)
                .enableDictionaryEncoding()
                .withPageSize(PAGE_SIZE)
                .withDictionaryPageSize(DICTIONARY_PAGE_SIZE)
                .withValidation(false)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withExtraMetaData(extraMeta)
                .build();
    }

    private void writeRecords(RecordReceiver lineReceiver, List<ColumnPlan> plans,
            TaskPluginCollector taskPluginCollector, org.apache.parquet.hadoop.ParquetWriter<Group> writer,
            MessageType schema)
            throws IOException
    {

        SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            Group group = buildRecord(record, plans, taskPluginCollector, simpleGroupFactory);
            if (group == null) {
                // a field could not be converted; the record has been reported as dirty
                continue;
            }
            writer.write(group);
        }
    }

    /** Buildrecord. */
    public Group buildRecord(
            Record record, List<ColumnPlan> plans,
            TaskPluginCollector taskPluginCollector, SimpleGroupFactory simpleGroupFactory)
    {
        Group group = simpleGroupFactory.newGroup();
        for (int i = 0; i < record.getColumnNumber(); i++) {
            Column column = record.getColumn(i);
            if (null == column || column.getRawData() == null) {
                continue;
            }

            ColumnPlan plan = plans.get(i);
            try {
                switch (plan.kind()) {
                    case ARRAY -> appendArrayValue(group, column, plan.name());
                    case MAP -> appendMapValue(group, column, plan.name());
                    case SCALAR -> appendValueByType(group, column, plan);
                }
            }
            catch (Exception e) {
                // a value the declared type cannot hold is a dirty record, not a string to stuff
                // into a typed field: SimpleGroup accepts it and the parquet writer then dies with
                // a ClassCastException once the group reaches it
                taskPluginCollector.collectDirtyRecord(record, String.format(
                        "Type conversion error: target field type: [%s], field value: [%s], error: %s",
                        plan.declaredType(), column.getRawData(), e.getMessage()));
                return null;
            }
        }
        return group;
    }

    /**
     * Resolve every configured column before a single record is read.
     * <p>
     * A column type the writer cannot convert fails here rather than on every record, which would
     * collect a dirty record for each row and leave an empty file behind a job that reported
     * success.
     *
     * @param columns the configured columns
     * @return the resolved plans, in column order
     */
    private static List<ColumnPlan> resolveColumns(List<Configuration> columns)
    {
        List<ColumnPlan> plans = new ArrayList<>(columns.size());
        for (Configuration column : columns) {
            String declaredType = column.getString(Key.TYPE);
            String name = column.getString(Key.NAME);
            String upper = declaredType.trim().toUpperCase(Locale.ROOT);
            int scale = column.getInt(Key.SCALE, Constant.DEFAULT_DECIMAL_MAX_SCALE);

            ColumnKind kind;
            SupportHiveDataType type = null;
            if (upper.startsWith("ARRAY<")) {
                kind = ColumnKind.ARRAY;
            }
            else if (upper.startsWith("MAP<")) {
                kind = ColumnKind.MAP;
            }
            else {
                kind = ColumnKind.SCALAR;
                try {
                    type = SupportHiveDataType.of(upper);
                }
                catch (IllegalArgumentException e) {
                    throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                            String.format("Unsupported field type. Field name: [%s], Field type: [%s].",
                                    name, declaredType));
                }
            }
            plans.add(new ColumnPlan(name, declaredType, kind, type, scale));
        }
        return plans;
    }

    private void appendValueByType(Group group, Column column, ColumnPlan plan)
    {
        switch (plan.type()) {
            case TINYINT, SMALLINT, INT -> group.append(plan.name(), Integer.parseInt(column.getRawData().toString()));
            case BIGINT -> group.append(plan.name(), column.asLong());
            case FLOAT -> group.append(plan.name(), column.asDouble().floatValue());
            case DOUBLE -> group.append(plan.name(), column.asDouble());
            case BOOLEAN -> group.append(plan.name(), column.asBoolean());
            case DECIMAL -> group.append(plan.name(), decimalToBinary(column.asBigDecimal(), plan.scale()));
            case TIMESTAMP -> group.append(plan.name(), tsToBinary(column.asTimestamp()));
            case DATE -> group.append(plan.name(), dateToEpochDay(column));
            default -> group.append(plan.name(), formatTimeWithNanos(column, columnTimeZone));
        }
    }

    /**
     * Converts a DATE column to the number of days since the unix epoch.
     * <p>
     * A DATE arrives as an instant at local midnight, so it has to be read as a calendar date.
     * Dividing the raw millis by {@code MILLIS_PER_DAY} instead counts elapsed days since the
     * epoch and lands a day early in any zone east of UTC, which also disagreed with
     * {@link OrcWriter} for the very same job configuration.
     *
     * @param column the column holding the date value
     * @return the epoch day of the date
     */
    private static int dateToEpochDay(Column column)
    {
        return (int) new java.sql.Date(column.asLong()).toLocalDate().toEpochDay();
    }

    /**
     * Appends an array value to a Parquet group structure.
     * <p>
     * This method processes a JSON array string representation from a column
     * and converts it into a properly nested Parquet array structure.
     * The array is added to the specified group with the given column name.
     *
     * @param group The Parquet group to which the array will be added
     * @param column The column containing the array data as a JSON string
     * @param colName The name of the column/field in the Parquet schema
     */
    private void appendArrayValue(Group group, Column column, String colName)
    {
        //  "['value1', 'value2', ...]"
        JSONArray jsonArray = JSONArray.parseArray(column.asString());
        Group arrayGroup = group.addGroup(colName);

        for (Object value : jsonArray) {
            Group listItem = arrayGroup.addGroup("list");
            if (value == null) {
                // keep null value
                continue;
            }
            appendPrimitiveValue(value, "element", listItem);
        }
    }

    private static void appendPrimitiveValue(Object value, String element, Group group)
    {
        if (value instanceof Number number) {
            if (value instanceof Integer i ) {
                group.append(element, i);
            }
            else if (value instanceof Long i) {
                group.append(element, i);
            }
            else if (value instanceof Short i) {
                group.append(element, i);
            }
            else if (value instanceof Float || value instanceof Double) {
                group.append(element, number.doubleValue());
            }
            else {
                // BigDecimal
                group.append(element, value.toString());
            }
        }
        else if (value instanceof Boolean) {
            group.append(element, (Boolean) value);
        }
        else {
            // string or other type
            group.append(element, value.toString());
        }
    }

    /**
     * Appends a map value to a Parquet group structure.
     * <p>
     * This method processes a JSON object string representation from a column
     * and converts it into a properly nested Parquet map structure.
     * The map is added to the specified group with the given column name.
     *
     * @param group The Parquet group to which the map will be added
     * @param column The column containing the map data as a JSON string
     * @param colName The name of the column/field in the Parquet schema
     */
    private void appendMapValue(Group group, Column column, String colName)
    {
        //  {'key1':'value1', 'key2':'value2', ...}
        JSONObject jsonObject = JSONObject.parseObject(column.asString());

        Group mapGroup = group.addGroup(colName);

        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            Group kvGroup = mapGroup.addGroup("key_value");
            kvGroup.append("key", entry.getKey());

            Object value = entry.getValue();
            if (value == null) {
                continue;
            }
            appendPrimitiveValue(value, "value", kvGroup);
        }
    }

    /**
     * Convert timestamp to parquet INT96.
     * <p>
     * INT96 holds a UTC instant, so the day and the time of day are both taken from the epoch
     * milliseconds directly. This deliberately differs from {@link #formatTimeWithNanos}, which
     * renders a TIME column as a wall clock in the configured zone: the two look alike but are
     * not interchangeable, and unifying them would shift every INT96 timestamp by an offset.
     *
     * @param ts the {@link Timestamp} to convert
     * @return {@link Binary}
     */
    private Binary tsToBinary(Timestamp ts)
    {
        long millis = ts.getTime();
        // floorDiv/floorMod rather than plain division: truncation rounds towards zero, so any
        // pre-1970 value would leave a negative time-of-day inside the 12-byte INT96 buffer
        int julianDays = (int) Math.floorDiv(millis, MILLIS_PER_DAY) + JULIAN_EPOCH_OFFSET_DAYS;
        long nanosOfDay = Math.floorMod(millis, MILLIS_PER_DAY) * NANOS_PER_MILLISECOND
                + ts.getNanos() % NANOS_PER_MILLISECOND;

        // Write INT96 timestamp
        byte[] timestampBuffer = new byte[12];
        ByteBuffer buf = ByteBuffer.wrap(timestampBuffer);
        buf.order(ByteOrder.LITTLE_ENDIAN).putLong(nanosOfDay).putInt(julianDays);

        // This is the properly encoded INT96 timestamp
        return Binary.fromConstantByteArray(timestampBuffer);
    }

    /**
     * Convert Decimal to {@link Binary} using fixed 16 bytes array
     *
     * @param bigDecimal the decimal value to convert
     * @param scale the desired scale
     * @return {@link Binary}
     */
    private Binary decimalToBinary(BigDecimal bigDecimal, int scale)
    {
        int realScale = bigDecimal.scale();
        RoundingMode mode = scale >= realScale ? RoundingMode.UNNECESSARY : RoundingMode.HALF_UP;

        byte[] decimalBytes = bigDecimal.setScale(scale, mode)
                .unscaledValue()
                .toByteArray();

        // Preallocate fixed size array
        byte[] fixedSizeBytes = new byte[DECIMAL_BYTE_LENGTH];

        if (decimalBytes.length <= DECIMAL_BYTE_LENGTH) {
            // Pad left with zeros (copy from right to left)
            int destPos = DECIMAL_BYTE_LENGTH - decimalBytes.length;
            System.arraycopy(decimalBytes, 0, fixedSizeBytes, destPos, decimalBytes.length);
            return Binary.fromConstantByteArray(fixedSizeBytes);
        }
        else {
            throw new IllegalArgumentException(String.format(
                    "Decimal size: %d exceeds maximum allowed: %d",
                    decimalBytes.length, DECIMAL_BYTE_LENGTH));
        }
    }

    private MessageType generateParquetSchema(List<Configuration> columns)
    {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        Type.Repetition repetition = Type.Repetition.OPTIONAL;

        for (Configuration column : columns) {
            String type = column.getString(Key.TYPE).trim().toUpperCase(Locale.ROOT);
            String fieldName = column.getString(Key.NAME);
            Type field = createFieldByType(type, fieldName, repetition, column);
            builder.addField(field);
        }

        return builder.named("addax");
    }

    private Type createFieldByType(String type, String fieldName, Type.Repetition repetition, Configuration column)
    {
        if (type.startsWith("ARRAY<")) {
            // extract element type，e.g the String of ARRAY<STRING>
            String elementType = type.substring(6, type.length() - 1).trim().toUpperCase();
            PrimitiveType element = getPrimitiveType(elementType, "element", Type.Repetition.REQUIRED, column);
            return Types.optionalList()
                    .element(element)
                    .named(fieldName);
        }
        else if (type.startsWith("MAP<")) {
            // MAP<STRING,STRING>
            String mapTypes = type.substring(4, type.length() - 1);
            String[] keyValueTypes = mapTypes.split(",", 2);
            String keyType = keyValueTypes[0].trim().toUpperCase();
            String valueType = keyValueTypes[1].trim().toUpperCase();

            Type keyField = Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named("key");
            Type valueField = getPrimitiveType(valueType, "value", repetition, column);

            return Types.map(repetition)
                    .key(keyField)
                    .value(valueField)
                    .named(fieldName);
        }

        return getPrimitiveType(type, fieldName, repetition, column);
    }

    /**
     * Reduce a configured type name to the token the schema switch below is written in.
     * <p>
     * The configuration carries the SQL/Hive spellings: aliases ({@code integer}, {@code long})
     * and parameters ({@code varchar(10)}, {@code char(3)}). None of them matched a case here, so
     * such a column fell through to the parquet primitive lookup, threw, and was silently
     * declared as a STRING - a different logical type from the one ORC wrote for the same config.
     *
     * @param type the configured type
     * @return the canonical token, or the upper-cased input when it is not a Hive type at all
     */
    private static String canonicalTypeName(String type)
    {
        try {
            return SupportHiveDataType.of(type).name();
        }
        catch (IllegalArgumentException e) {
            // not part of the Hive vocabulary: BYTES and the parquet primitive type names
            return type.trim().toUpperCase(Locale.ROOT);
        }
    }

    private static PrimitiveType getPrimitiveType(String type, String fieldName, Type.Repetition repetition, Configuration column)
    {
        switch (canonicalTypeName(type)) {
            case "TINYINT", "SMALLINT", "INT" -> {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition).named(fieldName);
            }
            case "BIGINT" -> {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition).named(fieldName);
            }
            case "FLOAT" -> {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition).named(fieldName);
            }
            case "DOUBLE" -> {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition).named(fieldName);
            }
            case "BOOLEAN" -> {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition).named(fieldName);
            }
            case "DECIMAL" -> {
                int precision = column.getInt(Key.PRECISION, Constant.DEFAULT_DECIMAL_MAX_PRECISION);
                int scale = column.getInt(Key.SCALE, Constant.DEFAULT_DECIMAL_MAX_SCALE);
                return Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
                        .length(DECIMAL_BYTE_LENGTH)
                        .as(decimalType(scale, precision))
                        .named(fieldName);
            }
            case "STRING", "VARCHAR", "CHAR" -> {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                        .as(LogicalTypeAnnotation.stringType())
                        .named(fieldName);
            }
            case "BINARY", "BYTES" -> {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                        .named(fieldName);
            }
            case "DATE" -> {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                        .as(LogicalTypeAnnotation.dateType())
                        .named(fieldName);
            }
            case "TIMESTAMP" -> {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, repetition)
                        .named(fieldName);
            }
            default -> throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                    String.format("Unsupported field type. Field name: [%s], Field type: [%s].", fieldName, type));
        }
    }
}