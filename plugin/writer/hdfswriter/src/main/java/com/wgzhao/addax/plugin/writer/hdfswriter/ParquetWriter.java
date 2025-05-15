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
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericData;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;

public class ParquetWriter
        extends HdfsHelper
        implements IHDFSWriter
{
    private static final Logger logger = LoggerFactory.getLogger(ParquetWriter.class.getName());
    private static final int DECIMAL_BYTE_LENGTH = 16;
    private static final int PAGE_SIZE = 1024 * 1024;
    private static final int DICTIONARY_PAGE_SIZE = 512 * 1024;
    private static final String WRITER_TIME_ZONE = "writer.time.zone";
    private static final long MILLIS_PER_DAY = 86400000L;
    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    private static final long NANOS_PER_MILLISECOND = 1000000L;

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

        // Construct parquet schema
        MessageType schema = generateParquetSchema(columns);
        Path path = new Path(fileName);
        logger.info("Begin to write parquet file [{}]", fileName);

        // Configure Hadoop and Parquet settings
        setupHadoopConfiguration(schema);

        try (org.apache.parquet.hadoop.ParquetWriter<Group> writer = createParquetWriter(path, codecName, schema)) {
            writeRecords(lineReceiver, columns, taskPluginCollector, writer, schema);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to write Parquet file: " + fileName, e);
        }
    }

    private void setupHadoopConfiguration(MessageType schema)
    {
        GenericData decimalSupport = new GenericData();
        decimalSupport.addLogicalTypeConversion(new Conversions.DecimalConversion());
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

    private void writeRecords(RecordReceiver lineReceiver, List<Configuration> columns,
            TaskPluginCollector taskPluginCollector, org.apache.parquet.hadoop.ParquetWriter<Group> writer,
            MessageType schema)
            throws IOException
    {

        SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            Group group = buildRecord(record, columns, taskPluginCollector, simpleGroupFactory);
            writer.write(group);
        }
    }

    public Group buildRecord(
            Record record, List<Configuration> columns,
            TaskPluginCollector taskPluginCollector, SimpleGroupFactory simpleGroupFactory)
    {
        Group group = simpleGroupFactory.newGroup();
        for (int i = 0; i < record.getColumnNumber(); i++) {
            Column column = record.getColumn(i);
            if (null == column || column.getRawData() == null) {
                continue;
            }

            String colName = columns.get(i).getString(Key.NAME);
            String typename = columns.get(i).getString(Key.TYPE).toUpperCase();

            try {
                if (typename.startsWith("ARRAY<")) {
                    appendArrayValue(group, column, colName);
                }
                else if (typename.startsWith("MAP<")) {
                    appendMapValue(group, column, colName);
                }
                else {
                    SupportHiveDataType columnType = SupportHiveDataType.valueOf(typename);
                    appendValueByType(group, column, colName, columnType, columns.get(i));
                }
            }
            catch (IllegalArgumentException e) {
                logger.warn("Convert type [{}] into string", typename);
                group.append(colName, column.asString());
            }
        }
        return group;
    }

    private void appendValueByType(Group group, Column column, String colName,
            SupportHiveDataType columnType, Configuration colConfig)
    {

        switch (columnType) {
            case INT, INTEGER -> group.append(colName, Integer.parseInt(column.getRawData().toString()));
            case BIGINT, LONG -> group.append(colName, column.asLong());
            case FLOAT -> group.append(colName, column.asDouble().floatValue());
            case DOUBLE -> group.append(colName, column.asDouble());
            case BOOLEAN -> group.append(colName, column.asBoolean());
            case DECIMAL -> {
                int scale = colConfig.getInt(Key.SCALE, Constant.DEFAULT_DECIMAL_MAX_SCALE);
                group.append(colName, decimalToBinary(column.asString(), scale));
            }
            case TIMESTAMP -> group.append(colName, tsToBinary(column.asTimestamp()));
            case DATE -> group.append(colName, (int) Math.round(column.asLong() * 1.0 / MILLIS_PER_DAY));
            default -> group.append(colName, column.asString());
        }
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
     * Convert timestamp to parquet INT96
     *
     * @param ts the {@link Timestamp} to convert
     * @return {@link Binary}
     */
    private Binary tsToBinary(Timestamp ts)
    {
        long millis = ts.getTime();
        int julianDays = (int) (millis / MILLIS_PER_DAY) + JULIAN_EPOCH_OFFSET_DAYS;
        long nanosOfDay = (millis % MILLIS_PER_DAY) * NANOS_PER_MILLISECOND;

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
     * @param decimal the decimal value string to convert
     * @param scale the desired scale
     * @return {@link Binary}
     */
    private Binary decimalToBinary(String decimal, int scale)
    {
        BigDecimal bigDecimal = new BigDecimal(decimal);
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
            String type = column.getString(Key.TYPE).trim().toUpperCase();
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

    private static PrimitiveType getPrimitiveType(String type, String fieldName, Type.Repetition repetition, Configuration column)
    {
        switch (type) {
            case "INT" -> {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition).named(fieldName);
            }
            case "BIGINT", "LONG" -> {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition).named(fieldName);
            }
            case "DECIMAL" -> {
                int precision = column.getInt(Key.PRECISION, Constant.DEFAULT_DECIMAL_MAX_PRECISION);
                int scale = column.getInt(Key.SCALE, Constant.DEFAULT_DECIMAL_MAX_SCALE);
                return Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
                        .length(DECIMAL_BYTE_LENGTH)
                        .as(decimalType(scale, precision))
                        .named(fieldName);
            }
            case "STRING" -> {
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                        .as(LogicalTypeAnnotation.stringType())
                        .named(fieldName);
            }
            case "BYTES" -> {
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
            default -> {
                try {
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.valueOf(type), repetition)
                            .named(fieldName);
                }
                catch (IllegalArgumentException e) {
                    logger.warn("Unknown type: {}, using STRING instead", type);
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                            .as(LogicalTypeAnnotation.stringType())
                            .named(fieldName);
                }
            }
        }
    }
}