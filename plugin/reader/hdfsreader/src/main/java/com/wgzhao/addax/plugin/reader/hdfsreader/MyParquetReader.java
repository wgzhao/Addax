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

package com.wgzhao.addax.plugin.reader.hdfsreader;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.BytesColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.ColumnEntry;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;

public class MyParquetReader
{
    private static final Logger LOG = LoggerFactory.getLogger(MyParquetReader.class);

    // the offset of julian, 2440588 is 1970/1/1
    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
    private static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);

    private final MessageType schema;
    private final String nullFormat;
    private final List<ColumnEntry> columnEntries;
    private final ParquetReader<Group> reader;

    public MyParquetReader(org.apache.hadoop.conf.Configuration hadoopConf, Path path, String nullFormat, List<ColumnEntry> columns)
    {
        hadoopConf.set("parquet.avro.readInt96AsFixed", "true");
        this.nullFormat = nullFormat;
        try {
            this.schema = ParquetFileReader.open(HadoopInputFile.fromPath(path, hadoopConf)).getFileMetaData().getSchema();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to open parquet file", e);
        }
        GenericData decimalSupport = new GenericData();
        decimalSupport.addLogicalTypeConversion(new Conversions.DecimalConversion());
        try {
            this.reader = ParquetReader.builder(new GroupReadSupport(), path).withConf(hadoopConf).build();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (!columns.isEmpty()) {
            this.columnEntries = columns;
        }
        else {
            // the columns maybe empty or '*' in the configuration, we need to get the schema from the parquet file
            List<Type> fields = schema.getFields();
            this.columnEntries = new ArrayList<>(fields.size());
            for (int i = 0; i < schema.getFields().size(); i++) {
                ColumnEntry columnEntry = new ColumnEntry();
                columnEntry.setIndex(i);
                columnEntry.setType(getJavaType(fields.get(i)).name());
                this.columnEntries.add(columnEntry);
            }
        }
    }

    public void reader(RecordSender recordSender, TaskPluginCollector taskPluginCollector)
    {
        try {
            Group group = reader.read();
            while (group != null) {
                Record record = recordSender.createRecord();
                Column columnGenerated;
                try {
                    for (ColumnEntry columnEntry : columnEntries) {
                        columnGenerated = getColumn(group, columnEntry);
                        record.addColumn(columnGenerated);
                    } // end for
                    recordSender.sendToWriter(record);
                }
                catch (Exception e) {
                    if (e instanceof AddaxException ae) {
                        throw ae;
                    }
                    // cast failed means dirty data, including number format, date format, etc.
                    taskPluginCollector.collectDirtyRecord(record, e.getMessage());
                    throw new RuntimeException(e);
                }
                group = reader.read();
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Column getColumn(Group group, ColumnEntry columnEntry)
    {
        Column columnGenerated;
        String columnType = columnEntry.getType();
        Integer columnIndex = columnEntry.getIndex();
        String columnConst = columnEntry.getValue();
        if (columnConst != null) {
            return new StringColumn(columnConst);
        }

        if (schema.getFields().get(columnIndex).getRepetition() == Type.Repetition.OPTIONAL
                && group.getFieldRepetitionCount(columnIndex) == 0) {
            return new StringColumn(nullFormat);
        }

        var type = JavaType.valueOf(columnType.toUpperCase());
        return switch (type) {
            case ARRAY -> getArrayColumn(group, columnEntry, columnIndex, columnType);
            case MAP -> getMapColumn(group, columnEntry, columnIndex, columnType);
            default -> {
                try {
                    yield getPrimitiveColumn(group, columnEntry, type, columnIndex, columnType);
                }
                catch (Exception e) {
                    throw new IllegalArgumentException(
                            "Cannot convert column type %s to %s: %s".formatted(columnType, type, e));
                }
            }
        };
    }

    private static @NotNull Column getArrayColumn(Group group, ColumnEntry columnEntry, Integer columnIndex, String columnType)
    {
        // Convert array to JSON string
        Group arrayGroup = group.getGroup(columnIndex, 0);
        int listSize = arrayGroup.getFieldRepetitionCount(0);
        // all elements in the array has the same data type
        JavaType eleType = JavaType.valueOf(arrayGroup.getGroup(0, 0)
                .getType().getType(0)
                .asPrimitiveType().getPrimitiveTypeName().javaType.getTypeName().toUpperCase());
        JSONArray jsonArray = new JSONArray();
        for (int i = 0; i < listSize; i++) {
            // Try to get as string first, fallback to binary if needed
            // the array in parquet is represented as a repeated group
            Column column = getPrimitiveColumn(arrayGroup.getGroup(0, i), columnEntry, eleType, 0, columnType);
            jsonArray.add(column.getRawData());
        }
        return new StringColumn(jsonArray.toString());
    }

    private @NotNull Column getMapColumn(Group group, ColumnEntry columnEntry, Integer columnIndex, String columnType)
    {
        // Convert map to JSON string
        // Map in Parquet is represented as a repeated group of key-value pairs
        // This is a simplified approach assuming the map's key and value elements are accessible
        Group mapGroup = group.getGroup(columnIndex, 0);
        JSONObject jsonObject = new JSONObject();
        int fieldRepetitionCount = mapGroup.getFieldRepetitionCount(0);
        JavaType javaType;

        Type valueType = mapGroup.getGroup(0, 0).getType().getType(1);
        LogicalTypeAnnotation logicalTypeAnnotation = valueType.getLogicalTypeAnnotation();

        if (logicalTypeAnnotation != null) {
            javaType = JavaType.valueOf(logicalTypeAnnotation.toString());
        }
        else if (valueType.isPrimitive()) {
            javaType = JavaType.valueOf(valueType.asPrimitiveType()
                    .getPrimitiveTypeName()
                    .javaType
                    .getName()
                    .toUpperCase()
            );
        }
        else {
            throw AddaxException.asAddaxException(IO_ERROR, "Value type is unknown or complex type: " + valueType);
        }
        for (int i = 0; i < fieldRepetitionCount; i++) {
            Group keyValue = mapGroup.getGroup(0, i);
            String key = keyValue.getString(0, 0);
            // Convert the key-value pair to JSON format
            Column column = getPrimitiveColumn(keyValue, columnEntry, javaType, 1, columnType);
            jsonObject.put(key, column.getRawData());
        }
        return new StringColumn(jsonObject.toString());
    }

    private static @NotNull Column getPrimitiveColumn(Group group, ColumnEntry columnEntry, JavaType type,
            Integer columnIndex, String columnType)
    {
        return switch (type) {
            case STRING -> new StringColumn(group.getString(columnIndex, 0));
            case INT -> new LongColumn(group.getInteger(columnIndex, 0));
            case LONG -> new LongColumn(group.getLong(columnIndex, 0));
            case FLOAT -> new DoubleColumn(group.getFloat(columnIndex, 0));
            case DOUBLE -> new DoubleColumn(group.getDouble(columnIndex, 0));
            case DECIMAL -> {
                var binary = group.getBinary(columnIndex, 0);
                if (binary == null) {
                    yield new DoubleColumn((Double) null);
                }
                var scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)
                        group.getType().getType(columnIndex).getLogicalTypeAnnotation()).getScale();
                var bigDecimal = new BigDecimal(new BigInteger(binary.getBytes()), scale)
                        .setScale(scale, RoundingMode.HALF_UP);
                yield new DoubleColumn(bigDecimal);
            }
            case BOOLEAN -> new BoolColumn(group.getBoolean(columnIndex, 0));
            case DATE -> {
                var epoch = group.getInteger(columnIndex, 0);
                yield epoch == 0 ? new StringColumn(null) :
                        new StringColumn(LocalDate.of(1970, 1, 1).plusDays(epoch).toString());
            }
            case TIMESTAMP -> new DateColumn(new Date(getTimestampMills(group.getInt96(columnIndex, 0))));
            case BINARY -> new BytesColumn(group.getBinary(columnIndex, 0).getBytes());
            default -> {
                LOG.debug("Converting column type {} to String", columnType);
                yield new StringColumn(group.getString(columnIndex, 0));
            }
        };
    }

    /**
     * Returns GMT's timestamp from binary encoded parquet timestamp (12 bytes - julian date + time of day nanos).
     *
     * @param timestampBinary INT96 parquet timestamp
     * @return timestamp in millis, GMT timezone
     */
    public static long getTimestampMills(Binary timestampBinary)
    {
        if (timestampBinary.length() != 12) {
            return 0;
        }
        byte[] bytes = timestampBinary.getBytes();

        return getTimestampMills(bytes);
    }

    public static long getTimestampMills(byte[] bytes)
    {
        assert bytes.length == 12;
        // little endian encoding - need to invert byte order
        long timeOfDayNanos = Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
        int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);

        return julianDayToMillis(julianDay) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
    }

    private static long julianDayToMillis(int julianDay)
    {
        return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;
    }

    private JavaType getJavaType(Type field)
    {
        if (field.isPrimitive()) {
            LogicalTypeAnnotation logicalTypeAnnotation = field.asPrimitiveType().getLogicalTypeAnnotation();
            if (logicalTypeAnnotation != null) {
                String type = logicalTypeAnnotation.toString().toUpperCase();
                if (type.contains("(")) {
                    // trim ()
                    return JavaType.valueOf(type.substring(0, type.indexOf("(")));
                }
                else {
                    return JavaType.valueOf(type);
                }
            }
            return switch (field.asPrimitiveType().getPrimitiveTypeName()) {
                case INT32 -> JavaType.INT;
                case INT64 -> JavaType.LONG;
                case INT96 -> JavaType.TIMESTAMP;
                case FLOAT -> JavaType.FLOAT;
                case DOUBLE -> JavaType.DOUBLE;
                case BOOLEAN -> JavaType.BOOLEAN;
                case FIXED_LEN_BYTE_ARRAY -> JavaType.BINARY;
                default -> JavaType.STRING; //Binary as string
            };
        }
        else {
            LogicalTypeAnnotation logicalTypeAnnotation = field.getLogicalTypeAnnotation();
            if (logicalTypeAnnotation == LogicalTypeAnnotation.listType()) {
                return JavaType.ARRAY;
            }
            if (logicalTypeAnnotation == LogicalTypeAnnotation.mapType()) {
                return JavaType.MAP;
            }
            throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE, "The complex type " + logicalTypeAnnotation.toString() + " is not supported.");
        }
    }
}
