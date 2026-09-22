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
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.element.TimestampColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;

/** My Parquet Reader. */
public class MyParquetReader
{
    private static final Logger LOG = LoggerFactory.getLogger(MyParquetReader.class);

    // the offset of julian, 2440588 is 1970/1/1
    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
    private static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long MICROS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toMicros(1);

    private final MessageType schema;
    private final String nullFormat;
    private final List<ColumnPlan> plans;
    private final ParquetReader<Group> reader;

    /**
     * A column of the file with what the read loop needs for every row: the parquet field it
     * comes from and the java types of its value and of the elements of a collection.
     * <p>
     * Resolving these per record (as the raw {@code JavaType.valueOf(columnType.toUpperCase())}
     * and the schema lookups did) ran a string case conversion and a list lookup on every cell.
     *
     * @param index the column index in the file schema, -1 for a constant column
     * @param type the type the value is converted to, null for a constant column
     * @param field the parquet field the value comes from, null for a constant column
     * @param elementType the type of the elements of a list column
     * @param valueType the type of the values of a map column
     * @param constant the constant value of the column, null when it comes from the file
     */
    private record ColumnPlan(int index, JavaType type, Type field, JavaType elementType, JavaType valueType, String constant)
    {
    }

    /** Myparquetreader. */
    public MyParquetReader(org.apache.hadoop.conf.Configuration hadoopConf, Path path, String nullFormat, List<ColumnEntry> columns)
    {
        hadoopConf.set("parquet.avro.readInt96AsFixed", "true");
        this.nullFormat = nullFormat;
        // the schema is a detached copy of the footer, holding the reader open for it only
        // keeps the file handle of every file of the job alive
        try (ParquetFileReader fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(path, hadoopConf))) {
            this.schema = fileReader.getFileMetaData().getSchema();
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "Failed to open the parquet file [%s].".formatted(path), e);
        }
        this.plans = resolveColumns(path, columns);
        try {
            this.reader = ParquetReader.builder(new GroupReadSupport(), path).withConf(hadoopConf).build();
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "Failed to read the parquet file [%s].".formatted(path), e);
        }
    }

    /** Reader. */
    public void reader(RecordSender recordSender, TaskPluginCollector taskPluginCollector)
    {
        try {
            Group group = reader.read();
            while (group != null) {
                buildRecord(group, recordSender, taskPluginCollector);
                group = reader.read();
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "Failed to read the parquet file.", e);
        }
    }

    private void buildRecord(Group group, RecordSender recordSender, TaskPluginCollector taskPluginCollector)
    {
        Record record = recordSender.createRecord();
        for (ColumnPlan plan : plans) {
            Column column;
            try {
                column = getColumn(group, plan);
            }
            catch (AddaxException e) {
                throw e;
            }
            catch (Exception e) {
                // cast failed means dirty data, including number format, date format, etc. The
                // record is collected and the file keeps being read: failing here threw away every
                // record that came after the first bad one
                taskPluginCollector.collectDirtyRecord(record,
                        "Cannot convert the column at index [%d] to [%s]: %s".formatted(plan.index(), plan.type(), e.getMessage()));
                return;
            }
            record.addColumn(column);
        }
        recordSender.sendToWriter(record);
    }

    /**
     * Resolve every configured column before a single record is read. A type or an index the file
     * cannot serve fails here rather than once per row, which would collect a dirty record for
     * every record of the file and leave a job that reported success with no data behind.
     *
     * @param path the file the columns are read from, for the error messages
     * @param columns the configured columns, empty to read the columns of the file schema
     * @return the resolved columns, in configuration order
     */
    private List<ColumnPlan> resolveColumns(Path path, List<ColumnEntry> columns)
    {
        if (columns == null || columns.isEmpty()) {
            // the columns are empty or '*' in the configuration, the file schema says what to read
            List<Type> fields = schema.getFields();
            List<ColumnPlan> plans = new ArrayList<>(fields.size());
            for (int i = 0; i < fields.size(); i++) {
                Type field = fields.get(i);
                plans.add(buildPlan(i, getJavaType(field), field));
            }
            return plans;
        }

        List<ColumnPlan> plans = new ArrayList<>(columns.size());
        for (ColumnEntry columnEntry : columns) {
            String constant = columnEntry.getValue();
            if (constant != null) {
                // a constant column carries no type of its own, its value is used as it is
                plans.add(new ColumnPlan(-1, null, null, null, null, constant));
                continue;
            }
            Integer index = columnEntry.getIndex();
            if (index == null) {
                throw AddaxException.asAddaxException(CONFIG_ERROR, "The index or constant is required when type is present.");
            }
            if (index < 0 || index >= schema.getFieldCount()) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The column index [%d] is out of range, the file [%s] has [%d] columns.".formatted(index, path, schema.getFieldCount()));
            }
            String configuredType = columnEntry.getType();
            if (StringUtils.isBlank(configuredType)) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "The type of the column at index [%d] is required.".formatted(index));
            }
            JavaType type;
            try {
                type = JavaType.of(configuredType);
            }
            catch (IllegalArgumentException e) {
                throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                        "The column type [%s] of the column at index [%d] is not supported.".formatted(configuredType, index));
            }
            plans.add(buildPlan(index, type, schema.getType(index)));
        }
        return plans;
    }

    private ColumnPlan buildPlan(int index, JavaType type, Type field)
    {
        JavaType elementType = null;
        JavaType valueType = null;
        if (type == JavaType.ARRAY) {
            elementType = getJavaType(listElement(field, index));
        }
        else if (type == JavaType.MAP) {
            valueType = getJavaType(mapValue(field, index));
        }
        return new ColumnPlan(index, type, field, elementType, valueType, null);
    }

    /**
     * The parquet field the elements of a list come from.
     * <p>
     * A list is a group with a single repeated child, and the element is the child of that child
     * in the three level encoding that parquet-avro, Hive and Spark all write. The deprecated two
     * level encoding repeats the element itself and cannot be read as a group.
     *
     * @param field the schema of the list column
     * @param index the index of the column, for the error messages
     * @return the schema of an element
     */
    private Type listElement(Type field, int index)
    {
        Type repeated = field.asGroupType().getType(0);
        if (repeated.isPrimitive()) {
            throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                    "The two level list encoding of the column at index [%d] is not supported, only the three level one is.".formatted(index));
        }
        return primitiveValue(repeated, index);
    }

    /**
     * The value field of the repeated group of a collection, its only field.
     *
     * @param repeated the repeated group of the collection
     * @param index the index of the column, for the error messages
     * @return the schema of the value
     */
    private Type primitiveValue(Type repeated, int index)
    {
        Type value = repeated.asGroupType().getType(0);
        if (!value.isPrimitive()) {
            throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                    "The nested type [%s] of the column at index [%d] is not supported.".formatted(value.getName(), index));
        }
        return value;
    }

    /**
     * The parquet field the values of a map come from.
     * <p>
     * A map is a group with a single repeated key/value child, whose first field is the key and
     * whose second field is the value.
     *
     * @param field the schema of the map column
     * @param index the index of the column, for the error messages
     * @return the schema of a value
     */
    private Type mapValue(Type field, int index)
    {
        Type keyValue = field.asGroupType().getType(0).asGroupType().getType(1);
        if (!keyValue.isPrimitive()) {
            throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                    "The nested type [%s] of the column at index [%d] is not supported.".formatted(keyValue.getName(), index));
        }
        return keyValue;
    }

    private Column getColumn(Group group, ColumnPlan plan)
    {
        if (plan.constant() != null) {
            return new StringColumn(plan.constant());
        }

        // the field is absent from this record, that is a null value and not the null format
        // text of the job
        if (plan.field().getRepetition() == Type.Repetition.OPTIONAL
                && group.getFieldRepetitionCount(plan.index()) == 0) {
            return new StringColumn();
        }

        return switch (plan.type()) {
            case ARRAY -> getArrayColumn(group, plan);
            case MAP -> getMapColumn(group, plan);
            default -> getPrimitiveColumn(group, plan.type(), plan.index(), 0);
        };
    }

    /**
     * Render a list column as a JSON array.
     * <p>
     * The elements are read from the schema, not from the first value of the row: a list of
     * strings holds binary values whose java type is not a {@link JavaType} name, and an empty
     * list has no element to ask at all.
     *
     * @param group the record
     * @param plan the column to read
     * @return the value of the column
     */
    private Column getArrayColumn(Group group, ColumnPlan plan)
    {
        Group list = group.getGroup(plan.index(), 0);
        int size = list.getFieldRepetitionCount(0);
        JSONArray array = new JSONArray(size);
        for (int i = 0; i < size; i++) {
            // the array in parquet is represented as a repeated group
            array.add(getPrimitiveColumn(list.getGroup(0, i), plan.elementType(), 0, 0).getRawData());
        }
        return new StringColumn(array.toString());
    }

    /**
     * Render a map column as a JSON object.
     *
     * @param group the record
     * @param plan the column to read
     * @return the value of the column
     */
    private Column getMapColumn(Group group, ColumnPlan plan)
    {
        Group map = group.getGroup(plan.index(), 0);
        int size = map.getFieldRepetitionCount(0);
        JSONObject object = new JSONObject(size);
        for (int i = 0; i < size; i++) {
            Group keyValue = map.getGroup(0, i);
            String key = keyValue.getString(0, 0);
            object.put(key, getPrimitiveColumn(keyValue, plan.valueType(), 1, 0).getRawData());
        }
        return new StringColumn(object.toString());
    }

    private Column getPrimitiveColumn(Group group, JavaType type, int fieldIndex, int valueIndex)
    {
        return switch (type) {
            case STRING, VARCHAR, CHAR -> new StringColumn(group.getString(fieldIndex, valueIndex));
            case TINYINT, SMALLINT, INT, INTEGER -> new LongColumn(group.getInteger(fieldIndex, valueIndex));
            case BIGINT, LONG -> new LongColumn(group.getLong(fieldIndex, valueIndex));
            case FLOAT -> new DoubleColumn(group.getFloat(fieldIndex, valueIndex));
            case DOUBLE -> new DoubleColumn(group.getDouble(fieldIndex, valueIndex));
            case BOOLEAN -> new BoolColumn(group.getBoolean(fieldIndex, valueIndex));
            case DECIMAL -> decimalColumn(group, fieldIndex, valueIndex);
            case DATE -> new StringColumn(LocalDate.ofEpochDay(group.getInteger(fieldIndex, valueIndex)).toString());
            case TIMESTAMP -> new TimestampColumn(timestampMillis(group, fieldIndex, valueIndex));
            case BINARY -> new BytesColumn(group.getBinary(fieldIndex, valueIndex).getBytes());
            default -> {
                LOG.debug("Converting column type {} to String", type);
                yield new StringColumn(group.getString(fieldIndex, valueIndex));
            }
        };
    }

    /**
     * The value of a decimal field as a {@link BigDecimal}.
     * <p>
     * The unscaled value is a fixed length byte array in the files Hive and addax write, and the
     * int32 or int64 that is wide enough for its precision in the ones Spark and Arrow write.
     * Reading the latter two as a byte array threw, which made their decimals unreadable.
     *
     * @param group the record
     * @param fieldIndex the field of the value in the group
     * @param valueIndex the value of the field, used by the repeated groups of a collection
     * @return the column holding the decimal
     */
    private Column decimalColumn(Group group, int fieldIndex, int valueIndex)
    {
        Type field = group.getType().getType(fieldIndex);
        var annotation = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) field.getLogicalTypeAnnotation();
        BigInteger unscaled;
        if (field.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32) {
            unscaled = BigInteger.valueOf(group.getInteger(fieldIndex, valueIndex));
        }
        else if (field.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64) {
            unscaled = BigInteger.valueOf(group.getLong(fieldIndex, valueIndex));
        }
        else {
            Binary binary = group.getBinary(fieldIndex, valueIndex);
            if (binary == null) {
                return new DoubleColumn();
            }
            unscaled = new BigInteger(binary.getBytes());
        }
        return new DoubleColumn(new BigDecimal(unscaled, annotation.getScale()));
    }

    /**
     * The value of a timestamp field as epoch milliseconds.
     * <p>
     * INT96 is the encoding addax and Hive write, an INT64 carrying a MILLIS, MICROS or NANOS
     * logical type is what Spark and Arrow write. Reading the latter with {@code getInt96} threw,
     * so a parquet file written by another engine could not be read at all.
     *
     * @param group the record
     * @param fieldIndex the field of the value in the group
     * @param valueIndex the value of the field, used by the repeated groups of a collection
     * @return the timestamp in milliseconds
     */
    private static long timestampMillis(Group group, int fieldIndex, int valueIndex)
    {
        Type field = group.getType().getType(fieldIndex);
        if (field.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
            return getTimestampMills(group.getInt96(fieldIndex, valueIndex));
        }
        long value = group.getLong(fieldIndex, valueIndex);
        if (field.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestamp) {
            return switch (timestamp.getUnit()) {
                case MILLIS -> value;
                case MICROS -> Math.floorDiv(value, MICROS_PER_MILLISECOND);
                case NANOS -> Math.floorDiv(value, NANOS_PER_MILLISECOND);
            };
        }
        return value;
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

    /** Returns the timestampmills. */
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

    /**
     * The java type a parquet field is read as.
     *
     * @param field the schema of the field
     * @return the java type of the field
     */
    private JavaType getJavaType(Type field)
    {
        if (field.isPrimitive()) {
            LogicalTypeAnnotation logicalTypeAnnotation = field.asPrimitiveType().getLogicalTypeAnnotation();
            if (logicalTypeAnnotation != null) {
                String type = logicalTypeAnnotation.toString().toUpperCase(Locale.ROOT);
                if (type.contains("(")) {
                    // trim (), a decimal or a timestamp carries its parameters in the annotation
                    type = type.substring(0, type.indexOf("("));
                }
                try {
                    return JavaType.valueOf(type);
                }
                catch (IllegalArgumentException e) {
                    // enums, json, uuid and bson are all backed by binary, they read as strings
                    return JavaType.STRING;
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
            throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE, "The complex type " + logicalTypeAnnotation + " is not supported.");
        }
    }
}
