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
import com.wgzhao.addax.core.spi.ErrorCode;
import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericData;
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
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.plugin.reader.hdfsreader.JavaType.ARRAY;
import static com.wgzhao.addax.plugin.reader.hdfsreader.JavaType.MAP;

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
            this.reader = ParquetReader.builder(new GroupReadSupport(), path)
                    .withConf(hadoopConf)
                    .build();
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
                    if (e instanceof AddaxException) {
                        throw (AddaxException) e;
                    }
                    // cast failed means dirty data, including number format, date format, etc.
                    taskPluginCollector.collectDirtyRecord(record, e.getMessage());
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
        String columnValue;
        if (columnConst != null) {
            return new StringColumn(columnConst);
        }

        if (schema.getFields().get(columnIndex).getRepetition() == Type.Repetition.OPTIONAL
                && group.getFieldRepetitionCount(columnIndex) == 0) {
            return new StringColumn(nullFormat);
        }

        JavaType type = JavaType.valueOf(columnType.toUpperCase());
        if (type == ARRAY) {
            // Convert array to JSON string
            // current array group
            Group arrayGroup = group.getGroup(columnIndex, 0);
            int listSize = arrayGroup.getFieldRepetitionCount(0);
            // all elements in the array has the same data type
            String eleType = arrayGroup.getGroup(0, 0).getType().getFields().get(0).asPrimitiveType().getName();
            Type arrayType = schema.getFields().get(columnIndex).asGroupType().getType(0);
            JSONArray jsonArray = new JSONArray();
            for (int i = 0; i < listSize; i++) {
                // Try to get as string first, fallback to binary if needed
                // the array in parquet is represented as a repeated group
                Group elementGroup = arrayGroup.getGroup(0, i);
//                        String arrayElementValue = group.getValueToString(columnIndex, i);
                JavaType name = JavaType.valueOf(elementGroup.getType().getType(0).asPrimitiveType().getPrimitiveTypeName().javaType.getName().toUpperCase());
                Column primitiveColumn = getPrimitiveColumn(elementGroup, columnEntry, name, 0, columnType);
                jsonArray.add(primitiveColumn.asString());
            }
            columnGenerated = new StringColumn(jsonArray.toString());
        } else if (type == MAP) {
            // Convert map to JSON string
            int mapRepetitionCount = group.getFieldRepetitionCount(columnIndex);
            JSONObject jsonObject = new JSONObject();
            // Map in Parquet is represented as a repeated group of key-value pairs
            // This is a simplified approach assuming the map's key and value elements are accessible
            try {
                Group mapGroup = group.getGroup(columnIndex, 0);
                for (int i = 0; i < mapGroup.getType().getFieldCount(); i++) {
                    String key = mapGroup.getType().getFieldName(i);
                    String value = mapGroup.getValueToString(i, 0);
                    jsonObject.put(key, value);
                }
            }
            catch (Exception e) {
                LOG.warn("Failed to convert MAP type, using empty JSON object: {}", e.getMessage());
            }
            columnGenerated = new StringColumn(jsonObject.toString());
        }
         else {
            try {
                columnGenerated = getPrimitiveColumn(group, columnEntry, type, columnIndex, columnType);
            }
            catch (Exception e) {
                throw new IllegalArgumentException(String.format(
                        "Can not convert column type %s to %s: %s", columnType, type, e));
            }
        }
        return columnGenerated;
    }

    private static @NotNull Column getPrimitiveColumn(Group group, ColumnEntry columnEntry, JavaType type, Integer columnIndex, String columnType)
    {
        String columnValue;
        Column columnGenerated;
        switch (type) {
            case STRING:
                columnGenerated = new StringColumn(group.getString(columnIndex, 0));
                break;
            case INT:
                columnGenerated = new LongColumn(group.getInteger(columnIndex, 0));
                break;
            case LONG:
                columnGenerated = new LongColumn(group.getLong(columnIndex, 0));
                break;
            case FLOAT:
                columnGenerated = new DoubleColumn(group.getFloat(columnIndex, 0));
                break;
            case DOUBLE:
                columnGenerated = new DoubleColumn(group.getDouble(columnIndex, 0));
                break;
            case DECIMAL:
                // get decimal value
                columnValue = group.getString(columnIndex, 0);
                if (null == columnValue) {
                    columnGenerated = new DoubleColumn((Double) null);
                }
                else {
                    columnGenerated = new DoubleColumn(new BigDecimal(columnValue).setScale(10, RoundingMode.HALF_UP));
                }
                break;
            case BOOLEAN:
                columnGenerated = new BoolColumn(group.getBoolean(columnIndex, 0));
                break;
            case DATE:
                columnValue = group.getString(columnIndex, 0);
                if (columnValue == null) {
                    columnGenerated = new DateColumn((Date) null);
                }
                else {
                    String formatString = columnEntry.getFormat();
                    if (StringUtils.isNotBlank(formatString)) {
                        // 用户自己配置的格式转换
                        SimpleDateFormat format = new SimpleDateFormat(formatString);
                        try {
                            columnGenerated = new DateColumn(format.parse(columnValue));
                        }
                        catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    else {
                        // 框架尝试转换
                        columnGenerated = new DateColumn(new StringColumn(columnValue).asDate());
                    }
                }
                break;
            case TIMESTAMP:
                Binary binaryTs = group.getInt96(columnIndex, 0);
                columnGenerated = new DateColumn(new Date(getTimestampMills(binaryTs)));
                break;
            case BINARY:
                columnGenerated = new BytesColumn(group.getBinary(columnIndex, 0).getBytes());
                break;
            default:
                // try to convert it to string
                LOG.debug("try to convert column type {} to String, ", columnType);
                columnGenerated = new StringColumn(group.getString(columnIndex, 0));
        }
        return columnGenerated;
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
            switch (field.asPrimitiveType().getPrimitiveTypeName()) {
                case INT32:
                    return JavaType.INT;
                case INT64:
                    return JavaType.LONG;
                case INT96:
                    return JavaType.TIMESTAMP;
                case FLOAT:
                    return JavaType.FLOAT;
                case DOUBLE:
                    return JavaType.DOUBLE;
                case BOOLEAN:
                    return JavaType.BOOLEAN;
                case FIXED_LEN_BYTE_ARRAY:
                    return JavaType.BINARY;
                default:
                    return JavaType.STRING; //Binary as string
            }
        }
        else {
//            return JavaType.STRING;
            LogicalTypeAnnotation logicalTypeAnnotation = field.getLogicalTypeAnnotation();
            if (logicalTypeAnnotation == LogicalTypeAnnotation.listType()) {
                return ARRAY;
            }
            if (logicalTypeAnnotation == LogicalTypeAnnotation.mapType()) {
                return MAP;
            }
            throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE, "The complex type " + logicalTypeAnnotation.toString() + " is not supported.");
        }
    }
}
