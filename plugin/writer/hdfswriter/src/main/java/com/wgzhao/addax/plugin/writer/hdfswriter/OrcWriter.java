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
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

public class OrcWriter
        extends HdfsHelper
        implements IHDFSWriter
{
    private static final Logger logger = LoggerFactory.getLogger(OrcWriter.class);
    private static final int DEFAULT_BATCH_SIZE = 1024;
    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));
    // the type of the element in the array
    private SupportHiveDataType elementType;
    // the type of the value in the map
    private SupportHiveDataType valueType;

    public OrcWriter(Configuration conf)
    {
        super();
        getFileSystem(conf);
    }

    /**
     * write an orc record
     *
     * @param batch {@link VectorizedRowBatch}
     * @param row row number
     * @param record {@link Record}
     * @param columns table columns, {@link List}
     * @param taskPluginCollector {@link TaskPluginCollector}
     */
    private void setRow(VectorizedRowBatch batch, int row, Record record, List<Configuration> columns,
            TaskPluginCollector taskPluginCollector)
    {
        for (int i = 0; i < columns.size(); i++) {
            Configuration eachColumnConf = columns.get(i);
            String type = eachColumnConf.getString(Key.TYPE).trim().toUpperCase();
            ColumnVector col = batch.cols[i];

            // Handle null values
            Column recordColumn = record.getColumn(i);
            if (recordColumn == null || recordColumn.getRawData() == null) {
                col.isNull[row] = true;
                col.noNulls = false;
                continue;
            }

            if (type.startsWith("ARRAY")) {
                appendArrayValue(row, recordColumn, (ListColumnVector) col);
                continue;
            }

            if (type.startsWith("MAP")) {
                appendMapValue(row, recordColumn, (MapColumnVector) col);
                continue;
            }

            try {
                // Determine column type
                SupportHiveDataType columnType;
                if (type.startsWith("DECIMAL")) {
                    columnType = SupportHiveDataType.DECIMAL;
                }
                else {
                    try {
                        columnType = SupportHiveDataType.valueOf(type);
                    }
                    catch (IllegalArgumentException e) {
                        throw AddaxException.asAddaxException(
                                NOT_SUPPORT_TYPE,
                                String.format("Unsupported field type. Field name: [%s], Field type:[%s].",
                                        eachColumnConf.getString(Key.NAME), type));
                    }
                }

                // Set value based on column type
                appendPrimitiveColumn(row, columnType, col, recordColumn, eachColumnConf, type);
            }
            catch (Exception e) {
                taskPluginCollector.collectDirtyRecord(record, e.getMessage());
                throw AddaxException.asAddaxException(RUNTIME_ERROR,
                        String.format("Failed to set ORC row, source field type: %s, destination type: %s, " +
                                        "field name: %s, value: %s, error: %s",
                                recordColumn.getType(), type,
                                eachColumnConf.getString(Key.NAME),
                                recordColumn.getRawData(), e.getMessage()));
            }
        }
    }

    /**
     * Appends a primitive column value to the ORC file.
     *
     * @param row the row number in the batch
     * @param columnType the type of the column
     * @param col the column vector to append the value to
     * @param recordColumn the record column containing the value
     * @param eachColumnConf the configuration for the column
     * @param type the type of the column as a string
     */
    private void appendPrimitiveColumn(int row, SupportHiveDataType columnType, ColumnVector col, Column recordColumn, Configuration eachColumnConf, String type)
    {
        switch (columnType) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case BOOLEAN:
                ((LongColumnVector) col).vector[row] = recordColumn.asLong();
                break;
            case DATE:
                java.sql.Date sqlDate = new java.sql.Date(recordColumn.asDate().getTime());
                ((LongColumnVector) col).vector[row] = sqlDate.toLocalDate().toEpochDay();
                break;
            case FLOAT:
            case DOUBLE:
                ((DoubleColumnVector) col).vector[row] = recordColumn.asDouble();
                break;
            case DECIMAL:
                int scale = eachColumnConf.getInt(Key.SCALE, Constant.DEFAULT_DECIMAL_MAX_SCALE);
                HiveDecimalWritable hdw = new HiveDecimalWritable();
                hdw.set(HiveDecimal.create(recordColumn.asBigDecimal())
                        .setScale(scale, HiveDecimal.ROUND_HALF_UP));
                ((DecimalColumnVector) col).set(row, hdw);
                break;
            case TIMESTAMP:
                ((TimestampColumnVector) col).set(row, recordColumn.asTimestamp());
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                setStringValue(col, row, recordColumn);
                break;
            case BINARY:
                byte[] content = (byte[]) recordColumn.getRawData();
                ((BytesColumnVector) col).setRef(row, content, 0, content.length);
                break;
            default:
                throw AddaxException.asAddaxException(
                        NOT_SUPPORT_TYPE,
                        String.format("Unsupported field type. Field name: [%s], Field type:[%s].",
                                eachColumnConf.getString(Key.NAME), type));
        }
    }

    /**
     * Appends an array value to the ORC file.
     *
     * @param row the row number in the batch
     * @param recordColumn the record column containing the array value
     * @param col the column vector to append the array value to
     */
    private void appendArrayValue(int row, Column recordColumn, ListColumnVector col)
    {
        // "['value1','value2'] ,convert the string to a list of V
        String arrayString = recordColumn.asString();
        JSONArray jsonArray = JSONArray.parseArray(arrayString);
        col.offsets[row] = col.childCount;
        col.lengths[row] = jsonArray.size();

        for (Object o : jsonArray) {
            if (o == null) {
                col.child.isNull[col.childCount] = true;
                col.child.noNulls = false;
                col.childCount++;
                continue;
            }
            appendPrimitiveColumn(col.childCount, elementType, col.child, new StringColumn(o.toString()), null, elementType.toString());

            col.childCount++;
        }
    }

    /**
     * Appends an array value to the ORC file.
     *
     * @param row the row number in the batch
     * @param recordColumn the record column containing the array value
     * @param col the column vector to append the array value to
     */
    private void appendMapValue(int row, Column recordColumn, MapColumnVector col)
    {
        // assume the column is a map of V or the string of map of V
        // {key1:value1,key2:value2}
        String mapString = recordColumn.asString();
        JSONObject jsonObject = JSONObject.parseObject(mapString);
        // convert the string to a map of V
        col.offsets[row] = col.childCount;
        col.lengths[row] = jsonObject.size();
        // The key in map must be a string type
        BytesColumnVector mapKeyVector = (BytesColumnVector) col.keys;
        ColumnVector mapValueVector = col.values;
        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);

            mapKeyVector.setRef(col.childCount, keyBytes, 0, keyBytes.length);

            Object value = entry.getValue();

            if (value == null) {
                mapValueVector.isNull[col.childCount] = true;
                mapValueVector.noNulls = false;
            }
            else {
                col.values.isNull[col.childCount] = false;
                appendPrimitiveColumn(col.childCount, valueType, mapValueVector, new StringColumn(value.toString()), null, valueType.toString());
            }
            col.childCount++;
        }
    }

    /**
     * Sets the string value for a column in the ORC file.
     *
     * @param col the column vector to set the value for
     * @param row the row number in the batch
     * @param column the column containing the value
     */
    private void setStringValue(ColumnVector col, int row, Column column)
    {
        byte[] buffer;
        Column.Type colType = column.getType();

        if (colType == Column.Type.BYTES) {
            buffer = Base64.getEncoder().encode((byte[]) column.getRawData());
        }
        else if (colType == Column.Type.DATE) {
            if (((DateColumn) column).getSubType() == DateColumn.DateType.TIME) {
                buffer = column.asString().getBytes(StandardCharsets.UTF_8);
            }
            else {
                buffer = DATE_FORMAT.get().format(column.asDate()).getBytes(StandardCharsets.UTF_8);
            }
        }
        else {
            buffer = column.getRawData().toString().getBytes(StandardCharsets.UTF_8);
        }

        ((BytesColumnVector) col).setRef(row, buffer, 0, buffer.length);
    }

    @Override
    public void write(RecordReceiver lineReceiver, Configuration config, String fileName,
            TaskPluginCollector taskPluginCollector)
    {
        List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, "NONE").toUpperCase();
        int batchSize = config.getInt(Key.BATCH_SIZE, DEFAULT_BATCH_SIZE);

        TypeDescription schema = buildOrcSchema(columns);
        Path filePath = new Path(fileName);

        try (Writer writer = OrcFile.createWriter(filePath,
                OrcFile.writerOptions(conf)
                        .setSchema(schema)
                        .compress(CompressionKind.valueOf(compress)))) {

            Record record;
            VectorizedRowBatch batch = schema.createRowBatch(batchSize);

            while ((record = lineReceiver.getFromReader()) != null) {
                int row = batch.size++;
                setRow(batch, row, record, columns, taskPluginCollector);

                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }

            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        catch (IOException e) {
            logger.error("IO exception occurred while writing file [{}]: {}", fileName, e.getMessage());
            deleteDir(filePath.getParent());
            throw AddaxException.asAddaxException(IO_ERROR, e);
        }
    }

    /**
     * Builds the ORC schema based on the provided column configurations.
     *
     * @param columns the list of column configurations
     * @return the ORC schema as a TypeDescription object
     */
    private TypeDescription buildOrcSchema(List<Configuration> columns)
    {
        TypeDescription schema = TypeDescription.createStruct().setAttribute("creator", "addax");

        for (Configuration column : columns) {
            String typeName = column.getString(Key.TYPE).toLowerCase();
            String fieldName = column.getString(Key.NAME);

            if ("decimal".equalsIgnoreCase(typeName)) {
                int precision = column.getInt(Key.PRECISION, Constant.DEFAULT_DECIMAL_MAX_PRECISION);
                int scale = column.getInt(Key.SCALE, Constant.DEFAULT_DECIMAL_MAX_SCALE);
                schema.addField(fieldName, TypeDescription.createDecimal().withPrecision(precision).withScale(scale));
            }
            else if (typeName.startsWith("array")) {
                String elementType = typeName.substring(typeName.indexOf("<") + 1, typeName.indexOf(">"));
                TypeDescription elementTypeDesc = TypeDescription.fromString(elementType);
                this.elementType = SupportHiveDataType.valueOf(elementType.toUpperCase());
                schema.addField(fieldName, TypeDescription.createList(elementTypeDesc));
            }
            else if (typeName.startsWith("map")) {
                String keyValueType = typeName.substring(typeName.indexOf("<") + 1, typeName.indexOf(">"));
                String[] keyValueTypes = keyValueType.split(",");
                TypeDescription keyTypeDesc = TypeDescription.fromString(keyValueTypes[0]);
                TypeDescription valueTypeDesc = TypeDescription.fromString(keyValueTypes[1].trim());
                this.valueType = SupportHiveDataType.valueOf(keyValueTypes[1].trim().toUpperCase());
                schema.addField(fieldName, TypeDescription.createMap(keyTypeDesc, valueTypeDesc));
            }
            else {
                schema.addField(fieldName, TypeDescription.fromString(typeName));
            }
        }

        return schema;
    }
}