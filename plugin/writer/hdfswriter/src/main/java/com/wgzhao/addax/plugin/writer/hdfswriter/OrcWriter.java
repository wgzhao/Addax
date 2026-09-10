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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

/** Orc Writer. */
public class OrcWriter
        extends HdfsHelper
        implements IHDFSWriter
{
    private static final Logger logger = LoggerFactory.getLogger(OrcWriter.class);
    private static final int DEFAULT_BATCH_SIZE = 1024;
    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));
    private static final ThreadLocal<SimpleDateFormat> TIMESTAMP_FORMAT =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

    /** Whether a configured column holds a single value, an array or a map. */
    private enum ColumnKind
    {
        SCALAR, ARRAY, MAP
    }

    /**
     * The resolved shape of one configured column.
     * <p>
     * Resolved while the schema is built so that the per-record path does not re-read and re-parse
     * the configuration for every field of every record: each lookup walks the configuration path
     * and allocates several short-lived objects on the way.
     *
     * @param kind whether the column holds a single value, an array or a map
     * @param name the field name, for diagnostics
     * @param type the scalar type, or the element/value type of a collection
     * @param scale the decimal scale, meaningful for DECIMAL
     */
    private record ColumnPlan(ColumnKind kind, String name, SupportHiveDataType type, int scale)
    {
    }

    /**
     * The ORC schema together with the resolved plan of every configured column.
     *
     * @param schema the ORC schema
     * @param columns the resolved columns, in the configured order
     */
    private record OrcLayout(TypeDescription schema, List<ColumnPlan> columns)
    {
    }

    /** Orcwriter. */
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
     * @param layout the schema and the resolved column plans
     */
    private void setRow(VectorizedRowBatch batch, int row, Record record, List<Configuration> columns,
            TaskPluginCollector taskPluginCollector, OrcLayout layout)
    {
        for (int i = 0; i < columns.size(); i++) {
            ColumnPlan plan = layout.columns().get(i);
            ColumnVector col = batch.cols[i];

            // Handle null values
            Column recordColumn = record.getColumn(i);
            if (recordColumn == null || recordColumn.getRawData() == null) {
                col.isNull[row] = true;
                col.noNulls = false;
                continue;
            }

            // the collection branches share the try below as well: outside it their failures
            // escaped as a raw ClassCastException from the middle of a batch
            try {
                switch (plan.kind()) {
                    case ARRAY -> appendArrayValue(row, recordColumn, (ListColumnVector) col, plan);
                    case MAP -> appendMapValue(row, recordColumn, (MapColumnVector) col, plan);
                    case SCALAR -> appendPrimitiveColumn(row, plan.type(), plan.scale(), plan.name(),
                            col, recordColumn);
                }
            }
            catch (Exception e) {
                taskPluginCollector.collectDirtyRecord(record, e.getMessage());
                throw AddaxException.asAddaxException(RUNTIME_ERROR,
                        String.format("Failed to set ORC row, source field type: %s, destination type: %s, " +
                                        "field name: %s, value: %s, error: %s",
                                recordColumn.getType(), plan.type(), plan.name(),
                                recordColumn.getRawData(), e.getMessage()));
            }
        }
    }

    /**
     * Appends a primitive value to an ORC column vector.
     *
     * @param row the row number in the batch
     * @param columnType the type of the value
     * @param scale the decimal scale, used by DECIMAL only
     * @param fieldName the field name, used by the error message only
     * @param col the column vector to append the value to
     * @param recordColumn the record column holding the value
     */
    private void appendPrimitiveColumn(int row, SupportHiveDataType columnType, int scale, String fieldName,
            ColumnVector col, Column recordColumn)
    {
        switch (columnType) {
            case TINYINT, SMALLINT, INT, BIGINT, BOOLEAN -> ((LongColumnVector) col).vector[row] = recordColumn.asLong();
            case DATE -> {
                java.sql.Date sqlDate = new java.sql.Date(recordColumn.asDate().getTime());
                ((LongColumnVector) col).vector[row] = sqlDate.toLocalDate().toEpochDay();
            }
            case FLOAT, DOUBLE -> ((DoubleColumnVector) col).vector[row] = recordColumn.asDouble();
            case DECIMAL -> {
                HiveDecimalWritable hdw = new HiveDecimalWritable();
                hdw.set(HiveDecimal.create(recordColumn.asBigDecimal())
                        .setScale(scale, HiveDecimal.ROUND_HALF_UP));
                ((DecimalColumnVector) col).set(row, hdw);
            }
            case TIMESTAMP -> ((TimestampColumnVector) col).set(row, recordColumn.asTimestamp());
            case STRING, VARCHAR, CHAR -> setStringValue(col, row, recordColumn);
            case BINARY -> {
                byte[] content = (byte[]) recordColumn.getRawData();
                ((BytesColumnVector) col).setRef(row, content, 0, content.length);
            }
            default -> throw AddaxException.asAddaxException(
                    NOT_SUPPORT_TYPE,
                    String.format("Unsupported field type. Field name: [%s], Field type:[%s].",
                            fieldName, columnType));
        }
    }

    /**
     * Appends an array value to the ORC file.
     *
     * @param row the row number in the batch
     * @param recordColumn the record column containing the array value
     * @param col the column vector to append the array value to
     * @param plan the resolved plan of this column, whose type is the element type
     */
    private void appendArrayValue(int row, Column recordColumn, ListColumnVector col, ColumnPlan plan)
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
            appendPrimitiveColumn(col.childCount, plan.type(), plan.scale(), plan.name(), col.child,
                    new StringColumn(o.toString()));

            col.childCount++;
        }
    }

    /**
     * Appends a map value to the ORC file.
     *
     * @param row the row number in the batch
     * @param recordColumn the record column containing the map value
     * @param col the column vector to append the map value to
     * @param plan the resolved plan of this column, whose type is the value type
     */
    private void appendMapValue(int row, Column recordColumn, MapColumnVector col, ColumnPlan plan)
    {
        // assume the column is a map of V or the string of map of V
        // {key1:value1,key2:value2}
        String mapString = recordColumn.asString();
        JSONObject jsonObject = JSONObject.parseObject(mapString);
        // convert the string to a map of V
        col.offsets[row] = col.childCount;
        col.lengths[row] = jsonObject.size();
        // buildOrcSchema rejects every other key type, so this cast cannot fail here
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
                appendPrimitiveColumn(col.childCount, plan.type(), plan.scale(), plan.name(), mapValueVector,
                        new StringColumn(value.toString()));
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
                buffer = formatTimeWithNanos(column, columnTimeZone).getBytes(StandardCharsets.UTF_8);
            }
            else {
                buffer = DATE_FORMAT.get().format(column.asDate()).getBytes(StandardCharsets.UTF_8);
            }
        }
        else if (column.getRawData() instanceof java.sql.Timestamp ts) {
            buffer = TIMESTAMP_FORMAT.get().format(ts).getBytes(StandardCharsets.UTF_8);
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
        String compress = config.getString(Key.COMPRESS, "NONE").toUpperCase(Locale.ROOT).trim();
        int batchSize = config.getInt(Key.BATCH_SIZE, DEFAULT_BATCH_SIZE);

        OrcLayout layout = buildOrcSchema(columns);
        TypeDescription schema = layout.schema();
        Path filePath = new Path(fileName);
        org.apache.orc.OrcFile.WriterOptions writerOptions =
                buildWriterOptions(conf, config, schema, columns, compress);

        try (Writer writer = OrcFile.createWriter(filePath, writerOptions)) {

            Record record;
            VectorizedRowBatch batch = schema.createRowBatch(batchSize);

            while ((record = lineReceiver.getFromReader()) != null) {
                int row = batch.size++;
                setRow(batch, row, record, columns, taskPluginCollector, layout);

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
            // no per-task cleanup here: the parent is the staging directory shared by every split
            // task, and Job.destroy() removes the whole staging directory on failure anyway
            throw AddaxException.asAddaxException(IO_ERROR, e);
        }
    }

    /**
     * Translate a configured type into the spelling ORC's parser understands.
     * <p>
     * The configuration accepts the SQL aliases ({@code integer}, {@code long}) that the rest of
     * the plugin also accepts, but {@code TypeDescription.fromString} only knows {@code int} and
     * {@code bigint} and aborted the task before a single record was read.
     *
     * @param type the configured type, lower case and trimmed
     * @return the ORC type name
     */
    private static String toOrcTypeName(String type)
    {
        return switch (type) {
            case "integer" -> "int";
            case "long" -> "bigint";
            default -> type;
        };
    }

    /**
     * Reject a map key type the writer cannot produce.
     * <p>
     * The key is always read out of a JSON object, so it is a string, and {@code appendMapValue}
     * writes it through a bytes vector. Any other key type used to be accepted here and then fail
     * with a ClassCastException in the middle of a batch, after the file had been opened.
     *
     * @param fieldName the field being built, for the error message
     * @param keyType the configured key type
     */
    private static void validateMapKeyType(String fieldName, String keyType)
    {
        SupportHiveDataType type = SupportHiveDataType.of(keyType);
        if (type != SupportHiveDataType.STRING && type != SupportHiveDataType.VARCHAR
                && type != SupportHiveDataType.CHAR) {
            throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                    String.format("The key of the map field [%s] must be a string type, but got [%s].",
                            fieldName, keyType.trim()));
        }
    }

    /**
     * Builds the ORC schema based on the provided column configurations.
     *
     * @param columns the list of column configurations
     * @return the ORC schema along with the plan resolved for every column
     */
    private OrcLayout buildOrcSchema(List<Configuration> columns)
    {
        TypeDescription schema = TypeDescription.createStruct().setAttribute("creator", "addax");
        List<ColumnPlan> plans = new ArrayList<>(columns.size());

        for (Configuration column : columns) {
            String typeName = column.getString(Key.TYPE).toLowerCase(Locale.ROOT);
            String fieldName = column.getString(Key.NAME);
            int scale = column.getInt(Key.SCALE, Constant.DEFAULT_DECIMAL_MAX_SCALE);
            ColumnKind kind = ColumnKind.SCALAR;
            SupportHiveDataType type;

            if ("decimal".equalsIgnoreCase(typeName)) {
                int precision = column.getInt(Key.PRECISION, Constant.DEFAULT_DECIMAL_MAX_PRECISION);
                type = SupportHiveDataType.DECIMAL;
                schema.addField(fieldName, TypeDescription.createDecimal().withScale(scale).withPrecision(precision));
            }
            else if (typeName.startsWith("array")) {
                String elementTypeName = typeName.substring(typeName.indexOf("<") + 1, typeName.lastIndexOf(">"));
                kind = ColumnKind.ARRAY;
                type = resolveFieldType(fieldName, elementTypeName);
                schema.addField(fieldName, TypeDescription.createList(
                        TypeDescription.fromString(toOrcTypeName(elementTypeName.trim()))));
            }
            else if (typeName.startsWith("map")) {
                String keyValueType = typeName.substring(typeName.indexOf("<") + 1, typeName.lastIndexOf(">"));
                // limit 2: a parameterised value type carries its own comma, e.g. map<string,decimal(10,2)>
                String[] keyValueTypes = keyValueType.split(",", 2);
                validateMapKeyType(fieldName, keyValueTypes[0]);
                kind = ColumnKind.MAP;
                type = resolveFieldType(fieldName, keyValueTypes[1]);
                schema.addField(fieldName, TypeDescription.createMap(
                        TypeDescription.fromString(toOrcTypeName(keyValueTypes[0].trim())),
                        TypeDescription.fromString(toOrcTypeName(keyValueTypes[1].trim()))));
            }
            else {
                type = resolveFieldType(fieldName, typeName);
                schema.addField(fieldName, TypeDescription.fromString(toOrcTypeName(typeName.trim())));
            }

            plans.add(new ColumnPlan(kind, fieldName, type, scale));
        }
        return new OrcLayout(schema, plans);
    }

    /**
     * Resolve a configured type name to a constant the value writers can switch on.
     *
     * @param fieldName the field being built, for the error message
     * @param typeName the configured type
     * @return the resolved constant
     */
    private static SupportHiveDataType resolveFieldType(String fieldName, String typeName)
    {
        try {
            return SupportHiveDataType.of(typeName);
        }
        catch (IllegalArgumentException e) {
            throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                    String.format("Unsupported field type. Field name: [%s], Field type:[%s].",
                            fieldName, typeName.trim().toUpperCase(Locale.ROOT)));
        }
    }

    private org.apache.orc.OrcFile.WriterOptions buildWriterOptions(org.apache.hadoop.conf.Configuration hadoopConf,
            Configuration config, TypeDescription schema, List<Configuration> columns, String compress)
    {
        org.apache.orc.OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(hadoopConf)
                .setSchema(schema)
                .compress(CompressionKind.valueOf(compress));

        BloomFilterConfig bloomFilterConfig = resolveBloomFilterConfiguration(config, columns);
        if (bloomFilterConfig != null) {
            writerOptions.bloomFilterColumns(bloomFilterConfig.columns())
                    .bloomFilterFpp(bloomFilterConfig.fpp());
        }

        return writerOptions;
    }
}
