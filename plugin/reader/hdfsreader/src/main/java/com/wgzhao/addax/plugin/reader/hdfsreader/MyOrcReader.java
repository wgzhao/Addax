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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;

/** My Orc Reader. */
public class MyOrcReader
{
    private static final Logger LOG = LoggerFactory.getLogger(MyOrcReader.class);

    /** Rows per batch, the ORC default. A batch is decoded at once and then handed out row by row. */
    private static final int BATCH_SIZE = 1024;

    private final org.apache.hadoop.conf.Configuration hadoopConf;
    private final String nullFormat;
    private final List<ColumnEntry> columnEntries;
    private final Path path;

    /**
     * A column of the file with what the read loop needs for every row.
     * <p>
     * The type is resolved once per file: doing it per record (as a raw
     * {@code JavaType.valueOf(column.getType().toUpperCase())} did) allocated a string and ran an
     * enum lookup on every cell, and threw for the configured spellings that are not constant
     * names, for example {@code varchar(10)} or {@code array<string>}.
     *
     * @param index the column index in the row batch, -1 for a constant column
     * @param type the type the value is converted to, null for a constant column
     * @param constant the constant value of the column, null when it comes from the file
     * @param constantIsNull true when the constant is the literal {@code null}
     */
    private record ColumnPlan(int index, JavaType type, String constant, boolean constantIsNull)
    {
    }

    /** Myorcreader. */
    public MyOrcReader(org.apache.hadoop.conf.Configuration hadoopConf, Path path, String nullFormat, List<ColumnEntry> columns)
    {
        this.hadoopConf = hadoopConf;
        this.nullFormat = nullFormat;
        this.path = path;
        this.columnEntries = columns;
    }

    /** Reader. */
    public void reader(RecordSender recordSender, TaskPluginCollector taskPluginCollector)
    {
        try (Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(hadoopConf)); org.apache.orc.RecordReader rowIterator = reader.rows(reader.options().schema(reader.getSchema()))) {
            TypeDescription schema = reader.getSchema();
            fillColumnsFromSchema(schema);
            List<ColumnPlan> plans = resolveColumns();
            VectorizedRowBatch rowBatch = schema.createRowBatch(BATCH_SIZE);
            while (rowIterator.nextBatch(rowBatch)) {
                buildRecords(rowBatch, plans, recordSender, taskPluginCollector);
            }
        }
        catch (IOException e) {
            String message = "Exception occurred while reading the file [%s].".formatted(path);
            LOG.error(message, e);
            throw AddaxException.asAddaxException(IO_ERROR, message, e);
        }
    }

    /**
     * Derive the columns from the file schema when the job did not configure any, which is what
     * the {@code "*"} column marker leaves behind.
     *
     * @param schema the schema of the file
     */
    private void fillColumnsFromSchema(TypeDescription schema)
    {
        if (!columnEntries.isEmpty()) {
            return;
        }
        List<TypeDescription> children = schema.getChildren();
        for (int i = 0; i < children.size(); i++) {
            ColumnEntry columnEntry = new ColumnEntry();
            columnEntry.setIndex(i);
            columnEntry.setType(toJavaType(children.get(i)).name());
            columnEntries.add(columnEntry);
        }
    }

    /**
     * Map an ORC schema category to the type its values are read as.
     * <p>
     * The category names are not {@link JavaType} constants: {@code byte} and {@code short} are
     * spelled {@code tinyint} and {@code smallint} there, an array is a {@code list}. Looking
     * them up by name failed for every one of those columns of a file that was read without a
     * column list.
     *
     * @param type the schema of the column
     * @return the type the column is read as
     */
    private JavaType toJavaType(TypeDescription type)
    {
        return switch (type.getCategory()) {
            case BYTE -> JavaType.TINYINT;
            case SHORT -> JavaType.SMALLINT;
            case INT -> JavaType.INT;
            case LONG -> JavaType.BIGINT;
            case FLOAT -> JavaType.FLOAT;
            case DOUBLE -> JavaType.DOUBLE;
            case BOOLEAN -> JavaType.BOOLEAN;
            case STRING, CHAR, VARCHAR -> JavaType.STRING;
            case BINARY -> JavaType.BINARY;
            case DATE -> JavaType.DATE;
            case TIMESTAMP -> JavaType.TIMESTAMP;
            case DECIMAL -> JavaType.DECIMAL;
            case LIST -> JavaType.ARRAY;
            case MAP -> JavaType.MAP;
            default -> throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                    "The column type [%s] of the file [%s] is not supported.".formatted(type.getCategory().getName(), path));
        };
    }

    /**
     * Resolve every configured column before a single record is read. A type that cannot be read
     * fails here rather than once per row, which would collect a dirty record for every record of
     * the file and leave a job that reported success with no data behind.
     *
     * @return the resolved columns, in configuration order
     */
    private List<ColumnPlan> resolveColumns()
    {
        List<ColumnPlan> plans = new ArrayList<>(columnEntries.size());
        for (ColumnEntry columnEntry : columnEntries) {
            String constant = columnEntry.getValue();
            if (constant != null) {
                // a constant column carries no type of its own, its value is used as it is
                plans.add(new ColumnPlan(-1, null, constant, "null".equals(constant)));
                continue;
            }
            String configuredType = columnEntry.getType();
            if (configuredType == null) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "The type of the column at index [%d] is required.".formatted(columnEntry.getIndex()));
            }
            JavaType type;
            try {
                type = JavaType.of(configuredType);
            }
            catch (IllegalArgumentException e) {
                throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                        "The column type [%s] of the column at index [%d] is not supported.".formatted(configuredType, columnEntry.getIndex()));
            }
            plans.add(new ColumnPlan(columnEntry.getIndex(), type, null, false));
        }
        return plans;
    }

    private void buildRecords(VectorizedRowBatch rowBatch, List<ColumnPlan> plans,
            RecordSender recordSender, TaskPluginCollector taskPluginCollector)
    {
        for (int row = 0; row < rowBatch.size; row++) {
            Record record = recordSender.createRecord();
            boolean valid = true;
            for (ColumnPlan plan : plans) {
                try {
                    record.addColumn(buildColumn(rowBatch, plan, row));
                }
                catch (AddaxException e) {
                    throw e;
                }
                catch (Exception e) {
                    // a value the declared type cannot hold is a dirty record, and a dirty record is
                    // collected: a partially built record is not sent to the writer
                    valid = false;
                    taskPluginCollector.collectDirtyRecord(record,
                            "Cannot convert the column at index [%d] to [%s]: %s".formatted(plan.index(), plan.type(), e.getMessage()));
                    break;
                }
            }
            if (valid) {
                recordSender.sendToWriter(record);
            }
        }
    }

    private Column buildColumn(VectorizedRowBatch rowBatch, ColumnPlan plan, int row)
    {
        if (plan.constant() != null) {
            return new StringColumn(plan.constantIsNull() ? nullFormat : plan.constant());
        }
        ColumnVector col = rowBatch.cols[plan.index()];
        if (col.isNull[row]) {
            return new StringColumn();
        }
        return switch (plan.type()) {
            case ARRAY -> getArrayColumn(nullFormat, (ListColumnVector) col, row);
            case MAP -> getMapColumn(nullFormat, (MapColumnVector) col, row);
            default -> getPrimitiveColumn(nullFormat, plan.type(), col, row);
        };
    }

    private static @NotNull Column getMapColumn(String nullFormat, MapColumnVector col, int row)
    {
        var mapBuilder = new StringBuilder("{");
        // all value type must be same
        for (int j = (int) col.offsets[row]; j < col.offsets[row] + col.lengths[row]; j++) {
            if (j > col.offsets[row]) {
                mapBuilder.append(", ");
            }

            // The key must be string
            var key = ((BytesColumnVector) col.keys).toString(j);
            mapBuilder.append('"').append(key).append("\": ");

            var valueCol = col.values;
            if (valueCol.isNull[j]) {
                mapBuilder.append(nullFormat);
            }
            else {
                valueCol.stringifyValue(mapBuilder, j);
            }
        }
        return new StringColumn(mapBuilder.append('}').toString());
    }

    private static @NotNull Column getArrayColumn(String nullFormat, ListColumnVector col, int row)
    {
        // StringJoiner takes the CharSequence straight, a StringBuilder per element only to
        // turn it into a String again is one allocation per element of every array
        var joiner = new StringJoiner(", ");
        var element = new StringBuilder();
        for (var j = (int) col.offsets[row]; j < col.offsets[row] + col.lengths[row]; j++) {
            var childCol = col.child;
            if (childCol.isNull[j]) {
                joiner.add(nullFormat);
            }
            else {
                element.setLength(0);
                childCol.stringifyValue(element, j);
                joiner.add(element);
            }
        }
        return new StringColumn("[%s]".formatted(joiner));
    }

    private static @NotNull Column getPrimitiveColumn(String nullFormat, JavaType type, ColumnVector col, int row) {
        return switch (type) {
            // the tinyint, smallint, integer and long spellings are the same java type: an
            // integer that the hive vector holds in a long, whatever the declared width is
            case TINYINT, SMALLINT, INT, INTEGER, BIGINT, LONG ->
                new LongColumn(((LongColumnVector) col).vector[row]);
            case BOOLEAN ->
                // the value is stored in a long vector, but a boolean is not a number: reading it
                // as one turned a source true into the 1 that the parquet reader returns true for
                new BoolColumn(((LongColumnVector) col).vector[row] != 0);
            case DATE -> {
                // an epoch day is a calendar day, rendering it as an instant puts a date one
                // day off in every zone west of UTC
                var date = LocalDate.ofEpochDay(((LongColumnVector) col).vector[row]);
                yield new StringColumn(date.toString());
            }
            case FLOAT, DOUBLE ->
                new DoubleColumn(((DoubleColumnVector) col).vector[row]);
            case DECIMAL -> {
                // through the BigDecimal: a decimal does not survive a double. The scale of the
                // column is asked for rather than the scale the value happens to be stored with,
                // so that the value renders the way the schema of the file describes it
                var decimalCol = (DecimalColumnVector) col;
                var decimal = decimalCol.vector[row].getHiveDecimal(decimalCol.precision, decimalCol.scale);
                yield decimal == null ? new StringColumn() : new DoubleColumn(decimal.bigDecimalValue());
            }
            case BINARY -> {
                var b = (BytesColumnVector) col;
                var val = Arrays.copyOfRange(b.vector[row], b.start[row], b.start[row] + b.length[row]);
                yield new BytesColumn(val);
            }
            case TIMESTAMP ->
                new TimestampColumn(((TimestampColumnVector) col).getTime(row));
            default -> {
                var v = ((BytesColumnVector) col).toString(row);
                yield v.equals(nullFormat) ? new StringColumn() : new StringColumn(v);
            }
        };
    }
}
