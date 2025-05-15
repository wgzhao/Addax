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

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.StringJoiner;

import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.plugin.reader.hdfsreader.JavaType.ARRAY;
import static com.wgzhao.addax.plugin.reader.hdfsreader.JavaType.MAP;

public class MyOrcReader
{
    private static final Logger LOG = LoggerFactory.getLogger(MyOrcReader.class);

    private final org.apache.hadoop.conf.Configuration hadoopConf;
    private final String nullFormat;
    private final List<ColumnEntry> columnEntries;
    private final Path path;

    public MyOrcReader(org.apache.hadoop.conf.Configuration hadoopConf, Path path, String nullFormat, List<ColumnEntry> columns)
    {
        this.hadoopConf = hadoopConf;
        this.nullFormat = nullFormat;
        this.path = path;
        this.columnEntries = columns;
    }

    public void reader(RecordSender recordSender, TaskPluginCollector taskPluginCollector)
    {
        try (Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(hadoopConf))) {
            TypeDescription schema = reader.getSchema();
            assert columnEntries != null;
            if (columnEntries.isEmpty()) {
                for (int i = 0; i < schema.getChildren().size(); i++) {
                    ColumnEntry columnEntry = new ColumnEntry();
                    columnEntry.setIndex(i);
                    columnEntry.setType(schema.getChildren().get(i).getCategory().getName());
                    this.columnEntries.add(columnEntry);
                }
            }

            VectorizedRowBatch rowBatch = schema.createRowBatch(1024);
            org.apache.orc.RecordReader rowIterator = reader.rows(reader.options().schema(schema));
            while (rowIterator.nextBatch(rowBatch)) {
                buildRecord(rowBatch, recordSender, taskPluginCollector, nullFormat);
            }
        }
        catch (Exception e) {
            String message = String.format("Exception occurred while reading the file [%s].", path);
            LOG.error(message);
            throw AddaxException.asAddaxException(IO_ERROR, message);
        }
    }

    private void buildRecord(VectorizedRowBatch rowBatch, RecordSender recordSender,
            TaskPluginCollector taskPluginCollector, String nullFormat)
    {
        Record record;
        for (int row = 0; row < rowBatch.size; row++) {
            record = recordSender.createRecord();
            try {
                for (ColumnEntry column : columnEntries) {
                    Column columnGenerated;
                    if (column.getValue() != null) {
                        columnGenerated = "null".equals(column.getValue()) ?
                            new StringColumn(nullFormat) : new StringColumn(column.getValue());
                        record.addColumn(columnGenerated);
                        continue;
                    }
                    int i = column.getIndex();
                    String columnType = column.getType().toUpperCase();
                    ColumnVector col = rowBatch.cols[i];
                    JavaType type = JavaType.valueOf(columnType);
                    if (col.isNull[row]) {
                        record.addColumn(new StringColumn(null));
                        continue;
                    }

                    columnGenerated = switch (type) {
                        case ARRAY -> getArrayColumn(nullFormat, (ListColumnVector) col, row);
                        case MAP -> getMapColumn(nullFormat, (MapColumnVector) col, row);
                        default -> getPrimitiveColumn(nullFormat, type, col, row);
                    };
                    record.addColumn(columnGenerated);
                }
                recordSender.sendToWriter(record);
            }
            catch (Exception e) {
                if (e instanceof AddaxException ae) {
                    throw ae;
                }
                taskPluginCollector.collectDirtyRecord(record, e.getMessage());
            }
        }
    }

    private static @NotNull Column getMapColumn(String nullFormat, MapColumnVector col, int row) {
        var mapBuilder = new StringBuilder("{");
        // all value type must be same
        for (int j = (int) col.offsets[row]; j < col.offsets[row] + col.lengths[row]; j++) {
            if (j > col.offsets[row]) {
                mapBuilder.append(", ");
            }

            // The key must be string
            var key = ((BytesColumnVector) col.keys).toString(j);
            mapBuilder.append("\"%s\": ".formatted(key));

            var valueCol = col.values;
            if (valueCol.isNull[j]) {
                mapBuilder.append(nullFormat);
            }
            else {
                var sb = new StringBuilder();
                valueCol.stringifyValue(sb, j);
                mapBuilder.append(sb);
            }
        }
        return new StringColumn(mapBuilder.append("}").toString());
    }

    private static @NotNull Column getArrayColumn(String nullFormat, ListColumnVector col, int row) {
        var joiner = new StringJoiner(", ");
        for (var j = (int) col.offsets[row]; j < col.offsets[row] + col.lengths[row]; j++) {
            var childCol = col.child;
            if (childCol.isNull[j]) {
                joiner.add(nullFormat);
            }
            else {
                var sb = new StringBuilder();
                childCol.stringifyValue(sb, j);
                joiner.add(sb.toString());
            }
        }
        return new StringColumn("[%s]".formatted(joiner));
    }

    private static @NotNull Column getPrimitiveColumn(String nullFormat, JavaType type, ColumnVector col, int row) {
        return switch (type) {
            case INT, LONG, BOOLEAN, BIGINT ->
                new LongColumn(((LongColumnVector) col).vector[row]);
            case DATE -> {
                var date = new Date(((LongColumnVector) col).vector[row] * 86400 * 1000);
                var sdf = new SimpleDateFormat("yyyy-MM-dd");
                yield new StringColumn(sdf.format(date));
            }
            case FLOAT, DOUBLE ->
                new DoubleColumn(((DoubleColumnVector) col).vector[row]);
            case DECIMAL ->
                new DoubleColumn(((DecimalColumnVector) col).vector[row].doubleValue());
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
