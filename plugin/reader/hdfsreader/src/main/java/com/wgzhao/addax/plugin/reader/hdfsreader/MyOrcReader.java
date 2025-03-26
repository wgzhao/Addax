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
import com.wgzhao.addax.core.element.DateColumn;
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
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;

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
                        if (!"null".equals(column.getValue())) {
                            columnGenerated = new StringColumn(column.getValue());
                        }
                        else {
                            columnGenerated = new StringColumn(nullFormat);
                        }
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
                    switch (type) {
                        case INT:
                        case LONG:
                        case BOOLEAN:
                        case BIGINT:
                            columnGenerated = new LongColumn(((LongColumnVector) col).vector[row]);
                            break;
                        case DATE:
                            columnGenerated = new DateColumn(new Date(((LongColumnVector) col).vector[row]));
                            break;
                        case FLOAT:
                        case DOUBLE:
                            columnGenerated = new DoubleColumn(((DoubleColumnVector) col).vector[row]);
                            break;
                        case DECIMAL:
                            columnGenerated = new DoubleColumn(((DecimalColumnVector) col).vector[row].doubleValue());
                            break;
                        case BINARY:
                            BytesColumnVector b = (BytesColumnVector) col;
                            byte[] val = Arrays.copyOfRange(b.vector[row], b.start[row], b.start[row] + b.length[row]);
                            columnGenerated = new BytesColumn(val);
                            break;
                        case TIMESTAMP:
                            // FIXME: incorrect timezone value
                            columnGenerated = new TimestampColumn(((TimestampColumnVector) col).getTime(row));
                            break;
                        default:
                            // type is string or other
                            String v = ((BytesColumnVector) col).toString(row);
                            columnGenerated = v.equals(nullFormat) ? new StringColumn() : new StringColumn(v);
                            break;
                    }
                    record.addColumn(columnGenerated);
                }
                recordSender.sendToWriter(record);
            }
            catch (Exception e) {
                if (e instanceof AddaxException) {
                    throw (AddaxException) e;
                }
                taskPluginCollector.collectDirtyRecord(record, e.getMessage());
            }
        }
    }
}
