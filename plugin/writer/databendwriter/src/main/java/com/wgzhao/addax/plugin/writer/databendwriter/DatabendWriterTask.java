/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.plugin.writer.databendwriter;

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtilErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static com.wgzhao.addax.common.base.Constant.DEFAULT_BATCH_BYTE_SIZE;

public class DatabendWriterTask
{
    private static final Logger LOG = LoggerFactory.getLogger(DatabendWriterTask.class);
    private static final String DEFAULT_LABEL_PREFIX = "addax_databend_writer_";
    private final Configuration configuration;
    private List<String> column;
    private static final String SEPARATOR = "\t";
    private long batchSize;
    private long batchByteSize;
    private DatabendCodec rowCodec;
    private int batchNum = 0;
    private DatabendWriterEmitter databendWriterEmitter;

    public DatabendWriterTask(Configuration configuration) {this.configuration = configuration;}

    public void init() {
        if ("csv".equalsIgnoreCase(configuration.getString(DatabendKey.FORMAT, "csv"))) {
            this.rowCodec = new DatabendCsvCodec(configuration.getList(DatabendKey.COLUMN, String.class), configuration.getString(Key.FIELD_DELIMITER));
        }
        else {
            LOG.error("Now only support csv format.");
            this.rowCodec = new DatabendJsonCodec(configuration.getList(DatabendKey.COLUMN, String.class));
        }
        this.databendWriterEmitter = new DatabendWriterEmitter(configuration);

        this.column = configuration.getList(Key.COLUMN, String.class);
        // 如果 column 填写的是 * ，直接设置为null，方便后续判断
        if (this.column != null && this.column.size() == 1 && "*".equals(this.column.get(0))) {
            this.column = null;
        }
        this.batchSize = configuration.getInt(Key.BATCH_SIZE, 1024);
        this.batchByteSize = configuration.getInt(Key.BATCH_BYTE_SIZE, DEFAULT_BATCH_BYTE_SIZE);
    }

    public void startWrite(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector)
    {
        String lineDelimiter = configuration.getString(DatabendKey.LINE_DELIMITER);

        try {
            DatabendFlushBatch flushBatch = new DatabendFlushBatch(lineDelimiter);
            Record record;
            long batchCount = 0L;
            long batchSize = 0L;
            while ((record = recordReceiver.getFromReader()) != null) {
                int len = record.getColumnNumber();
                // check column size
                if (this.column != null && len != this.column.size()) {
                    throw AddaxException.asAddaxException(
                            DatabendWriterErrorCode.ILLEGAL_VALUE,
                            String.format("config writer column info error. because the column number of reader is :%s" +
                                    "and the column number of writer is:%s. please check your job config json", len, this.column.size())
                    );
                }
                // codec record
                final String recordStr = this.rowCodec.serialize(record);
                // put into buffer
                flushBatch.putData(recordStr);
                batchCount++;
                byte[] bts = recordStr.getBytes(StandardCharsets.UTF_8);
                batchSize += bts.length;
                // trigger buffer
                if (batchCount >= this.batchSize || batchSize >= this.batchByteSize) {
                    flush(flushBatch);
                    // clear buffer
                    batchCount = 0L;
                    batchSize = 0L;
                    flushBatch = new DatabendFlushBatch(lineDelimiter);
                }
            } // end of while
            // flush the last batch
            if (flushBatch.getSize() > 0) {
                flush(flushBatch);
            }
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
        }
    }

    private void flush(DatabendFlushBatch flushBatch)
            throws IOException
    {
        final String label = getStreamLoadLabel();
        flushBatch.setLabel(label);
        databendWriterEmitter.doStreamLoad(flushBatch);
    }

    private String getStreamLoadLabel() {
        return DEFAULT_LABEL_PREFIX + UUID.randomUUID() + "_" + (batchNum++);
    }
}
