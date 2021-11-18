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

package com.wgzhao.addax.plugin.writer.doriswriter;

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtilErrorCode;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static com.wgzhao.addax.common.base.Constant.DEFAULT_BATCH_BYTE_SIZE;

public class DorisWriterTask
{
    private static final Logger LOG = LoggerFactory.getLogger(DorisWriterTask.class);
    private static final String DEFAULT_LABEL_PREFIX = "addax_doris_writer_";
    private final Configuration configuration;
    private List<String> column;
    private static final String SEPARATOR = "|";
    private long batchSize;
    private long batchByteSize;
    private DorisCodec rowCodec;
    private int batchNum = 0;
    private DorisWriterEmitter dorisWriterEmitter;

    public DorisWriterTask(Configuration configuration) {this.configuration = configuration;}

    public void init()
    {
        List<Object> connList = configuration.getList(Key.CONNECTION);
        Configuration conn = Configuration.from(connList.get(0).toString());
        if ("csv".equalsIgnoreCase(configuration.getString(DorisKey.FORMAT, "csv"))) {
            this.rowCodec = new DorisCsvCodec(configuration.getList(DorisKey.COLUMN, String.class), SEPARATOR);
        }
        else {
            this.rowCodec = new DorisJsonCodec(configuration.getList(DorisKey.COLUMN, String.class));
        }
        this.dorisWriterEmitter = new DorisWriterEmitter(configuration);

        this.column = configuration.getList(Key.COLUMN, String.class);
        // 如果 column 填写的是 * ，直接设置为null，方便后续判断
        if (this.column != null && this.column.size() == 1 && "*".equals(this.column.get(0))) {
            this.column = null;
        }
        this.batchSize = configuration.getInt(Key.BATCH_SIZE, 1024);
        this.batchByteSize = configuration.getLong(DorisKey.BATCH_BYTE_SIZE, DEFAULT_BATCH_BYTE_SIZE);
    }

    public void startWrite(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector)
    {
        String lineDelimiter = configuration.getString(DorisKey.LINE_DELIMITER, "\n");

        try {
            DorisFlushBatch flushBatch = new DorisFlushBatch(lineDelimiter);
            Record record;
            long batchCount = 0L;
            long batchSize = 0L;
            while ((record = recordReceiver.getFromReader()) != null) {
                int len = record.getColumnNumber();
                // check column size
                if (this.column != null && len != this.column.size()) {
                    throw AddaxException.asAddaxException(
                            DorisWriterErrorCode.ILLEGAL_VALUE,
                            String.format("config writer column info error. because the column number of reader is :%s" +
                                    "and the column number of writer is:%s. please check your job config json", len, this.column.size())
                    );
                }
                // codec record
                final String recordStr = this.rowCodec.serialize(record);
                // put into buffer
                flushBatch.putData(recordStr);
                batchCount++;
                batchSize += recordStr.length();
                // trigger buffer
                if (batchCount >= this.batchSize || batchSize >= this.batchByteSize) {
                    flush(flushBatch);
                    // clear buffer
                    batchCount = 0L;
                    batchByteSize = 0L;
                    flushBatch = new DorisFlushBatch(lineDelimiter);
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

    private void flush(DorisFlushBatch flushBatch)
            throws IOException
    {
        final String label = getStreamLoadLabel();
        flushBatch.setLabel(label);
        dorisWriterEmitter.doStreamLoad(flushBatch);
    }

    private String getStreamLoadLabel() {
        return DEFAULT_LABEL_PREFIX + UUID.randomUUID() + "_" + (batchNum++);
    }
}
