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

package com.wgzhao.addax.plugin.writer.kuduwriter;

import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Upsert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import static com.wgzhao.addax.core.base.Constant.DEFAULT_BATCH_SIZE;
import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

public class KuduWriterTask
        extends Writer
{
    private static final Logger LOG = LoggerFactory.getLogger(KuduWriterTask.class);
    public KuduSession session;
    private final Double batchSize;
    private final List<String> columns;
    private final Boolean isUpsert;
    private final Boolean isSkipFail;
    private final KuduTable table;
    private final KuduHelper kuduHelper;

    public KuduWriterTask(Configuration configuration)
    {

        String masterAddress = configuration.getString(KuduKey.KUDU_MASTER_ADDRESSES);
        this.kuduHelper = new KuduHelper(masterAddress, configuration.getLong(KuduKey.KUDU_TIMEOUT));
        this.columns = configuration.getList(KuduKey.COLUMN, String.class);

        this.batchSize = configuration.getDouble(KuduKey.BATCH_SIZE, DEFAULT_BATCH_SIZE);
        this.isUpsert = !"insert".equalsIgnoreCase(configuration.getString(KuduKey.WRITE_MODE));
        this.isSkipFail = configuration.getBool(KuduKey.SKIP_FAIL);
        long mutationBufferSpace = configuration.getLong(KuduKey.MUTATION_BUFFER_SPACE);

        this.table = kuduHelper.getKuduTable(configuration.getString(KuduKey.TABLE));

        this.session = kuduHelper.getSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace((int) mutationBufferSpace);
    }

    public void startWriter(RecordReceiver lineReceiver, TaskPluginCollector taskPluginCollector)
    {
        LOG.info("Begin to write");
        Record record;
        int commit = 0;
        final Schema schema = this.table.getSchema();
        while ((record = lineReceiver.getFromReader()) != null) {
            if (record.getColumnNumber() != columns.size()) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "The number of record fields (" + record.getColumnNumber()
                                + ") is different from the number of configuration fields (" + columns.size() + ")");
            }
            Upsert upsert = table.newUpsert();
            Insert insert = table.newInsert();
            PartialRow row;
            if (isUpsert) {
                //override update
                row = upsert.getRow();
            }
            else {
                //incremental update
                row = insert.getRow();
            }
            for (int i = 0; i < record.getColumnNumber(); i++) {
                Column column = record.getColumn(i);
                String name = columns.get(i);
                Type type = schema.getColumn(name).getType();
                if (column == null || column.getRawData() == null) {
                    row.setNull(name);
                    continue;
                }
                switch (type) {
                    case INT8:
                    case INT16:
                    case INT32:
                        row.addInt(name, Integer.parseInt(column.asString()));
                        break;
                    case INT64:
                        row.addLong(name, Long.parseLong(column.asString()));
                        break;
                    case FLOAT:
                    case DOUBLE:
                        row.addDouble(name, column.asDouble());
                        break;
                    case STRING:
                        row.addString(name, column.asString());
                        break;
                    case BOOL:
                        row.addBoolean(name, column.asBoolean());
                        break;
                    case BINARY:
                        row.addBinary(name, column.asBytes());
                        break;
                    case DECIMAL:
                        row.addDecimal(name, new BigDecimal(column.asString()));
                        break;
                    case UNIXTIME_MICROS:
                        // need convert local timestamp to  UTC
                        int offsetSecs = ZonedDateTime.now(ZoneId.systemDefault()).getOffset().getTotalSeconds();
                        // use nanos timestamp value
                        row.addLong(name, (column.asTimestamp().getTime() * 1_000L + offsetSecs * 1_000_000L));
                        break;
                    case DATE:
                        // Kudu take date as string
                        row.addString(name, column.asString());
                        break;
                    default:
                        throw AddaxException.asAddaxException(
                                NOT_SUPPORT_TYPE, "The data type " + type + " is unsupported"
                        );
                }
            } // end a row
            try {
                if (isUpsert) {
                    session.apply(upsert);
                }
                else {
                    session.apply(insert);
                }
                commit++;
                if (commit % batchSize == 0) {
                    // flush
                    session.flush();
                }
            }
            catch (KuduException e) {
                LOG.error("Failed to write a record: ", e);
                if (isSkipFail) {
                    LOG.warn("Since you have configured 'skipFail' to be true, this record will be skipped.");
                    taskPluginCollector.collectDirtyRecord(record, e.getMessage());
                }
                else {
                    throw AddaxException.asAddaxException(RUNTIME_ERROR, e.getMessage());
                }
            }
        }

        try {
            // try to flush last upsert/insert
            session.flush();
        }
        catch (KuduException e) {
            LOG.error("Failed to write a record: ", e);
            if (isSkipFail) {
                LOG.warn("Since you have configured 'skipFail' to be true, this record will be skipped !");
                taskPluginCollector.collectDirtyRecord(record, e.getMessage());
            }
            else {
                throw AddaxException.asAddaxException(RUNTIME_ERROR, e.getMessage());
            }
        }
    }

    public void close() {
        kuduHelper.closeClient();
    }
}
