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

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.DataXException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.kudu.Type;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Upsert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

public class KuduWriterTask
    extends Writer
{
    private static final Logger LOG = LoggerFactory.getLogger(KuduWriterTask.class);
    public KuduClient kuduClient;
    public KuduSession session;
    private List<Configuration> columns;
    private List<List<Configuration>> columnLists;
    private List<Map<String, Type>> needColumns;
    private ThreadPoolExecutor pool;
    private String encoding;
    private Double batchSize;
    private Boolean isUpsert;
    private Boolean isSkipFail;
    private KuduTable table;
    private Integer primaryKeyIndexUntil;
    private Configuration configuration;

    public KuduWriterTask(Configuration configuration)
    {
        this.configuration = configuration;
        columns = configuration.getListConfiguration(Key.COLUMN);
        columnLists = KuduHelper.getColumnLists(columns);
        pool = KuduHelper.createRowAddThreadPool(columnLists.size());

        this.encoding = configuration.getString(Key.ENCODING);
        this.batchSize = configuration.getDouble(Key.WRITE_BATCH_SIZE);
        this.isUpsert = !"insert".equalsIgnoreCase(configuration.getString(Key.WRITE_MODE));
        this.isSkipFail = configuration.getBool(Key.SKIP_FAIL);
        long mutationBufferSpace = configuration.getLong(Key.MUTATION_BUFFER_SPACE);

        this.kuduClient = KuduHelper.getKuduClient(configuration);
        this.table = KuduHelper.getKuduTable(this.kuduClient,
                configuration.getString(Key.KUDU_TABLE_NAME));

        this.session = kuduClient.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace((int) mutationBufferSpace);
//        this.primaryKeyIndexUntil = KuduHelper.getPrimaryKeyIndexUntil(columns);
//        tableName = configuration.getString(Key.TABLE);
    }

    public void startWriter(RecordReceiver lineReceiver, TaskPluginCollector taskPluginCollector)
    {
        LOG.info("kuduwriter begin to write!");
        Record record;
        List<Configuration> columns = configuration.getListConfiguration(Key.COLUMN);
        List<String> columnNames = KuduHelper.getColumnNames(columns);
        while ((record = lineReceiver.getFromReader()) != null) {
            if (record.getColumnNumber() != columns.size()) {
                throw DataXException.asDataXException(KuduWriterErrorCode.PARAMETER_NUM_ERROR,
                        " number of record fields:" + record.getColumnNumber()
                                + " number of configuration fields:" + columns.size());
            }
            Upsert upsert = table.newUpsert();
            Insert insert = table.newInsert();
            PartialRow row;
            if (isUpsert) {
                //覆盖更新
                row = upsert.getRow();
            }
            else {
                //增量更新
                row = insert.getRow();
            }
            for (int i = 0; i < record.getColumnNumber(); i++) {
                Column column = record.getColumn(i);
                String name = columnNames.get(i);
                if (column.getRawData() == null) {
                    row.setNull(name);
                    continue;
                }
                Type type = Type.getTypeForName(columns.get(i).getString(Key.TYPE));
                switch (type) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                        row.addInt(name, Integer.parseInt(column.asString()));
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
                        row.addTimestamp(name, new Timestamp(column.asLong()));
                        break;
                    case DATE:
                        row.addDate(name,
                                (Date) new java.util.Date(column.asDate().getTime()));
                        break;
                    default:
                        throw DataXException.asDataXException(
                                KuduWriterErrorCode.ILLEGAL_VALUE, "unsupport data type " + type
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
            }
            catch (Exception e) {
                LOG.error("Record Write Failure!", e);
                if (isSkipFail) {
                    LOG.warn("Since you have configured \"skipFail\" to be true, this record will be skipped !");
                    taskPluginCollector.collectDirtyRecord(record, e.getMessage());
                }
                else {
                    throw DataXException.asDataXException(KuduWriterErrorCode.PUT_KUDU_ERROR, e.getMessage());
                }
            }
        }
    }
}
