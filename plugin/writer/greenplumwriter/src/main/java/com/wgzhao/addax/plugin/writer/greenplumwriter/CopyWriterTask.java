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

package com.wgzhao.addax.plugin.writer.greenplumwriter;

import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.plugin.rdbms.util.DBUtil;
import com.wgzhao.addax.plugin.rdbms.util.DBUtilErrorCode;
import com.wgzhao.addax.plugin.rdbms.util.DataBaseType;
import com.wgzhao.addax.plugin.rdbms.writer.CommonRdbmsWriter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CopyWriterTask
        extends CommonRdbmsWriter.Task
{
    private static final Logger LOG = LoggerFactory.getLogger(CopyWriterTask.class);
    private Configuration writerSliceConfig = null;
    private volatile boolean stopProcessor = false;
    private volatile boolean stopWriter = false;

    private CompletionService<Long> cs = null;

    public CopyWriterTask()
    {
        super(DataBaseType.PostgreSQL);
    }

    public String getJdbcUrl()
    {
        return this.jdbcUrl;
    }

    public Connection createConnection()
    {
        String basicMsg = String.format("jdbcUrl:[%s]", this.jdbcUrl);
        Connection connection = DBUtil.getConnection(this.dataBaseType, this.jdbcUrl, username, password);
        DBUtil.dealWithSessionConfig(connection, writerSliceConfig, this.dataBaseType, basicMsg);
        return connection;
    }

    private String constructColumnNameList(List<String> columnList)
    {
        List<String> columns = new ArrayList<>();

        for (String column : columnList) {
            if (column.endsWith("\"") && column.startsWith("\"")) {
                columns.add(column);
            }
            else {
                columns.add("\"" + column + "\"");
            }
        }

        return StringUtils.join(columns, ",");
    }

    public String getCopySql(String tableName, List<String> columnList)
    {

        return "COPY " + tableName + "(" +
                constructColumnNameList(columnList) +
                ") FROM STDIN WITH DELIMITER '" +
                Constant.DELIMTER + "' NULL '' CSV QUOTE '" + Constant.QUOTE_CHAR  + "' ESCAPE E'" + Constant.ESCAPE + Constant.ESCAPE + "';";
    }

    private void send(Record record, LinkedBlockingQueue<Record> queue)
            throws InterruptedException, ExecutionException
    {
        while (!queue.offer(record, Constant.TIME_OUT_MS, TimeUnit.MILLISECONDS)) {
            LOG.debug("Record queue is full, increase num_copy_processor for performance.");
            Future<Long> result = cs.poll();

            if (result != null) {
                result.get();
            }
        }
    }

    public boolean moreRecord()
    {
        return !stopProcessor;
    }

    public boolean moreData()
    {
        return !stopWriter;
    }

    @Override
    public void startWrite(RecordReceiver recordReceiver, Configuration writerSliceConfig,
            TaskPluginCollector taskPluginCollector)
    {
        this.writerSliceConfig = writerSliceConfig;
        int queueSize = writerSliceConfig.getInt(Key.QUEUE_SIZE, Constant.COPY_QUEUE_SIZE);
        int numProcessor = writerSliceConfig.getInt(Key.NUM_PROCESS, Constant.NUM_COPY_PROCESSOR);
        int numWriter = writerSliceConfig.getInt(Key.NUM_WRITER, Constant.NUM_COPY_WRITER);

        String sql = getCopySql(this.table, this.columns);
        LinkedBlockingQueue<Record> recordQueue = new LinkedBlockingQueue<>(queueSize);
        LinkedBlockingQueue<byte[]> dataQueue = new LinkedBlockingQueue<>(queueSize);
        ExecutorService threadPool;

        threadPool = Executors.newFixedThreadPool(numProcessor + numWriter);
        cs = new ExecutorCompletionService<>(threadPool);
        Connection connection = createConnection();

        try {

            this.resultSetMetaData = DBUtil.getColumnMetaData(connection, this.table,
                    constructColumnNameList(this.columns));
            for (int i = 0; i < numProcessor; i++) {
                cs.submit(new CopyProcessor(this, this.columnNumber, resultSetMetaData, recordQueue, dataQueue));
            }

            for (int i = 0; i < numWriter; i++) {
                cs.submit(new CopyWorker(this, sql, dataQueue));
            }

            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                send(record, recordQueue);
                Future<Long> result = cs.poll();

                if (result != null) {
                    result.get();
                }
            }

            stopProcessor = true;
            for (int i = 0; i < numProcessor; i++) {
                cs.take().get();
            }

            stopWriter = true;
            for (int i = 0; i < numWriter; i++) {
                cs.take().get();
            }
        }
        catch (ExecutionException e) {
            throw AddaxException.asAddaxException(DBUtilErrorCode.WRITE_DATA_ERROR, e.getCause());
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
        }
        finally {
            threadPool.shutdownNow();
            DBUtil.closeDBResources(null, null, connection);
        }
    }
}
