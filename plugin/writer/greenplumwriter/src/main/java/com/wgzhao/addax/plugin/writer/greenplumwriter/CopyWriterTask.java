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

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DBUtilErrorCode;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;
import com.wgzhao.addax.rdbms.writer.util.WriterUtil;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.base.Constant.DEFAULT_BATCH_SIZE;
import static com.wgzhao.addax.common.base.Key.BATCH_SIZE;

public class CopyWriterTask
        extends CommonRdbmsWriter.Task
{
    private static final Logger LOG = LoggerFactory.getLogger(CopyWriterTask.class);
    private Configuration writerSliceConfig = null;
//    private volatile boolean stopProcessor = false;
//    private volatile boolean stopWriter = false;

//    private CompletionService<Long> cs = null;

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
                GPConstant.DELIMITER + "' NULL '' CSV QUOTE '" + GPConstant.QUOTE_CHAR + "' ESCAPE E'" + GPConstant.ESCAPE + GPConstant.ESCAPE + "';";
    }

//    private void send(Record record, LinkedBlockingQueue<Record> queue)
//            throws InterruptedException, ExecutionException
//    {
//        while (!queue.offer(record, GPConstant.TIME_OUT_MS, TimeUnit.MILLISECONDS)) {
//            LOG.debug("Record queue is full, increase num_copy_processor for performance.");
//            Future<Long> result = cs.poll();
//
//            if (result != null) {
//                result.get();
//            }
//        }
//    }

//    public boolean moreRecord()
//    {
//        return !stopProcessor;
//    }

//    public boolean moreData()
//    {
//        return !stopWriter;
//    }

    @Override
    public void startWrite(RecordReceiver recordReceiver, Configuration writerSliceConfig,
            TaskPluginCollector taskPluginCollector)
    {
        this.writerSliceConfig = writerSliceConfig;
//        int queueSize = writerSliceConfig.getInt(GPKey.QUEUE_SIZE, GPConstant.COPY_QUEUE_SIZE);
//        int numProcessor = writerSliceConfig.getInt(GPKey.NUM_PROCESS, GPConstant.NUM_COPY_PROCESSOR);
//        int numWriter = writerSliceConfig.getInt(GPKey.NUM_WRITER, GPConstant.NUM_COPY_WRITER);
        int batchSize = writerSliceConfig.getInt(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        Record record;
        int numRecord = 0;
        StringBuilder multiRecords = new StringBuilder();
        String sql = getCopySql(this.table, this.columns);
        LOG.info("Write data with [{}]", sql);
//        LinkedBlockingQueue<Record> recordQueue = new LinkedBlockingQueue<>(queueSize);
//        LinkedBlockingQueue<byte[]> dataQueue = new LinkedBlockingQueue<>(queueSize);
//        ExecutorService threadPool;

//        threadPool = Executors.newFixedThreadPool(numProcessor + numWriter);
//        cs = new ExecutorCompletionService<>(threadPool);
        Connection connection = createConnection();
        changeCsvSizeLimit(connection);

        try {
            CopyManager mgr = connection.unwrap(PGConnection.class).getCopyAPI();
            this.resultSetMetaData = DBUtil.getColumnMetaData(connection, this.table, constructColumnNameList(this.columns));
//            for (int i = 0; i < numProcessor; i++) {
//                cs.submit(new CopyProcessor(this, this.columnNumber, resultSetMetaData, recordQueue, dataQueue));
//            }
//
//            for (int i = 0; i < numWriter; i++) {
//                cs.submit(new CopyWorker(this, sql, dataQueue));
//            }

            while ((record = recordReceiver.getFromReader()) != null) {
                multiRecords.append(serializeRecord(record));
                numRecord++;
                if (numRecord % batchSize == 0) {
//                    reader.unread(multiRecords.toString().toCharArray());
                    mgr.copyIn(sql, new ByteArrayInputStream(multiRecords.toString().getBytes(StandardCharsets.UTF_8)));
                    multiRecords.delete(0, multiRecords.length());
                }
            }
            if (multiRecords.length() > 0) {
                mgr.copyIn(sql, new ByteArrayInputStream(multiRecords.toString().getBytes(StandardCharsets.UTF_8)));
            }
        }

        catch (Exception e) {
            throw AddaxException.asAddaxException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
        }
        finally {
            DBUtil.closeDBResources(null, null, connection);
        }
    }

    /**
     * Any occurrence within the value of a QUOTE character or the ESCAPE
     * character is preceded by the escape character.
     *
     * @param data string will be escaped
     * @return escaped string
     */
    protected String escapeString(String data)
    {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < data.length(); ++i) {
            char c = data.charAt(i);
            switch (c) {
                case 0x00:
                    LOG.warn("Illegal symbol 0x00 exists, has dropped it");
                    continue;
                case GPConstant.QUOTE_CHAR:
                case GPConstant.ESCAPE:
                    sb.append(GPConstant.ESCAPE);
                    break;
                default:
                    break;
            }

            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Non-printable characters are inserted as '\nnn' (octal) and '\' as '\\'.
     *
     * @param data byte[] bytes array will be escaped
     * @return escaped string
     */
    protected String escapeBinary(byte[] data)
    {
        StringBuilder sb = new StringBuilder();

        for (byte datum : data) {
            if (datum == GPConstant.ESCAPE) {
                sb.append(GPConstant.ESCAPE);
                sb.append(GPConstant.ESCAPE);
            }
            else if (datum < 0x20 || datum > 0x7e) {
                byte b = datum;
                char[] val = new char[3];
                val[2] = (char) ((b & 7) + '0');
                b >>= 3;
                val[1] = (char) ((b & 7) + '0');
                b >>= 3;
                val[0] = (char) ((b & 3) + '0');
                sb.append('\\');
                sb.append(val);
            }
            else {
                sb.append((char) datum);
            }
        }

        return sb.toString();
    }

    protected String serializeRecord(Record record)
    {
        StringBuilder sb = new StringBuilder();
        Column column;
        for (int i = 0; i < this.columnNumber; i++) {
            column = record.getColumn(i);
            int columnSqlType = this.resultSetMetaData.getMiddle().get(i);

            switch (columnSqlType) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR: {
                    String data = column.asString();

                    if (data != null) {
                        sb.append(GPConstant.QUOTE_CHAR);
                        sb.append(escapeString(data));
                        sb.append(GPConstant.QUOTE_CHAR);
                    }

                    break;
                }
                case Types.BINARY:
                case Types.BLOB:
                case Types.CLOB:
                case Types.LONGVARBINARY:
                case Types.NCLOB:
                case Types.VARBINARY: {
                    byte[] data = column.asBytes();

                    if (data != null) {
                        sb.append(escapeBinary(data));
                    }

                    break;
                }
                default: {
                    String data = column.asString();

                    if (data != null) {
                        sb.append(data);
                    }

                    break;
                }
            }

            if (i + 1 < this.columnNumber) {
                sb.append(GPConstant.DELIMITER);
            }
        }
        sb.append(GPConstant.NEWLINE);
        return sb.toString();
    }

    private void changeCsvSizeLimit(Connection conn)
    {
        List<String> sqls = new ArrayList<>();
        sqls.add("set gp_max_csv_line_length = " + GPConstant.MAX_CSV_SIZE);

        try {
            WriterUtil.executeSqls(conn, sqls, getJdbcUrl(), DataBaseType.PostgreSQL);
        }
        catch (Exception e) {
            LOG.warn("Cannot set gp_max_csv_line_length to {}", GPConstant.MAX_CSV_SIZE);
        }
    }
}
