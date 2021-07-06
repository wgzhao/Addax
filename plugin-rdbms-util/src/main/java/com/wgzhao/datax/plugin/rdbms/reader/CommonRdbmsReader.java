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

package com.wgzhao.datax.plugin.rdbms.reader;

import com.wgzhao.datax.common.element.BoolColumn;
import com.wgzhao.datax.common.element.BytesColumn;
import com.wgzhao.datax.common.element.DateColumn;
import com.wgzhao.datax.common.element.DoubleColumn;
import com.wgzhao.datax.common.element.LongColumn;
import com.wgzhao.datax.common.element.Record;
import com.wgzhao.datax.common.element.StringColumn;
import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.plugin.RecordSender;
import com.wgzhao.datax.common.plugin.TaskPluginCollector;
import com.wgzhao.datax.common.statistics.PerfRecord;
import com.wgzhao.datax.common.statistics.PerfTrace;
import com.wgzhao.datax.common.util.Configuration;
import com.wgzhao.datax.plugin.rdbms.reader.util.GetPrimaryKeyUtil;
import com.wgzhao.datax.plugin.rdbms.reader.util.OriginalConfPretreatmentUtil;
import com.wgzhao.datax.plugin.rdbms.reader.util.PreCheckTask;
import com.wgzhao.datax.plugin.rdbms.reader.util.ReaderSplitUtil;
import com.wgzhao.datax.plugin.rdbms.reader.util.SingleTableSplitUtil;
import com.wgzhao.datax.plugin.rdbms.util.DBUtil;
import com.wgzhao.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.wgzhao.datax.plugin.rdbms.util.DataBaseType;
import com.wgzhao.datax.plugin.rdbms.util.RdbmsException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CommonRdbmsReader
{

    public static class Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        public Job(DataBaseType dataBaseType)
        {
            OriginalConfPretreatmentUtil.dataBaseType = dataBaseType;
            SingleTableSplitUtil.dataBaseType = dataBaseType;
            GetPrimaryKeyUtil.dataBaseType = dataBaseType;
        }

        public Configuration init(Configuration originalConfig)
        {

            OriginalConfPretreatmentUtil.doPretreatment(originalConfig);
            if (originalConfig.getString(Key.SPLIT_PK) == null && originalConfig.getBool(Key.AUTO_PK, false)) {
                    LOG.info("Does not configure splitPk, try to guess");
                    String splitPK = GetPrimaryKeyUtil.getPrimaryKey(originalConfig);
                    if (splitPK != null) {
                        LOG.info("Try to use `" + splitPK + "` as primary key to split");
                        originalConfig.set(Key.SPLIT_PK, splitPK);
                        if (originalConfig.getInt(Constant.EACH_TABLE_SPLIT_SIZE, -1) == -1) {
                            originalConfig.set(Constant.EACH_TABLE_SPLIT_SIZE, 5);
                        }
                    }

            }
            LOG.debug("After job init(), job config now is:[\n{}\n]", originalConfig.toJSON());
            return originalConfig;
        }

        public void preCheck(Configuration originalConfig, DataBaseType dataBaseType)
        {
            /* 检查每个表是否有读权限，以及querySql跟splik Key是否正确 */
            Configuration queryConf = ReaderSplitUtil.doPreCheckSplit(originalConfig);
            String splitPK = queryConf.getString(Key.SPLIT_PK);
            List<Object> connList = queryConf.getList(Constant.CONN_MARK, Object.class);
            String username = queryConf.getString(Key.USERNAME);
            String password = queryConf.getString(Key.PASSWORD);
            ExecutorService exec;
            if (connList.size() < 10) {
                exec = Executors.newFixedThreadPool(connList.size());
            }
            else {
                exec = Executors.newFixedThreadPool(10);
            }
            Collection<PreCheckTask> taskList = new ArrayList<>();
            for (Object o : connList) {
                Configuration connConf = Configuration.from(o.toString());
                PreCheckTask t = new PreCheckTask(username, password, connConf, dataBaseType, splitPK);
                taskList.add(t);
            }
            List<Future<Boolean>> results = new ArrayList<>();
            try {
                results = exec.invokeAll(taskList);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            for (Future<Boolean> result : results) {
                try {
                    result.get();
                }
                catch (ExecutionException e) {
                    throw (DataXException) e.getCause();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            exec.shutdownNow();
        }

        public List<Configuration> split(Configuration originalConfig, int adviceNumber)
        {
            return ReaderSplitUtil.doSplit(originalConfig, adviceNumber);
        }

        public void post(Configuration originalConfig)
        {
            // do nothing
        }

        public void destroy(Configuration originalConfig)
        {
            // do nothing
        }
    }

    public static class Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();
        protected final byte[] EMPTY_CHAR_ARRAY = new byte[0];

        private final DataBaseType dataBaseType;
        private final int taskGroupId;
        private final int taskId;

        private String username;
        private String password;
        private String jdbcUrl;
        private String mandatoryEncoding;

        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        private String basicMsg;

        public Task(DataBaseType dataBaseType)
        {
            this(dataBaseType, -1, -1);
        }

        public Task(DataBaseType dataBaseType, int taskGropuId, int taskId)
        {
            this.dataBaseType = dataBaseType;
            this.taskGroupId = taskGropuId;
            this.taskId = taskId;
        }

        public void init(Configuration readerSliceConfig)
        {

            /* for database connection */

            this.username = readerSliceConfig.getString(Key.USERNAME);
            this.password = readerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = readerSliceConfig.getString(Key.JDBC_URL);

            this.mandatoryEncoding = readerSliceConfig.getString(Key.MANDATORY_ENCODING, "");

            basicMsg = String.format("jdbcUrl:[%s]", this.jdbcUrl);
        }

        public void startRead(Configuration readerSliceConfig, RecordSender recordSender,
                TaskPluginCollector taskPluginCollector, int fetchSize)
        {
            String querySql = readerSliceConfig.getString(Key.QUERY_SQL);
            String table = readerSliceConfig.getString(Key.TABLE);

            PerfTrace.getInstance().addTaskDetails(taskId, table + "," + basicMsg);

            LOG.info("Begin to read record by Sql: [{}\n] {}.", querySql, basicMsg);
            PerfRecord queryPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.SQL_QUERY);
            queryPerfRecord.start();

            Connection conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl, username, password);

            // session config .etc related
            DBUtil.dealWithSessionConfig(conn, readerSliceConfig, this.dataBaseType, basicMsg);

            int columnNumber;
            ResultSet rs;
            try {
                rs = DBUtil.query(conn, querySql, fetchSize);
                queryPerfRecord.end();

                ResultSetMetaData metaData = rs.getMetaData();
                columnNumber = metaData.getColumnCount();

                // 这个统计干净的result_Next时间
                PerfRecord allResultPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.RESULT_NEXT_ALL);
                allResultPerfRecord.start();

                long rsNextUsedTime = 0;
                long lastTime = System.nanoTime();
                while (rs.next()) {
                    rsNextUsedTime += (System.nanoTime() - lastTime);
                    this.transportOneRecord(recordSender, rs, metaData, columnNumber, mandatoryEncoding,
                            taskPluginCollector);
                    lastTime = System.nanoTime();
                }

                allResultPerfRecord.end(rsNextUsedTime);
                // 目前大盘是依赖这个打印，而之前这个Finish read record是包含了sql查询和result next的全部时间
                LOG.info("Finished read record by Sql: [{}\n] {}.", querySql, basicMsg);
            }
            catch (Exception e) {
                throw RdbmsException.asQueryException(this.dataBaseType, e, querySql, table, username);
            }
            finally {
                DBUtil.closeDBResources(null, conn);
            }
        }

        public void post(Configuration originalConfig)
        {
            // do nothing
        }

        public void destroy(Configuration originalConfig)
        {
            // do nothing
        }

        protected void transportOneRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData,
                int columnNumber, String mandatoryEncoding, TaskPluginCollector taskPluginCollector)
        {
            Record record = buildRecord(recordSender, rs, metaData, columnNumber, mandatoryEncoding,
                    taskPluginCollector);
            recordSender.sendToWriter(record);
        }

        protected Record buildRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData,
                int columnNumber, String mandatoryEncoding, TaskPluginCollector taskPluginCollector)
        {
            Record record = recordSender.createRecord();

            try {
                for (int i = 1; i <= columnNumber; i++) {
                    switch (metaData.getColumnType(i)) {

                        case Types.CHAR:
                        case Types.NCHAR:
                        case Types.VARCHAR:
                        case Types.LONGVARCHAR:
                        case Types.NVARCHAR:
                        case Types.LONGNVARCHAR:
                            String rawData;
                            if (StringUtils.isBlank(mandatoryEncoding)) {
                                rawData = rs.getString(i);
                            }
                            else {
                                rawData = new String((rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY : rs.getBytes(i)),
                                        mandatoryEncoding);
                            }
                            record.addColumn(new StringColumn(rawData));
                            break;

                        case Types.CLOB:
                        case Types.NCLOB:
                            record.addColumn(new StringColumn(rs.getString(i)));
                            break;

                        case Types.SMALLINT:
                        case Types.TINYINT:
                        case Types.INTEGER:
                        case Types.BIGINT:
                            record.addColumn(new LongColumn(rs.getString(i)));
                            break;

                        case Types.NUMERIC:
                        case Types.DECIMAL:
                        case Types.FLOAT:
                        case Types.REAL:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;

                        case Types.DOUBLE:
                            if ("money".equalsIgnoreCase(metaData.getColumnTypeName(i))) {
                                // remove currency nonation($) and currency formatting nonation(,)
                                // TODO process it more elegantly
                                record.addColumn(new DoubleColumn(rs.getString(i).substring(1).replace(",", "")));
                            }
                            else {
                                record.addColumn(new DoubleColumn(rs.getString(i)));
                            }
                            break;

                        case Types.TIME:
                            record.addColumn(new DateColumn(rs.getTime(i)));
                            break;

                        // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                        case Types.DATE:
                            if ("year".equalsIgnoreCase(metaData.getColumnTypeName(i))) {
                                record.addColumn(new LongColumn(rs.getInt(i)));
                            }
                            else {
                                record.addColumn(new DateColumn(rs.getDate(i)));
                            }
                            break;

                        case Types.TIMESTAMP:
                        case -151: // 兼容老的SQLServer版本的datetime数据类型
                            if (metaData.getColumnTypeName(i).startsWith("DateTime(")) {
                                // clickhouse DateTime(zoneinfo)
                                // TODO 含时区，当作Timestamp处理会有时区的差异
                                record.addColumn(new StringColumn(rs.getString(i)));
                            }
                            else {
                                record.addColumn(new DateColumn(rs.getTimestamp(i)));
                            }
                            break;

                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.BLOB:
                        case Types.LONGVARBINARY:
                            record.addColumn(new BytesColumn(rs.getBytes(i)));
                            break;

                        case Types.BOOLEAN:
                            record.addColumn(new BoolColumn(rs.getBoolean(i)));
                            break;
                        case Types.BIT:
                            // bit(1) -> Types.BIT 可使用BoolColumn
                            // bit(>1) -> Types.VARBINARY 可使用BytesColumn
                            if (metaData.getPrecision(i) == 1) {
                                record.addColumn(new BoolColumn(rs.getBoolean(i)));
                            }
                            else {
                                record.addColumn(new BytesColumn(rs.getBytes(i)));
                            }
                            break;

                        case Types.NULL:
                            String stringData = null;
                            if (rs.getObject(i) != null) {
                                stringData = rs.getObject(i).toString();
                            }
                            record.addColumn(new StringColumn(stringData));
                            break;

                        case Types.ARRAY:
                            record.addColumn(new StringColumn(rs.getArray(i).toString()));
                            break;

                        case Types.JAVA_OBJECT:
                            record.addColumn(new StringColumn(rs.getObject(i).toString()));
                            break;

                        case Types.SQLXML:
                            record.addColumn(new StringColumn(rs.getSQLXML(i).getString()));
                            break;

                        case Types.OTHER:
                            // database-specific type, convert it to string as default
                            String dType = metaData.getColumnTypeName(i);
                            LOG.debug("data-specific data type , column name: {}, column type:{}"
                                    , metaData.getColumnName(i), dType);
                            if ("image".equals(dType)) {
                                record.addColumn(new BytesColumn(rs.getBytes(i)));
                            }
                            else if (dType.startsWith("DateTime64")) {
                                // ClickHouse DateTime64(zoneinfo)
                                if (dType.contains(",")) {
                                    // TODO 含时区，当作Timestamp处理会有时区的差异
                                    record.addColumn(new StringColumn(rs.getString(i)));
                                }
                                else {
                                    record.addColumn(new DateColumn(rs.getTimestamp(i)));
                                }
                            }
                            else {
                                record.addColumn(new StringColumn(rs.getObject(i).toString()));
                            }
                            break;

                        default:
                            throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE,
                                    String.format("您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. " + "字段名:[%s], 字段类型:[%s], "
                                                    + "字段类型名称:[%s], 字段Java类型:[%s]. " + "请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                            metaData.getColumnName(i), metaData.getColumnType(i),
                                            metaData.getColumnTypeName(i), metaData.getColumnClassName(i)));
                    }
                }
            }
            catch (Exception e) {
                if (IS_DEBUG) {
                    LOG.debug("read data " + record + " occur exception: " + e);
                }
                // TODO 这里识别为脏数据靠谱吗？
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }
            }
            return record;
        }
    }
}
