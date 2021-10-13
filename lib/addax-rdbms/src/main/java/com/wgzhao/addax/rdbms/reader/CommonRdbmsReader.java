/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.wgzhao.addax.rdbms.reader;

import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.BoolColumn;
import com.wgzhao.addax.common.element.BytesColumn;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.element.TimestampColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.statistics.PerfRecord;
import com.wgzhao.addax.common.statistics.PerfTrace;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.reader.util.GetPrimaryKeyUtil;
import com.wgzhao.addax.rdbms.reader.util.OriginalConfPretreatmentUtil;
import com.wgzhao.addax.rdbms.reader.util.PreCheckTask;
import com.wgzhao.addax.rdbms.reader.util.ReaderSplitUtil;
import com.wgzhao.addax.rdbms.reader.util.SingleTableSplitUtil;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DBUtilErrorCode;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.util.RdbmsException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
                    if (originalConfig.getInt(Key.EACH_TABLE_SPLIT_SIZE, -1) == -1) {
                        originalConfig.set(Key.EACH_TABLE_SPLIT_SIZE, Constant.DEFAULT_EACH_TABLE_SPLIT_SIZE);
                    }
                }
            }
            LOG.debug("After job init(), job config now is:[\n{}\n]", originalConfig.toJSON());
            return originalConfig;
        }

        public void preCheck(Configuration originalConfig, DataBaseType dataBaseType)
        {
            /* 检查每个表是否有读权限，以及querySql跟split Key是否正确 */
            Configuration queryConf = ReaderSplitUtil.doPreCheckSplit(originalConfig);
            String splitPK = queryConf.getString(Key.SPLIT_PK);
            List<Object> connList = queryConf.getList(Key.CONNECTION, Object.class);
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
                    throw (AddaxException) e.getCause();
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

        public Task(DataBaseType dataBaseType, int taskGroupId, int taskId)
        {
            this.dataBaseType = dataBaseType;
            this.taskGroupId = taskGroupId;
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
                    transportOneRecord(recordSender, rs, metaData, columnNumber, taskPluginCollector);
                    lastTime = System.nanoTime();
                }

                allResultPerfRecord.end(rsNextUsedTime);
                // 目前大盘是依赖这个打印，而之前这个Finish read record是包含了sql查询和result next的全部时间
                LOG.info("Finished read record by Sql: [{}\n] {}.", querySql, basicMsg);
            }
            catch (Exception e) {
                throw RdbmsException.asQueryException(e, querySql);
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
                int columnNumber, TaskPluginCollector taskPluginCollector)
        {
            Record record = buildRecord(recordSender, rs, metaData, columnNumber, taskPluginCollector);
            recordSender.sendToWriter(record);
        }

        protected Column createColumn(ResultSet rs, ResultSetMetaData metaData, int i)
                throws SQLException, UnsupportedEncodingException
        {
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
                        rawData = new String((rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY : rs.getBytes(i)), mandatoryEncoding);
                    }
                    return new StringColumn(rawData);

                case Types.CLOB:
                case Types.NCLOB:
                    return new StringColumn(rs.getString(i));

                case Types.SMALLINT:
                case Types.TINYINT:
                case Types.INTEGER:
                case Types.BIGINT:
                    return new LongColumn(rs.getString(i));

                case Types.NUMERIC:
                case Types.DECIMAL:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    return new DoubleColumn(rs.getString(i));

                case Types.TIME:
                    return new DateColumn(rs.getTime(i));

                case Types.DATE:
                    return new DateColumn(rs.getDate(i));

                case Types.TIMESTAMP:
                    return new TimestampColumn(rs.getTimestamp(i));

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.BLOB:
                case Types.LONGVARBINARY:
                    return new BytesColumn(rs.getBytes(i));

                case Types.BOOLEAN:
                    return new BoolColumn(rs.getBoolean(i));

                case Types.BIT:
                    // bit(1) -> Types.BIT 可使用BoolColumn
                    // bit(>1) -> Types.VARBINARY 可使用BytesColumn
                    if (metaData.getPrecision(i) == 1) {
                        return new BoolColumn(rs.getBoolean(i));
                    }
                    else {
                        return new BytesColumn(rs.getBytes(i));
                    }

                case Types.NULL:
                    String stringData = null;
                    if (rs.getObject(i) != null) {
                        stringData = rs.getObject(i).toString();
                    }
                    return new StringColumn(stringData);

                case Types.ARRAY:
                    return new StringColumn(rs.getArray(i).toString());

                case Types.JAVA_OBJECT:

                case Types.OTHER:
                    return new StringColumn(rs.getObject(i).toString());

                case Types.SQLXML:
                    return new StringColumn(rs.getSQLXML(i).getString());

                default:
                    throw AddaxException.asAddaxException(DBUtilErrorCode.UNSUPPORTED_TYPE,
                            String.format("您的配置文件中的列配置信息有误. 不支持数据库读取这种字段类型. " + "字段名:[%s], 字段类型:[%s], "
                                            + "字段类型名称:[%s], 字段Java类型:[%s]. " + "请尝试使用数据库函数将其转换支持的类型 或者不同步该字段 .",
                                    metaData.getColumnName(i), metaData.getColumnType(i),
                                    metaData.getColumnTypeName(i), metaData.getColumnClassName(i)));
            }
        }

        protected Record buildRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData, int columnNumber,
                TaskPluginCollector taskPluginCollector)
        {
            Record record = recordSender.createRecord();

            try {
                for (int i = 1; i <= columnNumber; i++) {
                    record.addColumn(createColumn(rs, metaData, i));
                }
            }
            catch (Exception e) {
                if (IS_DEBUG) {
                    LOG.debug("read data " + record + " occur exception: " + e);
                }
                // TODO 这里识别为脏数据靠谱吗？
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof AddaxException) {
                    throw (AddaxException) e;
                }
            }
            return record;
        }
    }
}
