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
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.reader.util.GetPrimaryKeyUtil;
import com.wgzhao.addax.rdbms.reader.util.OriginalConfPretreatmentUtil;
import com.wgzhao.addax.rdbms.reader.util.PreCheckTask;
import com.wgzhao.addax.rdbms.reader.util.ReaderSplitUtil;
import com.wgzhao.addax.rdbms.reader.util.SingleTableSplitUtil;
import com.wgzhao.addax.rdbms.util.DBUtil;
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
import java.util.Calendar;
import java.util.List;

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
                LOG.info("The split key is not configured, try to guess the split key.");
                String splitPK = GetPrimaryKeyUtil.getPrimaryKey(originalConfig);
                if (splitPK != null) {
                    LOG.info("Take the field {} as split key", splitPK);
                    originalConfig.set(Key.SPLIT_PK, splitPK);
                    if (originalConfig.getInt(Key.EACH_TABLE_SPLIT_SIZE, -1) == -1) {
                        originalConfig.set(Key.EACH_TABLE_SPLIT_SIZE, Constant.DEFAULT_EACH_TABLE_SPLIT_SIZE);
                    }
                } else {
                    LOG.warn("There is no primary key or unique key in the table, and the split key cannot be guessed.");
                }
            }

            LOG.debug("After the job is initialized, the job configuration is now as follows::[\n{}\n]", originalConfig.toJSON());
            return originalConfig;
        }

        public void preCheck(Configuration originalConfig, DataBaseType dataBaseType)
        {
            /* 检查每个表是否有读权限，以及querySql跟split Key是否正确 */
            Configuration queryConf = ReaderSplitUtil.doPreCheckSplit(originalConfig);
            String splitPK = queryConf.getString(Key.SPLIT_PK);
            Configuration connConf = queryConf.getConfiguration(Key.CONNECTION);
            String username = queryConf.getString(Key.USERNAME);
            String password = queryConf.getString(Key.PASSWORD);
            new PreCheckTask(username, password, connConf, dataBaseType, splitPK).call();
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

            basicMsg = "jdbcUrl: " + this.jdbcUrl;
        }

        public void startRead(Configuration readerSliceConfig, RecordSender recordSender,
                TaskPluginCollector taskPluginCollector, int fetchSize)
        {
            String querySql = readerSliceConfig.getString(Key.QUERY_SQL);

            LOG.info("Begin reading records by executing SQL query: [{}].", querySql);
            PerfRecord queryPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.SQL_QUERY);
            queryPerfRecord.start();

            Connection conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl, username, password);

            // session config related
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
                LOG.info("Finished reading records by executing SQL query: [{}].", querySql);
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
                    if(!"org.apache.hive.jdbc.HiveDriver".equals(this.dataBaseType.getDriverClassName())){
                        return new TimestampColumn(rs.getTimestamp(i, Calendar.getInstance()));
                    }else{
                        //hive not support method(Timestamp getTimestamp(int columnIndex, Calendar cal))
                        return new TimestampColumn(rs.getTimestamp(i));
                    }

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

                case Types.ARRAY:
                    return new StringColumn(rs.getArray(i).toString());

                case Types.SQLXML:
                    return new StringColumn(rs.getSQLXML(i).getString());

                default:
                    // use object as default data type for all unknown datatype
                    LOG.debug("Unknown data type: {} at field name: {}, using getObject().", metaData.getColumnType(i), metaData.getColumnName(i));
                    String stringData = null;
                    if (rs.getObject(i) != null) {
                        stringData = rs.getObject(i).toString();
                    }
                    return new StringColumn(stringData);
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
                    LOG.debug("Exception occurred while reading {} : {}", record, e.getMessage());
                }
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof AddaxException) {
                    throw (AddaxException) e;
                }
            }
            return record;
        }
    }
}
