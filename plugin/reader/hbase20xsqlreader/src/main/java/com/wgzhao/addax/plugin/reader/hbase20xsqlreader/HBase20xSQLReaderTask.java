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

package com.wgzhao.addax.plugin.reader.hbase20xsqlreader;

import com.wgzhao.addax.core.base.HBaseConstant;
import com.wgzhao.addax.core.base.HBaseKey;
import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.BytesColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.statistics.PerfRecord;
import com.wgzhao.addax.core.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;

public class HBase20xSQLReaderTask
{
    private static final Logger LOG = LoggerFactory.getLogger(HBase20xSQLReaderTask.class);

    private final Configuration readerConfig;
    private int taskGroupId;
    private int taskId;

    public HBase20xSQLReaderTask(Configuration config, int taskGroupId, int taskId)
    {
        this.readerConfig = config;
        this.taskGroupId = taskGroupId;
        this.taskId = taskId;
    }

    public void readRecord(RecordSender recordSender)
    {
        String querySql = readerConfig.getString(HBaseConstant.QUERY_SQL_PER_SPLIT);
        LOG.info("Begin to read record by Sql: [{}].", querySql);
        HBase20SQLReaderHelper helper = new HBase20SQLReaderHelper(readerConfig);
        Connection conn = helper.getConnection(readerConfig.getString(HBaseKey.QUERY_SERVER_ADDRESS),
                readerConfig.getString(HBaseKey.SERIALIZATION_NAME, HBaseConstant.DEFAULT_SERIALIZATION));
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            long rsNextUsedTime = 0;
            long lastTime = System.nanoTime();
            statement = conn.createStatement();
            // 统计查询时间
            PerfRecord queryPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.SQL_QUERY);
            queryPerfRecord.start();

            resultSet = statement.executeQuery(querySql);
            ResultSetMetaData meta = resultSet.getMetaData();
            int columnNum = meta.getColumnCount();
            // 统计的result_Next时间
            PerfRecord allResultPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.RESULT_NEXT_ALL);
            allResultPerfRecord.start();

            while (resultSet.next()) {
                Record record = recordSender.createRecord();
                rsNextUsedTime += (System.nanoTime() - lastTime);
                for (int i = 1; i <= columnNum; i++) {
                    Column column = this.convertPhoenixValueToAddaxColumn(meta.getColumnType(i), resultSet.getObject(i));
                    record.addColumn(column);
                }
                lastTime = System.nanoTime();
                recordSender.sendToWriter(record);
            }
            allResultPerfRecord.end(rsNextUsedTime);
            LOG.info("Finished read record by Sql: [{}].", querySql);
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(
                    EXECUTE_FAIL, "查询Phoenix数据出现异常，请检查服务状态或与HBase管理员联系！", e);
        }
        finally {
            helper.closeJdbc(conn, statement, resultSet);
        }
    }

    private Column convertPhoenixValueToAddaxColumn(int sqlType, Object value)
    {
        Column column;
        switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
                column = new StringColumn((String) value);
                break;
            case Types.BINARY:
            case Types.VARBINARY:
                column = new BytesColumn((byte[]) value);
                break;
            case Types.BOOLEAN:
                column = new BoolColumn((Boolean) value);
                break;
            case Types.INTEGER:
                column = new LongColumn((Integer) value);
                break;
            case Types.TINYINT:
                column = new LongColumn(((Byte) value).longValue());
                break;
            case Types.SMALLINT:
                column = new LongColumn(((Short) value).longValue());
                break;
            case Types.BIGINT:
                column = new LongColumn((Long) value);
                break;
            case Types.FLOAT:
                column = new DoubleColumn((Float.valueOf(value.toString())));
                break;
            case Types.DECIMAL:
                column = new DoubleColumn((BigDecimal) value);
                break;
            case Types.DOUBLE:
                column = new DoubleColumn((Double) value);
                break;
            case Types.DATE:
                column = new DateColumn((Date) value);
                break;
            case Types.TIME:
                column = new DateColumn((Time) value);
                break;
            case Types.TIMESTAMP:
                column = new DateColumn((Timestamp) value);
                break;
            default:
                throw AddaxException.asAddaxException(
                        NOT_SUPPORT_TYPE, "遇到不可识别的phoenix类型，" + "sqlType :" + sqlType);
        }
        return column;
    }
}
