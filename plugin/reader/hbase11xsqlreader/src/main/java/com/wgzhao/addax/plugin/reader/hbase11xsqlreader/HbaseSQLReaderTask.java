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

package com.wgzhao.addax.plugin.reader.hbase11xsqlreader;

import com.wgzhao.addax.common.base.HBaseKey;
import com.wgzhao.addax.common.element.BoolColumn;
import com.wgzhao.addax.common.element.BytesColumn;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.PhoenixInputFormat;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.apache.phoenix.mapreduce.PhoenixRecordReader;
import org.apache.phoenix.mapreduce.PhoenixRecordWritable;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by admin on 1/3/18.
 */
public class HbaseSQLReaderTask
{
    private static final Logger log = LoggerFactory.getLogger(HbaseSQLReaderTask.class);
    PhoenixInputSplit phoenixInputSplit;
    private PhoenixInputFormat phoenixInputFormat;
    private PhoenixRecordReader phoenixRecordReader;
    private Map<String, PColumn> pColumns;
    private HbaseSQLReaderConfig readerConfig;
    private TaskAttemptContextImpl hadoopAttemptContext;

    public HbaseSQLReaderTask(Configuration config)
    {
        this.readerConfig = HbaseSQLHelper.parseConfig(config);
        pColumns = new LinkedHashMap<>();
    }

    private void getPColumns()
            throws SQLException
    {
        try(Connection con =
                DriverManager.getConnection(this.readerConfig.getConnectionString()))
        {
            PhoenixConnection phoenixConnection = con.unwrap(PhoenixConnection.class);
            MetaDataClient metaDataClient = new MetaDataClient(phoenixConnection);
            PTable table = metaDataClient.updateCache("", this.readerConfig.getTableName()).getTable();
            List<String> columnNames = this.readerConfig.getColumns();
            for (PColumn pColumn : table.getColumns()) {
                if (columnNames.contains(pColumn.getName().getString())) {
                    pColumns.put(pColumn.getName().getString(), pColumn);
                }
            }
        }
    }

    public void init()
    {
        log.info("reader table info: {}", this.readerConfig);
        try {
            this.getPColumns();
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(
                    HbaseSQLReaderErrorCode.GET_PHOENIX_CONNECTIONINFO_ERROR, "获取表的列出问题，重试，若还有问题请检查hbase集群状态," + e.getMessage());
        }
        this.phoenixInputFormat = new PhoenixInputFormat<PhoenixRecordWritable>();
        String splitBase64Str = this.readerConfig.getOriginalConfig().getString(HBaseKey.SPLIT_KEY);
        byte[] splitBytes = org.apache.commons.codec.binary.Base64.decodeBase64(splitBase64Str);
        TaskAttemptID attemptId = new TaskAttemptID();
        org.apache.hadoop.conf.Configuration conf = HbaseSQLHelper.generatePhoenixConf(this.readerConfig);
        this.hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId);
        this.phoenixInputSplit = new PhoenixInputSplit();
        try {
            HadoopSerializationUtil.deserialize(phoenixInputSplit, splitBytes);
            this.phoenixRecordReader = (PhoenixRecordReader) phoenixInputFormat.createRecordReader(phoenixInputSplit, hadoopAttemptContext);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    HbaseSQLReaderErrorCode.PHOENIX_CREATEREADER_ERROR, "创建phoenix的reader出现问题,请重试，若还有问题请检查hbase集群状态," + e.getMessage());
        }
    }

    public void prepare()
    {
        try {
            this.phoenixRecordReader.initialize(this.phoenixInputSplit, hadoopAttemptContext);
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(
                    HbaseSQLReaderErrorCode.PHOENIX_READERINIT_ERROR, "phoenix的reader初始化出现问题,请重试，若还有问题请检查hbase集群状态" + e.getMessage());
        }
        catch (InterruptedException e) {
            throw AddaxException.asAddaxException(
                    HbaseSQLReaderErrorCode.PHOENIX_READERINIT_ERROR, "phoenix的reader初始化被中断,请重试," + e.getMessage());
        }
    }

    private Column convertPhoenixValueToDataxColumn(int sqlType, Object value)
            throws IOException
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
                column = new DoubleColumn(((Float) value).doubleValue());
                break;
            case Types.DECIMAL:
                column = new DoubleColumn(((BigDecimal) value));
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
                        HbaseSQLReaderErrorCode.PHOENIX_COLUMN_TYPE_CONVERT_ERROR, "遇到不可识别的phoenix类型，" + "sqlType :" + sqlType);
        }
        return column;
    }

    private void constructRecordFromPhoenix(Record record, Map<String, Object> phoenixRecord)
            throws IOException
    {
        for (Map.Entry<String, PColumn> pColumnItem : this.pColumns.entrySet()) {
            Column column = this.convertPhoenixValueToDataxColumn(
                    pColumnItem.getValue().getDataType().getSqlType(),
                    phoenixRecord.get(pColumnItem.getKey()));
            record.addColumn(column);
        }
    }

    public boolean readRecord(Record record)
            throws IOException, InterruptedException
    {
        boolean hasNext = false;
        hasNext = this.phoenixRecordReader.nextKeyValue();
        if (!hasNext) {
            return hasNext;
        }
        PhoenixRecordWritable phoenixRecordWritable = (PhoenixRecordWritable) this.phoenixRecordReader.getCurrentValue();
        Map<String, Object> phoenixRecord = phoenixRecordWritable.getResultMap();
        this.constructRecordFromPhoenix(record, phoenixRecord);
        return hasNext;
    }

    public void destroy()
    {
        if (this.phoenixRecordReader != null) {
            try {
                this.phoenixRecordReader.close();
            }
            catch (IOException e) {
                throw AddaxException.asAddaxException(
                        HbaseSQLReaderErrorCode.PHOENIX_READER_CLOSE_ERROR, "phoenix的reader close失败,请重试，若还有问题请检查hbase集群状态" + e.getMessage());
            }
        }
    }
}


