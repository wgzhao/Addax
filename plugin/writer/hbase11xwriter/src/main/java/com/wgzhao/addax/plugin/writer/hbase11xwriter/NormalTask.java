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

package com.wgzhao.addax.plugin.writer.hbase11xwriter;

import com.wgzhao.addax.common.base.HBaseKey;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.wgzhao.addax.common.exception.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.exception.ErrorCode.ILLEGAL_VALUE;

public class NormalTask
        extends HbaseAbstractTask
{
    private static final Logger LOG = LoggerFactory.getLogger(NormalTask.class);

    public NormalTask(Configuration configuration)
    {
        super(configuration);
    }

    @Override
    public Put convertRecordToPut(Record record)
    {
        byte[] rowkey = getRowkey(record);
        Put put;
        if (this.versionColumn == null) {
            put = new Put(rowkey);
            if (!super.walFlag) {
                //等价与0.94 put.setWriteToWAL(super.walFlag)
                put.setDurability(Durability.SKIP_WAL);
            }
        }
        else {
            long timestamp = getVersion(record);
            put = new Put(rowkey, timestamp);
        }
        for (Configuration aColumn : columns) {
            Integer index = aColumn.getInt(HBaseKey.INDEX);
            String type = aColumn.getString(HBaseKey.TYPE);
            ColumnType columnType = ColumnType.getByTypeName(type);
            String name = aColumn.getString(HBaseKey.NAME);
            String promptInfo = "The name[" + name + "] of column should be cf:qualifier";
            String[] cfAndQualifier = name.split(":");
            Validate.isTrue(cfAndQualifier.length == 2 && StringUtils.isNotBlank(cfAndQualifier[0]) && StringUtils.isNotBlank(cfAndQualifier[1]), promptInfo);
            if (index >= record.getColumnNumber()) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("The field[index] of column is out-range, it should be less than %s. actually got %s.", record.getColumnNumber(), index));
            }
            byte[] columnBytes = getColumnByte(columnType, record.getColumn(index));
            //columnBytes 为null忽略这列
            if (null != columnBytes) {
                put.addColumn(Bytes.toBytes(cfAndQualifier[0]), Bytes.toBytes(cfAndQualifier[1]), columnBytes);
            }
        }
        return put;
    }

    public byte[] getRowkey(Record record)
    {
        byte[] rowkeyBuffer = {};
        for (Configuration aRowkeyColumn : rowkeyColumn) {
            Integer index = aRowkeyColumn.getInt(HBaseKey.INDEX);
            String type = aRowkeyColumn.getString(HBaseKey.TYPE);
            ColumnType columnType = ColumnType.getByTypeName(type);
            if (index == -1) {
                String value = aRowkeyColumn.getString(HBaseKey.VALUE);
                rowkeyBuffer = Bytes.add(rowkeyBuffer, getValueByte(columnType, value));
            }
            else {
                if (index >= record.getColumnNumber()) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            String.format("The field[index] of rowkeyColumn is out-range, it should be less than %s. actually got %s.", record.getColumnNumber(), index));
                }
                byte[] value = getColumnByte(columnType, record.getColumn(index));
                rowkeyBuffer = Bytes.add(rowkeyBuffer, value);
            }
        }
        return rowkeyBuffer;
    }

    public long getVersion(Record record)
    {
        int index = versionColumn.getInt(HBaseKey.INDEX);
        long timestamp;
        if (index == -1) {
            //指定时间作为版本
            timestamp = versionColumn.getLong(HBaseKey.VALUE);
            if (timestamp < 0) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, "Illegal timestamp version");
            }
        }
        else {
            //指定列作为版本,long/doubleColumn直接record.asLong, 其它类型尝试用yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss SSS去format
            if (index >= record.getColumnNumber()) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        String.format("The field[index] of versionColumn is out-range, it should be less than %s. actually got %s.", record.getColumnNumber(), index));
            }
            if (record.getColumn(index).getRawData() == null) {
                throw AddaxException.asAddaxException(CONFIG_ERROR, "The version is empty");
            }
            SimpleDateFormat dfSeconds = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat dfMs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
            if (record.getColumn(index) instanceof LongColumn || record.getColumn(index) instanceof DoubleColumn) {
                timestamp = record.getColumn(index).asLong();
            }
            else {
                Date date;
                try {
                    date = dfMs.parse(record.getColumn(index).asString());
                }
                catch (ParseException e) {
                    try {
                        date = dfSeconds.parse(record.getColumn(index).asString());
                    }
                    catch (ParseException e1) {
                        LOG.info(String.format("The value of version %s can not parsed as Date type with 'yyyy-MM-dd HH:mm:ss' and 'yyyy-MM-dd HH:mm:ss SSS' format", index));
                        throw AddaxException.asAddaxException(ILLEGAL_VALUE, e1);
                    }
                }
                timestamp = date.getTime();
            }
        }
        return timestamp;
    }
}