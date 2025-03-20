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

import com.wgzhao.addax.core.base.HBaseConstant;
import com.wgzhao.addax.core.base.HBaseKey;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;

public abstract class HbaseAbstractTask
{
    private static final Logger LOG = LoggerFactory.getLogger(HbaseAbstractTask.class);

    public NullModeType nullMode;

    public List<Configuration> columns;
    public List<Configuration> rowkeyColumn;
    public Configuration versionColumn;

    //public Table htable
    public String encoding;
    public Boolean walFlag;
    public BufferedMutator bufferedMutator;

    public HbaseAbstractTask(Configuration configuration)
    {
        //this.htable = Hbase11xHelper.getTable(configuration)
        this.columns = configuration.getListConfiguration(HBaseKey.COLUMN);
        this.rowkeyColumn = configuration.getListConfiguration(HBaseKey.ROW_KEY_COLUMN);
        this.versionColumn = configuration.getConfiguration(HBaseKey.VERSION_COLUMN);
        this.encoding = configuration.getString(HBaseKey.ENCODING, HBaseConstant.DEFAULT_ENCODING);
        this.nullMode = NullModeType.getByTypeName(configuration.getString(HBaseKey.NULL_MODE, HBaseConstant.DEFAULT_NULL_MODE));
        this.walFlag = configuration.getBool(HBaseKey.WAL_FLAG, false);
        this.bufferedMutator = Hbase11xHelper.getBufferedMutator(configuration);
    }

    public void startWriter(RecordReceiver lineReceiver, TaskPluginCollector taskPluginCollector)
    {
        Record record;
        try {
            while ((record = lineReceiver.getFromReader()) != null) {
                Put put;
                try {
                    put = convertRecordToPut(record);
                }
                catch (Exception e) {
                    taskPluginCollector.collectDirtyRecord(record, e);
                    continue;
                }
                try {
                    this.bufferedMutator.mutate(put);
                }
                catch (IllegalArgumentException e) {
                    if (e.getMessage().equals("No columns to insert") && nullMode.equals(NullModeType.SKIP)) {
                        LOG.info("The record is empty, it will ignore because the item nullMode is configured as skip. record[{}]", record);
                    }
                    else {
                        taskPluginCollector.collectDirtyRecord(record, e);
                    }
                }
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR, e);
        }
        finally {
            Hbase11xHelper.closeBufferedMutator(this.bufferedMutator);
        }
    }

    public abstract Put convertRecordToPut(Record record);

    public void close()
    {
        Hbase11xHelper.closeBufferedMutator(this.bufferedMutator);
    }

    public byte[] getColumnByte(ColumnType columnType, Column column)
    {
        byte[] bytes;
        if (column.getRawData() != null) {
            switch (columnType) {
                case INT:
                    bytes = Bytes.toBytes(column.asLong().intValue());
                    break;
                case LONG:
                    bytes = Bytes.toBytes(column.asLong());
                    break;
                case DOUBLE:
                    bytes = Bytes.toBytes(column.asDouble());
                    break;
                case FLOAT:
                    bytes = Bytes.toBytes(column.asDouble().floatValue());
                    break;
                case SHORT:
                    bytes = Bytes.toBytes(column.asLong().shortValue());
                    break;
                case BOOLEAN:
                    bytes = Bytes.toBytes(column.asBoolean());
                    break;
                case STRING:
                    bytes = this.getValueByte(columnType, column.asString());
                    break;
                default:
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The data type " + columnType + "is unsupported");
            }
        }
        else {
            switch (nullMode) {
                case SKIP:
                    bytes = null;
                    break;
                case EMPTY:
                    bytes = HConstants.EMPTY_BYTE_ARRAY;
                    break;
                default:
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The item nullMode must be configured either skip or empty");
            }
        }
        return bytes;
    }

    public byte[] getValueByte(ColumnType columnType, String value)
    {
        byte[] bytes;
        if (value != null) {
            switch (columnType) {
                case INT:
                    bytes = Bytes.toBytes(Integer.parseInt(value));
                    break;
                case LONG:
                    bytes = Bytes.toBytes(Long.parseLong(value));
                    break;
                case DOUBLE:
                    bytes = Bytes.toBytes(Double.parseDouble(value));
                    break;
                case FLOAT:
                    bytes = Bytes.toBytes(Float.parseFloat(value));
                    break;
                case SHORT:
                    bytes = Bytes.toBytes(Short.parseShort(value));
                    break;
                case BOOLEAN:
                    bytes = Bytes.toBytes(Boolean.parseBoolean(value));
                    break;
                case STRING:
                    bytes = value.getBytes(Charset.forName(encoding));
                    break;
                default:
                    throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE, "The data type " + columnType + "is unsupported");
            }
        }
        else {
            bytes = HConstants.EMPTY_BYTE_ARRAY;
        }
        return bytes;
    }
}
