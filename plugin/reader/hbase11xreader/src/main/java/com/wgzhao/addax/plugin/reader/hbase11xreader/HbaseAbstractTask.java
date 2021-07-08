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

package com.wgzhao.addax.plugin.reader.hbase11xreader;

import com.wgzhao.addax.common.element.BoolColumn;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;

public abstract class HbaseAbstractTask
{
    private final static Logger LOG = LoggerFactory.getLogger(HbaseAbstractTask.class);

    private final byte[] startKey;
    private final byte[] endKey;

    protected Table htable;
    protected String encoding;
    protected int scanCacheSize;
    protected int scanBatchSize;

    protected Result lastResult = null;
    protected Scan scan;
    protected ResultScanner resultScanner;

    public HbaseAbstractTask(Configuration configuration)
    {

        this.htable = Hbase11xHelper.getTable(configuration);

        this.encoding = configuration.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
        this.startKey = Hbase11xHelper.convertInnerStartRowkey(configuration);
        this.endKey = Hbase11xHelper.convertInnerEndRowkey(configuration);
        this.scanCacheSize = configuration.getInt(Key.SCAN_CACHE_SIZE, Constant.DEFAULT_SCAN_CACHE_SIZE);
        this.scanBatchSize = configuration.getInt(Key.SCAN_BATCH_SIZE, Constant.DEFAULT_SCAN_BATCH_SIZE);
    }

    public abstract boolean fetchLine(Record record)
            throws Exception;

    //不同模式设置不同,如多版本模式需要设置版本
    public abstract void initScan(Scan scan);

    public void prepare()
            throws Exception
    {
        this.scan = new Scan();
        this.scan.setSmall(false);
        this.scan.withStartRow(startKey);
        this.scan.withStopRow(endKey);
        LOG.info("The task set startRowkey=[{}], endRowkey=[{}].", Bytes.toStringBinary(this.startKey), Bytes.toStringBinary(this.endKey));
        //scan的Caching Batch全部留在hconfig中每次从服务器端读取的行数，设置默认值未256
        this.scan.setCaching(this.scanCacheSize);
        //设置获取记录的列个数，hbase默认无限制，也就是返回所有的列,这里默认是100
        this.scan.setBatch(this.scanBatchSize);
        //为是否缓存块，hbase默认缓存,同步全部数据时非热点数据，因此不需要缓存
        this.scan.setCacheBlocks(false);
        initScan(this.scan);

        this.resultScanner = this.htable.getScanner(this.scan);
    }

    public void close()
    {
        Hbase11xHelper.closeResultScanner(this.resultScanner);
        Hbase11xHelper.closeTable(this.htable);
    }

    protected Result getNextHbaseRow()
            throws IOException
    {
        Result result;
        try {
            result = resultScanner.next();
        }
        catch (IOException e) {
            if (lastResult != null) {
                this.scan.withStopRow(lastResult.getRow());
            }
            resultScanner = this.htable.getScanner(scan);
            result = resultScanner.next();
            if (lastResult != null && Bytes.equals(lastResult.getRow(), result.getRow())) {
                result = resultScanner.next();
            }
        }
        lastResult = result;
        // may be null
        return result;
    }

    public Column convertBytesToAssignType(ColumnType columnType, byte[] byteArray, String dateformat)
            throws UnsupportedEncodingException, ParseException
    {
        Column column;
        boolean isEmpty = ArrayUtils.isEmpty(byteArray);
        switch (columnType) {
            case BOOLEAN:
                column = new BoolColumn(isEmpty ? null : Bytes.toBoolean(byteArray));
                break;
            case SHORT:
                column = new LongColumn(isEmpty ? null : String.valueOf(Bytes.toShort(byteArray)));
                break;
            case INT:
                column = new LongColumn(isEmpty ? null : Bytes.toInt(byteArray));
                break;
            case LONG:
                column = new LongColumn(isEmpty ? null : Bytes.toLong(byteArray));
                break;
            case FLOAT:
                column = new DoubleColumn(isEmpty ? null : Bytes.toFloat(byteArray));
                break;
            case DOUBLE:
                column = new DoubleColumn(isEmpty ? null : Bytes.toDouble(byteArray));
                break;
            case STRING:
                column = new StringColumn(isEmpty ? null : new String(byteArray, encoding));
                break;
            case BINARY_STRING:
                column = new StringColumn(isEmpty ? null : Bytes.toStringBinary(byteArray));
                break;
            case DATE:
                String dateValue = Bytes.toStringBinary(byteArray);
                column = new DateColumn(isEmpty ? null : DateUtils.parseDate(dateValue, dateformat));
                break;
            default:
                throw AddaxException.asAddaxException(Hbase11xReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 不支持您配置的列类型:" + columnType);
        }
        return column;
    }

    public Column convertValueToAssignType(ColumnType columnType, String constantValue, String dateformat)
            throws ParseException
    {
        Column column;
        switch (columnType) {
            case BOOLEAN:
                column = new BoolColumn(constantValue);
                break;
            case SHORT:
            case INT:
            case LONG:
                column = new LongColumn(constantValue);
                break;
            case FLOAT:
            case DOUBLE:
                column = new DoubleColumn(constantValue);
                break;
            case STRING:
                column = new StringColumn(constantValue);
                break;
            case DATE:
                column = new DateColumn(DateUtils.parseDate(constantValue, dateformat));
                break;
            default:
                throw AddaxException.asAddaxException(Hbase11xReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 常量列不支持您配置的列类型:" + columnType);
        }
        return column;
    }
}
