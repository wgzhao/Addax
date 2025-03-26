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

package com.wgzhao.addax.plugin.reader.hbase20xreader;

import com.wgzhao.addax.core.base.HBaseKey;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.element.Record;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Map;

public class NormalTask
        extends HbaseAbstractTask
{
    private final List<HbaseColumnCell> hbaseColumnCells;

    public NormalTask(Configuration configuration)
    {
        super(configuration);
        List<Map> column = configuration.getList(HBaseKey.COLUMN, Map.class);
        this.hbaseColumnCells = Hbase20xHelper.parseColumnOfNormalMode(column);
    }

    /**
     * normal模式下将用户配置的column 设置到scan中
     */
    @Override
    public void initScan(Scan scan)
    {
        boolean isConstant;
        boolean isRowkeyColumn;
        for (HbaseColumnCell cell : this.hbaseColumnCells) {
            isConstant = cell.isConstant();
            isRowkeyColumn = Hbase20xHelper.isRowkeyColumn(cell.getColumnName());
            if (!isConstant && !isRowkeyColumn) {
                this.scan.addColumn(cell.getColumnFamily(), cell.getQualifier());
            }
        }
    }

    @Override
    public boolean fetchLine(Record record)
            throws Exception
    {
        Result result = super.getNextHbaseRow();

        if (null == result) {
            return false;
        }
        super.lastResult = result;

        try {
            byte[] hbaseColumnValue;
            String columnName;
            ColumnType columnType;

            byte[] columnFamily;
            byte[] qualifier;

            for (HbaseColumnCell cell : this.hbaseColumnCells) {
                columnType = cell.getColumnType();
                if (cell.isConstant()) {
                    // 对常量字段的处理
                    String constantValue = cell.getColumnValue();

                    Column constantColumn = super.convertValueToAssignType(columnType, constantValue, cell.getDateFormat());
                    record.addColumn(constantColumn);
                }
                else {
                    // 根据列名称获取值
                    columnName = cell.getColumnName();
                    if (Hbase20xHelper.isRowkeyColumn(columnName)) {
                        hbaseColumnValue = result.getRow();
                    }
                    else {
                        columnFamily = cell.getColumnFamily();
                        qualifier = cell.getQualifier();
                        hbaseColumnValue = result.getValue(columnFamily, qualifier);
                    }

                    Column hbaseColumn = super.convertBytesToAssignType(columnType, hbaseColumnValue, cell.getDateFormat());
                    record.addColumn(hbaseColumn);
                }
            }
        }
        catch (Exception e) {
            // 注意，这里catch的异常，期望是byte数组转换失败的情况。而实际上，string的byte数组，转成整数类型是不容易报错的。但是转成double类型容易报错。
            record.setColumn(0, new StringColumn(Bytes.toStringBinary(result.getRow())));
            throw e;
        }
        return true;
    }
}
