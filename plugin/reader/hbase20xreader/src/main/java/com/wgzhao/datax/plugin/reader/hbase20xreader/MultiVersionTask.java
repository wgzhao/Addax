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

package com.wgzhao.datax.plugin.reader.hbase20xreader;

import com.wgzhao.datax.common.element.LongColumn;
import com.wgzhao.datax.common.util.Configuration;
import com.wgzhao.datax.common.element.Record;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class MultiVersionTask
        extends HbaseAbstractTask
{
    private static final byte[] colonByte = ":".getBytes(StandardCharsets.UTF_8);
    public static List<Map> column;
    private final int maxVersion;
    private final HashMap<String, HashMap<String, String>> familyQualifierMap;
    private Cell[] cellArr = null;
    private int currentReadPosition = 0;

    protected MultiVersionTask(Configuration configuration)
    {
        super(configuration);

        this.maxVersion = configuration.getInt(Key.MAX_VERSION);
        this.column = configuration.getList(Key.COLUMN, Map.class);
        this.familyQualifierMap = Hbase20xHelper.parseColumnOfMultiversionMode(this.column);
    }

    @Override
    public boolean fetchLine(Record record)
            throws Exception
    {
        Result result;
        if (this.cellArr == null || this.cellArr.length == this.currentReadPosition) {
            result = super.getNextHbaseRow();
            if (result == null) {
                return false;
            }
            super.lastResult = result;

            this.cellArr = result.rawCells();
            if (this.cellArr == null || this.cellArr.length == 0) {
                return false;
            }
            this.currentReadPosition = 0;
        }
        try {
            Cell cell = this.cellArr[this.currentReadPosition];

            convertCellToLine(cell, record);
        }
        finally {
            this.currentReadPosition++;
        }
        return true;
    }

    private void convertCellToLine(Cell cell, Record record)
            throws Exception
    {
        byte[] rawRowkey = CellUtil.cloneRow(cell);
        long timestamp = cell.getTimestamp();
        byte[] cfAndQualifierName = Bytes.add(CellUtil.cloneFamily(cell), MultiVersionTask.colonByte, CellUtil.cloneQualifier(cell));
        byte[] columnValue = CellUtil.cloneValue(cell);

        ColumnType rawRowkeyType = ColumnType.getByTypeName(familyQualifierMap.get(Constant.ROWKEY_FLAG).get(Key.TYPE));
        String familyQualifier = new String(cfAndQualifierName, StandardCharsets.UTF_8);
        ColumnType columnValueType = ColumnType.getByTypeName(familyQualifierMap.get(familyQualifier).get(Key.TYPE));
        String columnValueFormat = familyQualifierMap.get(familyQualifier).get(Key.FORMAT);
        if (StringUtils.isBlank(columnValueFormat)) {
            columnValueFormat = Constant.DEFAULT_DATA_FORMAT;
        }

        record.addColumn(convertBytesToAssignType(rawRowkeyType, rawRowkey, columnValueFormat));
        record.addColumn(convertBytesToAssignType(ColumnType.STRING, cfAndQualifierName, columnValueFormat));
        // 直接忽略了用户配置的 timestamp 的类型
        record.addColumn(new LongColumn(timestamp));
        record.addColumn(convertBytesToAssignType(columnValueType, columnValue, columnValueFormat));
    }

    public void setMaxVersions(Scan scan)
    {
        if (this.maxVersion == -1 || this.maxVersion == Integer.MAX_VALUE) {
            scan.readAllVersions();
        }
        else {
            scan.readVersions(this.maxVersion);
        }
    }
}
