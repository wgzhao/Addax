package com.alibaba.datax.plugin.reader.hbase20xreader;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.util.Configuration;
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
    public boolean fetchLine(com.alibaba.datax.common.element.Record record)
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

    private void convertCellToLine(Cell cell, com.alibaba.datax.common.element.Record record)
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
