package com.wgzhao.addax.plugin.writer.starrockswriter.row;

import com.wgzhao.addax.core.element.Record;

public class StarRocksCsvSerializer
        extends StarRocksBaseSerializer
        implements StarRocksISerializer
{

    private static final long serialVersionUID = 1L;

    private final String columnSeparator;

    public StarRocksCsvSerializer(String sp)
    {
        this.columnSeparator = StarRocksDelimiterParser.parse(sp, "\t");
    }

    @Override
    public String serialize(Record row)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.getColumnNumber(); i++) {
            String value = fieldConvertion(row.getColumn(i));
            sb.append(null == value ? "\\N" : value);
            if (i < row.getColumnNumber() - 1) {
                sb.append(columnSeparator);
            }
        }
        return sb.toString();
    }
}
