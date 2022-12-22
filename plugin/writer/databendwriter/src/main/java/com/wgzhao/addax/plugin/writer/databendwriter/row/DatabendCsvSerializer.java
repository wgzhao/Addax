package com.wgzhao.addax.plugin.writer.databendwriter.row;

import com.wgzhao.addax.common.element.Record;

public class DatabendCsvSerializer
        extends DatabendBaseSerializer
        implements DatabendISerializer
{

    private static final long serialVersionUID = 1L;

    private final String columnSeparator;

    public DatabendCsvSerializer(String sp)
    {
        this.columnSeparator = DatabendDelimiterParser.parse(sp, "\t");
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
