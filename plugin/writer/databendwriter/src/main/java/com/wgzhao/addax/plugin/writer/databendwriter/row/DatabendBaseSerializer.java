package com.wgzhao.addax.plugin.writer.databendwriter.row;

import com.wgzhao.addax.common.element.Column;

public class DatabendBaseSerializer
{

    protected String fieldConvertion(Column col)
    {
        if (null == col.getRawData() || Column.Type.NULL == col.getType()) {
            return null;
        }
        if (Column.Type.BOOL == col.getType()) {
            return String.valueOf(col.asLong());
        }
        if (Column.Type.BYTES == col.getType()) {
            byte[] bts = (byte[]) col.getRawData();
            long value = 0;
            for (int i = 0; i < bts.length; i++) {
                value += (bts[bts.length - i - 1] & 0xffL) << (8 * i);
            }
            return String.valueOf(value);
        }
        return col.asString();
    }
}
