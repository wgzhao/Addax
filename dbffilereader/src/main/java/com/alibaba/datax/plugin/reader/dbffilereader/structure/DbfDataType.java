package com.alibaba.datax.plugin.reader.dbffilereader.structure;
/**
 * The supported Dbf field types.
 * Source: http://www.fship.com/dbfspecs.txt
 */
public enum DbfDataType {
    CHAR('C'),
    DATE('D'),
    FLOAT('F'),
    LOGICAL('L'),
    MEMO('M'),
    NUMERIC('N');

    public final byte byteValue;

    private DbfDataType(char byteValue) {
        this.byteValue = (byte) (byteValue & 0xff);
    }

    /* package */ static DbfDataType valueOf(byte value) {
        final DbfDataType[] values = values();
        final int count = values.length;
        for (int i = 0; i < count; i++) {
            if (values[i].byteValue == value) {
                return values[i];
            }
        }
        return null;
    }
}