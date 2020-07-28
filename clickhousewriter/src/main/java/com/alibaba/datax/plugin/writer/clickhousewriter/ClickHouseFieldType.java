package com.alibaba.datax.plugin.writer.clickhousewriter;

public enum ClickhouseFieldType {
    UINT8,
    UINT16,
    UINT32,
    UINT64,
    INT8,
    INT16,
    INT32,
    INT64,
    FLOAT32,
    FLOAT64,
    DECIMAL,
    DATE,
    DATETIME,
    ARRAY;

    public static ClickhouseFieldType getCHFieldType(String type) {
        if (type == null) {
            return null;
        }
        for (ClickhouseFieldType f : ClickhouseFieldType.values()) {
            if (f.name().compareTo(type.toUpperCase()) == 0) {
                return f;
            }
        }
        return null;
    }
}
