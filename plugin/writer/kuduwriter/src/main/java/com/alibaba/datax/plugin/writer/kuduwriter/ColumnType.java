package com.alibaba.datax.plugin.writer.kuduwriter;

import com.alibaba.datax.common.exception.DataXException;

import java.util.Arrays;

public enum ColumnType
{
    INT("int"),
    FLOAT("float"),
    STRING("string"),
    BIGINT("bigint"),
    DOUBLE("double"),
    BOOLEAN("boolean"),
    LONG("long");
    private final String mode;

    ColumnType(String mode)
    {
        this.mode = mode.toLowerCase();
    }

    public static ColumnType getByTypeName(String modeName)
    {
        for (ColumnType modeType : values()) {
            if (modeType.mode.equalsIgnoreCase(modeName)) {
                return modeType;
            }
        }
        throw DataXException.asDataXException(KuduWriterErrorCode.ILLEGAL_VALUE,
                String.format("Kuduwriter does not support the type:%s, currently supported types are:%s",
                        modeName, Arrays.asList(values())));
    }

    public String getMode()
    {
        return mode;
    }
}
