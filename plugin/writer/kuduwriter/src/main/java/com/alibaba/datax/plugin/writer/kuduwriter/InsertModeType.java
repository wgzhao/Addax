package com.alibaba.datax.plugin.writer.kuduwriter;

import com.alibaba.datax.common.exception.DataXException;

import java.util.Arrays;


public enum InsertModeType
{
    Insert("insert"),
    Upsert("upsert"),
    Update("update");
    private final String mode;

    InsertModeType(String mode)
    {
        this.mode = mode.toLowerCase();
    }

    public static InsertModeType getByTypeName(String modeName)
    {
        for (InsertModeType modeType : values()) {
            if (modeType.mode.equalsIgnoreCase(modeName)) {
                return modeType;
            }
        }
        throw DataXException.asDataXException(KuduWriterErrorCode.ILLEGAL_VALUE,
                String.format("Kuduwriter does not support the mode :[%s], " +
                        "currently supported mode types are :%s", modeName, Arrays.asList(values())));
    }

    public String getMode()
    {
        return mode;
    }
}
