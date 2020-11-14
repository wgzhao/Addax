package com.alibaba.datax.plugin.reader.hbase20xreader;

import com.alibaba.datax.common.exception.DataXException;

import java.util.Arrays;

public enum ModeType
{
    Normal("normal"),
    MultiVersionFixedColumn("multiVersionFixedColumn");

    private final String mode;

    ModeType(String mode)
    {
        this.mode = mode.toLowerCase();
    }

    public static ModeType getByTypeName(String modeName)
    {
        for (ModeType modeType : values()) {
            if (modeType.mode.equalsIgnoreCase(modeName)) {
                return modeType;
            }
        }
        throw DataXException.asDataXException(Hbase20xReaderErrorCode.ILLEGAL_VALUE,
                String.format("HbaseReader 不支持该 mode 类型:%s, 目前支持的 mode 类型是:%s", modeName, Arrays.asList(values())));
    }

    public String getMode()
    {
        return mode;
    }
}
