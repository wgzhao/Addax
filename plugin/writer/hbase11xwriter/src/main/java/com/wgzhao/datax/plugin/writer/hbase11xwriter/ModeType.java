package com.wgzhao.datax.plugin.writer.hbase11xwriter;

import com.wgzhao.datax.common.exception.DataXException;

import java.util.Arrays;

public enum ModeType
{
    NORMAL("normal"),
    MULTI_VERSION("multiVersion");

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
        throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE,
                String.format("Hbasewriter 不支持该 mode 类型:%s, 目前支持的 mode 类型是:%s", modeName, Arrays.asList(values())));
    }
}
