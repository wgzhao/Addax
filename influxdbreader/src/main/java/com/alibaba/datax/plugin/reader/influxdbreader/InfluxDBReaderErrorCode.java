package com.alibaba.datax.plugin.reader.influxdbreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum InfluxDBReaderErrorCode
        implements ErrorCode
{
    REQUIRED_VALUE("InfluxDBReader-00", "缺失必要的值"),
    ILLEGAL_VALUE("InfluxDBReader-01", "值非法");

    private final String code;
    private final String description;

    InfluxDBReaderErrorCode(String code, String description)
    {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode()
    {
        return this.code;
    }

    @Override
    public String getDescription()
    {
        return this.description;
    }

    @Override
    public String toString()
    {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
