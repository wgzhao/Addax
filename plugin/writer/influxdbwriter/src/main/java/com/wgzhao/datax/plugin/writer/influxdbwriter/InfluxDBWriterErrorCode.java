package com.wgzhao.datax.plugin.writer.influxdbwriter;

import com.wgzhao.datax.common.spi.ErrorCode;

public enum InfluxDBWriterErrorCode
        implements ErrorCode
{
    REQUIRED_VALUE("InfluxDBWriter-00","缺失必要的值"),
    ILLEGAL_VALUE("InfluxDBWriter-01","值非法"),
    CONF_ERROR("InfluxDBWriter-02", "您的配置错误."),
    CONNECT_ERROR("InfluxDBWriter-03","连接错误"),
    WRITER_ERROR("InfluxDBWriter-04", "写入错误");

    private final String code;
    private final String description;

    InfluxDBWriterErrorCode(String code, String description)
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
