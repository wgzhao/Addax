package com.wgzhao.datax.plugin.reader.mysqlreader;

import com.wgzhao.datax.common.spi.ErrorCode;

public enum MysqlReaderErrorCode
        implements ErrorCode
{
    ;

    private final String code;
    private final String description;

    MysqlReaderErrorCode(String code, String description)
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
