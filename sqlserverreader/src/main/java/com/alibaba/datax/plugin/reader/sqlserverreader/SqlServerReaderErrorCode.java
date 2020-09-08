package com.alibaba.datax.plugin.reader.sqlserverreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum SqlServerReaderErrorCode implements ErrorCode {
    ;

    private final String code;
    private final String description;

    SqlServerReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

}
