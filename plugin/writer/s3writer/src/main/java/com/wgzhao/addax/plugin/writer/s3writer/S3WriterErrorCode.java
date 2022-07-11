package com.wgzhao.addax.plugin.writer.s3writer;

import com.wgzhao.addax.common.spi.ErrorCode;

public enum S3WriterErrorCode implements ErrorCode
{
    CONFIG_INVALID_EXCEPTION("S3Writer-00", "Invalid configure."),
    REQUIRED_VALUE("S3Writer-01", "Missing required parameters"),
    ILLEGAL_VALUE("S3Writer-02", "Illegal value"),
    WRITE_OBJECT_ERROR("S3Writer-03", "Failure to write object "),
    S3_COMM_ERROR("S3Writer-05", "S3 object operation error");

    private final String code;
    private final String description;

    S3WriterErrorCode(String code, String description) {
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

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code, this.description);
    }
}
