package com.wgzhao.addax.plugin.reader.s3reader;

import com.wgzhao.addax.common.spi.ErrorCode;

public enum S3ReaderErrorCode
        implements ErrorCode
{
    S3_EXCEPTION("S3FileReader-01", "Exception occurred when reading configure"),
    CONFIG_INVALID_EXCEPTION("S3FileReader-02", "Invalid configure"),
    NOT_SUPPORT_TYPE("S3Reader-03", "Non-supported type"),
    SECURITY_EXCEPTION("S3Reader-05", "Permission denied"),
    ILLEGAL_VALUE("S3Reader-06", "Illegal value"),
    REQUIRED_VALUE("S3Reader-07", "Missing required value"),
    NO_INDEX_VALUE("S3Reader-08", "Missing index"),
    MIXED_INDEX_VALUE("S3Reader-09", "Mix index and value"),
    EMPTY_BUCKET_EXCEPTION("S3Reader-10", "Empty bucket"),
    OBJECT_NOT_EXIST("S3Reader-11", "Object does not exists");

    private final String code;
    private final String description;

    S3ReaderErrorCode(String code, String description)
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
        return String.format("Code:[%s], Description:[%s].", this.code, this.description);
    }
}
