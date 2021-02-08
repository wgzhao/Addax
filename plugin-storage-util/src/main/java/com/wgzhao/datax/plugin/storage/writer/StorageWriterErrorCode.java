package com.wgzhao.datax.plugin.storage.writer;

import com.wgzhao.datax.common.spi.ErrorCode;

public enum StorageWriterErrorCode
        implements ErrorCode
{
    ILLEGAL_VALUE("UnstructuredStorageWriter-00", "您填写的参数值不合法."),
    WRITE_FILE_WITH_CHARSET_ERROR("UnstructuredStorageWriter-01", "您配置的编码未能正常写入."),
    WRITE_FILE_IO_ERROR("UnstructuredStorageWriter-02", "您配置的文件在写入时出现IO异常."),
    RUNTIME_EXCEPTION("UnstructuredStorageWriter-03", "出现运行时异常, 请联系我们"),
    REQUIRED_VALUE("UnstructuredStorageWriter-04", "您缺失了必须填写的参数值."),
    ;

    private final String code;
    private final String description;

    StorageWriterErrorCode(String code, String description)
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
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }
}
