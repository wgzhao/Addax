package com.alibaba.datax.plugin.writer.dbffilewriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public enum DbfFileWriterErrorCode
        implements ErrorCode
{

    CONFIG_INVALID_EXCEPTION("DbfFileWriter-00", "您的参数配置错误."),
    REQUIRED_VALUE("DbfFileWriter-01", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("DbfFileWriter-02", "您填写的参数值不合法."),
    WRITE_FILE_ERROR("DbfFileWriter-03", "您配置的目标文件在写入时异常."),
//    Write_FILE_IO_ERROR("DbfFileWriter-04", "您配置的文件在写入时出现IO异常."),
    SECURITY_NOT_ENOUGH("DbfFileWriter-05", "您缺少权限执行相应的文件写入操作.");

    private final String code;
    private final String description;

    DbfFileWriterErrorCode(String code, String description)
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
