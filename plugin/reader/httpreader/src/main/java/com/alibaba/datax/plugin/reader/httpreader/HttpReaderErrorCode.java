package com.alibaba.datax.plugin.reader.httpreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum HttpReaderErrorCode
        implements ErrorCode
{
    REQUIRED_VALUE("HttpReader-00","您缺失了必须填写的参数值."),

    ILLEGAL_VALUE("HttpReader-01","您填写的参数值不合法."),

    FILE_NOT_EXISTS("HttpReader-04","您配置的没有权限读取."),

//        OPEN_FILE_WITH_CHARSET_ERROR("HttpReader-05", "您配置的文件编码和实际文件编码不符合."),

    //    READ_FILE_IO_ERROR("HttpReader-07", "您配置的文件在读取时出现IO异常."),
//    SECURITY_NOT_ENOUGH("HttpReader-08", "您缺少权限执行相应的文件操作."),
//    CONFIG_INVALID_EXCEPTION("HttpReader-09", "您的参数配置错误."),
//    RUNTIME_EXCEPTION("HttpReader-10", "出现运行时异常, 请联系我们"),

    FAIL_LOGIN("HttpReader-12","登录失败,无法与http服务器建立连接."),

    FAIL_DISCONNECT("HttpReader-13","关闭http连接失败,无法与http服务器断开连接."),

    COMMAND_FTP_IO_EXCEPTION("HttpReader-14","与http服务器连接异常."),
    NOT_SUPPORT("HttpReader-15", "暂不支持该方式"),
            ;

    private final String code;
    private final String description;

    HttpReaderErrorCode(String code, String description)
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
}
