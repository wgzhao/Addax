/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.plugin.reader.excelreader;

import com.wgzhao.addax.common.spi.ErrorCode;

/**
 * Created by haiwei.luo on 14-9-20.
 */
public enum ExcelReaderErrorCode
        implements ErrorCode
{
    REQUIRED_VALUE("ExcelReader-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("ExcelReader-01", "您填写的参数值不合法."),
    MIXED_INDEX_VALUE("ExcelReader-02", "您的列信息配置同时包含了index,value."),
    NO_INDEX_VALUE("ExcelReader-03", "您明确的配置列信息,但未填写相应的index,value."),
    FILE_NOT_EXISTS("ExcelReader-04", "您配置的目录文件路径不存在."),
//    OPEN_FILE_WITH_CHARSET_ERROR("ExcelReader-05", "您配置的文件编码和实际文件编码不符合."),
    OPEN_FILE_ERROR("ExcelReader-06", "您配置的文件在打开时异常,建议您检查源目录是否有隐藏文件,管道文件等特殊文件."),
//    READ_FILE_IO_ERROR("ExcelReader-07", "您配置的文件在读取时出现IO异常."),
    SECURITY_NOT_ENOUGH("ExcelReader-08", "您缺少权限执行相应的文件操作."),
    CONFIG_INVALID_EXCEPTION("ExcelReader-09", "您的参数配置错误."),
//    RUNTIME_EXCEPTION("ExcelReader-10", "出现运行时异常, 请联系我们"),
    EMPTY_DIR_EXCEPTION("ExcelReader-11", "您尝试读取的文件目录为空."),
    NOT_SUPPORT_TYPE("ExcelReader-12", "您配置的列类型暂不支持."),
    OPEN_FILE_WITH_CHARSET_ERROR("ExcelReader-13", "您配置的编码和实际存储编码不符合."),
    //    OPEN_FILE_ERROR("UnstructuredStorageReader-08", "您配置的源在打开时异常,建议您检查源源是否有隐藏实体,管道文件等特殊文件."),
    READ_FILE_IO_ERROR("ExcelReader-14", "您配置的文件在读取时出现IO异常."),
    RUNTIME_EXCEPTION("ExcelReader-15", "出现运行时异常, 请联系我们")
    ;

    private final String code;
    private final String description;

    ExcelReaderErrorCode(String code, String description)
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
