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

package com.wgzhao.datax.plugin.reader.elasticsearchreader;

import com.wgzhao.datax.common.spi.ErrorCode;

public enum ESReaderErrorCode
        implements ErrorCode
{
    BAD_CONFIG_VALUE("ESReader-00", "您配置的值不合法."),
    ES_SEARCH_ERROR("ESReader-01", "search出错."),
    ES_INDEX_NOT_EXISTS("ESReader-02", "index不存在."),
    UNKNOWN_DATA_TYPE("ESReader-03", "无法识别的数据类型."),
    COLUMN_CANT_BE_EMPTY("ESReader-04", "column不能为空."),
    ;

    private final String code;
    private final String description;

    ESReaderErrorCode(String code, String description)
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