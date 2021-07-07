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

package com.wgzhao.addax.plugin.writer.elasticsearchwriter;

import com.wgzhao.addax.common.spi.ErrorCode;

public enum ESWriterErrorCode
        implements ErrorCode
{
    BAD_CONFIG_VALUE("ESWriter-00", "您配置的值不合法."),
//    ES_INDEX_DELETE("ESWriter-01", "删除index错误."),
//    ES_INDEX_CREATE("ESWriter-02", "创建index错误."),
    ES_MAPPINGS("ESWriter-03", "mappings错误."),
    ES_INDEX_INSERT("ESWriter-04", "插入数据错误."),
    ES_ALIAS_MODIFY("ESWriter-05", "别名修改错误."),
    ;

    private final String code;
    private final String description;

    ESWriterErrorCode(String code, String description)
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