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

package com.wgzhao.datax.core.transport.transformer;

import com.wgzhao.datax.common.spi.ErrorCode;

public enum TransformerErrorCode
        implements ErrorCode
{
    //重复命名
    TRANSFORMER_NAME_ERROR("TransformerErrorCode-01", "Transformer name illegal"),
    TRANSFORMER_DUPLICATE_ERROR("TransformerErrorCode-02", "Transformer name has existed"),
    TRANSFORMER_NOTFOUND_ERROR("TransformerErrorCode-03", "Transformer name not found"),
    TRANSFORMER_CONFIGURATION_ERROR("TransformerErrorCode-04", "Transformer configuration error"),
    TRANSFORMER_ILLEGAL_PARAMETER("TransformerErrorCode-05", "Transformer parameter illegal"),
    TRANSFORMER_RUN_EXCEPTION("TransformerErrorCode-06", "Transformer run exception"),
    TRANSFORMER_GROOVY_INIT_EXCEPTION("TransformerErrorCode-07", "Transformer Groovy init exception"),
    ;

    private final String code;

    private final String description;

    TransformerErrorCode(String code, String description)
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
