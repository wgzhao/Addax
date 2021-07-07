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

package com.wgzhao.addax.core.transport.transformer;

import java.util.List;
import java.util.Map;

/**
 * no comments.
 * Created by liqiang on 16/3/16.
 */
public class TransformerExecutionParas
{

    /**
     * 以下是function参数
     */

    private Integer columnIndex;
    private String[] paras;
    private Map<String, Object> tContext;
    private String code;
    private List<String> extraPackage;

    public Integer getColumnIndex()
    {
        return columnIndex;
    }

    public void setColumnIndex(Integer columnIndex)
    {
        this.columnIndex = columnIndex;
    }

    public String[] getParas()
    {
        return paras;
    }

    public void setParas(String[] paras)
    {
        this.paras = paras;
    }

    public Map<String, Object> gettContext()
    {
        return tContext;
    }

    public void settContext(Map<String, Object> tContext)
    {
        this.tContext = tContext;
    }

    public String getCode()
    {
        return code;
    }

    public void setCode(String code)
    {
        this.code = code;
    }

    public List<String> getExtraPackage()
    {
        return extraPackage;
    }

    public void setExtraPackage(List<String> extraPackage)
    {
        this.extraPackage = extraPackage;
    }
}
