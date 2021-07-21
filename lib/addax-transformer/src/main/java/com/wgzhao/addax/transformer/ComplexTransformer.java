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

package com.wgzhao.addax.transformer;

import com.wgzhao.addax.common.element.Record;

import java.util.Map;

/**
 * no comments.
 * Created by liqiang on 16/3/3.
 */
public abstract class ComplexTransformer
{
    //transformerName的唯一性在 addax 中检查，或者提交到插件中心检查。
    private String transformerName;

    public String getTransformerName()
    {
        return transformerName;
    }

    public void setTransformerName(String transformerName)
    {
        this.transformerName = transformerName;
    }

    /**
     * @param record 行记录，UDF进行record的处理后，更新相应的record
     * @param tContext transformer运行的配置项
     * @param paras transformer函数参数
     * @return record
     */
    public abstract Record evaluate(Record record, Map<String, Object> tContext, Object... paras);
}
