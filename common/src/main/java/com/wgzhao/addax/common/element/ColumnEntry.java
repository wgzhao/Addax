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

package com.wgzhao.addax.common.element;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class ColumnEntry
{
    private Integer index;
    private String type;
    private String value;
    private String format;
    private DateFormat dateParse;

    public static String toJSONString(ColumnEntry columnEntry)
    {
        return JSON.toJSONString(columnEntry);
    }

    public Integer getIndex()
    {
        return index;
    }

    public void setIndex(Integer index)
    {
        this.index = index;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public String getValue()
    {
        return value;
    }

    public void setValue(String value)
    {
        this.value = value;
    }

    public String getFormat()
    {
        return format;
    }

    public void setFormat(String format)
    {
        this.format = format;
        if (StringUtils.isNotBlank(this.format)) {
            this.dateParse = new SimpleDateFormat(this.format);
        }
    }

    public DateFormat getDateFormat()
    {
        return this.dateParse;
    }

    public String toJSONString()
    {
        return ColumnEntry.toJSONString(this);
    }
}
