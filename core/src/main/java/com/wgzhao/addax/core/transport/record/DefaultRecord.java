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

package com.wgzhao.addax.core.transport.record;

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.core.util.ClassSize;
import com.wgzhao.addax.core.util.FrameworkErrorCode;
import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jingxing on 14-8-24.
 */

public class DefaultRecord
        implements Record
{

    private static final int RECORD_AVERAGE_COLUMN_NUMBER = 16;

    private final List<Column> columns;

    private int byteSize;

    // 首先是Record本身需要的内存
    private int memorySize = ClassSize.DEFAULT_RECORD_HEAD;

    public DefaultRecord()
    {
        this.columns = new ArrayList<>(RECORD_AVERAGE_COLUMN_NUMBER);
    }

    @Override
    public void addColumn(Column column)
    {
        columns.add(column);
        incrByteSize(column);
    }

    @Override
    public Column getColumn(int i)
    {
        if (i < 0 || i >= columns.size()) {
            return null;
        }
        return columns.get(i);
    }

    @Override
    public void setColumn(int i, Column column)
    {
        if (i < 0) {
            throw AddaxException.asAddaxException(FrameworkErrorCode.ARGUMENT_ERROR,
                    "不能给index小于0的column设置值");
        }

        if (i >= columns.size()) {
            expandCapacity(i + 1);
        }

        decrByteSize(getColumn(i));
        this.columns.set(i, column);
        incrByteSize(getColumn(i));
    }

    @Override
    public String toString()
    {
        Map<String, Object> json = new HashMap<>();
        json.put("size", this.getColumnNumber());
        json.put("data", this.columns);
        return JSON.toJSONString(json);
    }

    @Override
    public int getColumnNumber()
    {
        return this.columns.size();
    }

    @Override
    public int getByteSize()
    {
        return byteSize;
    }

    public int getMemorySize()
    {
        return memorySize;
    }

    private void decrByteSize(Column column)
    {
        if (null == column) {
            return;
        }

        byteSize -= column.getByteSize();

        //内存的占用是column对象的头 再加实际大小
        memorySize = memorySize - ClassSize.COLUMN_HEAD - column.getByteSize();
    }

    private void incrByteSize(Column column)
    {
        if (null == column) {
            return;
        }

        byteSize += column.getByteSize();

        //内存的占用是column对象的头 再加实际大小
        memorySize = memorySize + ClassSize.COLUMN_HEAD + column.getByteSize();
    }

    private void expandCapacity(int totalSize)
    {
        if (totalSize <= 0) {
            return;
        }

        int needToExpand = totalSize - columns.size();
        while (needToExpand-- > 0) {
            this.columns.add(null);
        }
    }
}
