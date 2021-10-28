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

package com.wgzhao.addax.core.statistics.plugin.task.util;

import com.alibaba.fastjson.JSON;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.core.util.FrameworkErrorCode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DirtyRecord
        implements Record
{
    private List<Column> columns = new ArrayList<>();

    private static final String NOT_SUPPORT_METHOD = "该方法不支持!";

    public static DirtyRecord asDirtyRecord(final Record record)
    {
        DirtyRecord result = new DirtyRecord();
        for (int i = 0; i < record.getColumnNumber(); i++) {
            result.addColumn(record.getColumn(i));
        }

        return result;
    }

    @Override
    public void addColumn(Column column)
    {
        this.columns.add(DirtyColumn.asDirtyColumn(column, this.columns.size()));
    }

    @Override
    public String toString()
    {
        return JSON.toJSONString(this.columns);
    }

    @Override
    public void setColumn(int i, Column column)
    {
        throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR, NOT_SUPPORT_METHOD);
    }

    @Override
    public Column getColumn(int i)
    {
        throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR, NOT_SUPPORT_METHOD);
    }

    @Override
    public int getColumnNumber()
    {
        throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR, NOT_SUPPORT_METHOD);
    }

    @Override
    public int getByteSize()
    {
        throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR, NOT_SUPPORT_METHOD);
    }

    @Override
    public int getMemorySize()
    {
        throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR, NOT_SUPPORT_METHOD);
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    public void setColumns(List<Column> columns)
    {
        this.columns = columns;
    }
}

class DirtyColumn
        extends Column
{

    private static final String NOT_SUPPORT_METHOD = "该方法不支持!";

    private DirtyColumn(Column column, int index)
    {
        this(null == column ? null : column.getRawData(),
                null == column ? Column.Type.NULL : column.getType(),
                null == column ? 0 : column.getByteSize(), index);
    }

    private DirtyColumn(Object object, Type type, int byteSize, int index)
    {
        super(object, type, byteSize);
        // this.setIndex(index)
    }

    public static Column asDirtyColumn(final Column column, int index)
    {
        return new DirtyColumn(column, index);
    }

    @Override
    public Long asLong()
    {
        throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR, NOT_SUPPORT_METHOD);
    }

    @Override
    public Double asDouble()
    {
        throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR, NOT_SUPPORT_METHOD);
    }

    @Override
    public String asString()
    {
        throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR, NOT_SUPPORT_METHOD);
    }

    @Override
    public Date asDate()
    {
        throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR, NOT_SUPPORT_METHOD);
    }

    @Override
    public byte[] asBytes()
    {
        throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR, NOT_SUPPORT_METHOD);
    }

    @Override
    public Boolean asBoolean()
    {
        throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR, NOT_SUPPORT_METHOD);
    }

    @Override
    public BigDecimal asBigDecimal()
    {
        throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR, NOT_SUPPORT_METHOD);
    }

    @Override
    public BigInteger asBigInteger()
    {
        throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR, NOT_SUPPORT_METHOD);
    }

    @Override
    public Timestamp asTimestamp()
    {
        throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR, NOT_SUPPORT_METHOD);
    }
}
