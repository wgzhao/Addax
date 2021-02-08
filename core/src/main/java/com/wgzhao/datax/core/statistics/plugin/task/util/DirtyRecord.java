package com.wgzhao.datax.core.statistics.plugin.task.util;

import com.wgzhao.datax.common.element.Column;
import com.wgzhao.datax.common.element.Record;
import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.core.util.FrameworkErrorCode;
import com.alibaba.fastjson.JSON;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DirtyRecord
        implements Record
{
    private List<Column> columns = new ArrayList<>();

    private static final String NOT_SUPPORT_METHOD =  "该方法不支持!";

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
        this.columns.add(
                DirtyColumn.asDirtyColumn(column, this.columns.size()));
    }

    @Override
    public String toString()
    {
        return JSON.toJSONString(this.columns);
    }

    @Override
    public void setColumn(int i, Column column)
    {
        throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
               NOT_SUPPORT_METHOD);
    }

    @Override
    public Column getColumn(int i)
    {
        throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
               NOT_SUPPORT_METHOD);
    }

    @Override
    public int getColumnNumber()
    {
        throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
               NOT_SUPPORT_METHOD);
    }

    @Override
    public int getByteSize()
    {
        throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
               NOT_SUPPORT_METHOD);
    }

    @Override
    public int getMemorySize()
    {
        throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
               NOT_SUPPORT_METHOD);
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

    private static final String NOT_SUPPORT_METHOD =  "该方法不支持!";

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
        throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
               NOT_SUPPORT_METHOD);
    }

    @Override
    public Double asDouble()
    {
        throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
               NOT_SUPPORT_METHOD);
    }

    @Override
    public String asString()
    {
        throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
               NOT_SUPPORT_METHOD);
    }

    @Override
    public Date asDate()
    {
        throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
               NOT_SUPPORT_METHOD);
    }

    @Override
    public byte[] asBytes()
    {
        throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
               NOT_SUPPORT_METHOD);
    }

    @Override
    public Boolean asBoolean()
    {
        throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
               NOT_SUPPORT_METHOD);
    }

    @Override
    public BigDecimal asBigDecimal()
    {
        throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
               NOT_SUPPORT_METHOD);
    }

    @Override
    public BigInteger asBigInteger()
    {
        throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
               NOT_SUPPORT_METHOD);
    }
}
