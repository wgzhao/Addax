package com.wgzhao.datax.common.element;

import com.wgzhao.datax.common.exception.CommonErrorCode;
import com.wgzhao.datax.common.exception.DataXException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class DoubleColumn
        extends Column
{

    public DoubleColumn( String data)
    {
        this(data, null == data ? 0 : data.length());
        this.validate(data);
    }

    public DoubleColumn(Long data)
    {
        this(data == null ? null : String.valueOf(data));
    }

    public DoubleColumn(Integer data)
    {
        this(data == null ? null : String.valueOf(data));
    }

    /**
     * Double无法表示准确的小数数据，我们不推荐使用该方法保存Double数据，建议使用String作为构造入参
     */
    public DoubleColumn( Double data)
    {
        this(data == null ? null
                : new BigDecimal(String.valueOf(data)).toPlainString());
    }

    /**
     * Float无法表示准确的小数数据，我们不推荐使用该方法保存Float数据，建议使用String作为构造入参
     */
    public DoubleColumn( Float data)
    {
        this(data == null ? null
                : new BigDecimal(String.valueOf(data)).toPlainString());
    }

    public DoubleColumn( BigDecimal data)
    {
        this(null == data ? null : data.toPlainString());
    }

    public DoubleColumn( BigInteger data)
    {
        this(null == data ? null : data.toString());
    }

    public DoubleColumn()
    {
        this((String) null);
    }

    private DoubleColumn( String data, int byteSize)
    {
        super(data, Column.Type.DOUBLE, byteSize);
    }

    @Override
    public BigDecimal asBigDecimal()
    {
        if (null == this.getRawData()) {
            return null;
        }

        try {
            return new BigDecimal((String) this.getRawData());
        }
        catch (NumberFormatException e) {
            throw DataXException.asDataXException(
                    CommonErrorCode.CONVERT_NOT_SUPPORT,
                    String.format("String[%s] 无法转换为Double类型 .",
                            this.getRawData()));
        }
    }

    @Override
    public Double asDouble()
    {
        if (null == this.getRawData()) {
            return null;
        }

        String string = (String) this.getRawData();

        boolean isDoubleSpecific = "NaN".equals(string)
                || "-Infinity".equals(string) || "+Infinity".equals(string);
        if (isDoubleSpecific) {
            return Double.valueOf(string);
        }

        BigDecimal result = this.asBigDecimal();
        OverFlowUtil.validateDoubleNotOverFlow(result);

        return result.doubleValue();
    }

    @Override
    public Long asLong()
    {
        if (null == this.getRawData()) {
            return null;
        }

        BigDecimal result = this.asBigDecimal();
        OverFlowUtil.validateLongNotOverFlow(result.toBigInteger());

        return result.longValue();
    }

    @Override
    public BigInteger asBigInteger()
    {
        if (null == this.getRawData()) {
            return null;
        }

        return this.asBigDecimal().toBigInteger();
    }

    @Override
    public String asString()
    {
        if (null == this.getRawData()) {
            return null;
        }
        return (String) this.getRawData();
    }

    @Override
    public Boolean asBoolean()
    {
        throw DataXException.asDataXException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Double类型无法转为Bool .");
    }

    @Override
    public Date asDate()
    {
        throw DataXException.asDataXException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Double类型无法转为Date类型 .");
    }

    @Override
    public byte[] asBytes()
    {
        throw DataXException.asDataXException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Double类型无法转为Bytes类型 .");
    }

    private void validate( String data)
    {
        if (null == data) {
            return;
        }

        if ("NaN".equalsIgnoreCase(data) || "-Infinity".equalsIgnoreCase(data)
                || "Infinity".equalsIgnoreCase(data)) {
            return;
        }

        try {
            new BigDecimal(data);
        }
        catch (Exception e) {
            throw DataXException.asDataXException(
                    CommonErrorCode.CONVERT_NOT_SUPPORT,
                    String.format("String[%s]无法转为Double类型 .", data));
        }
    }
}