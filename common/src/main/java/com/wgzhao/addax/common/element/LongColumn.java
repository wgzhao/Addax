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

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.exception.CommonErrorCode;
import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Date;

public class LongColumn
        extends Column
{
    /**
     * 从整形字符串表示转为LongColumn，支持Java科学计数法
     * 如果data为浮点类型的字符串表示，数据将会失真，请使用DoubleColumn对接浮点字符串
     *
     * @param data 要转成long型的字符串
     */
    public LongColumn(String data)
    {
        super(null, Column.Type.LONG, 0);
        if (null == data) {
            return;
        }

        try {
            BigInteger rawData = NumberUtils.createBigDecimal(data).toBigInteger();
            super.setRawData(rawData);

			/*
			 * 当 rawData 为[0-127]时，rawData.bitLength() < 8，导致其 byteSize = 0，简单起见，直接认为其长度为 data.length()
			 super.setByteSize(rawData.bitLength() / 8)
			 */
            super.setByteSize(data.length());
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    CommonErrorCode.CONVERT_NOT_SUPPORT,
                    String.format("Cannot convert the string [%s] to Long.", data));
        }
    }

    public LongColumn(Long data)
    {
        this(null == data ? null : BigInteger.valueOf(data));
    }

    public LongColumn(Integer data)
    {
        this(null == data ? null : BigInteger.valueOf(data));
    }

    public LongColumn(BigInteger data)
    {
        this(data, null == data ? 0 : 8);
    }

    private LongColumn(BigInteger data, int byteSize)
    {
        super(data, Column.Type.LONG, byteSize);
    }

    public LongColumn()
    {
        this((BigInteger) null);
    }

    @Override
    public BigInteger asBigInteger()
    {
        if (null == this.getRawData()) {
            return null;
        }

        return (BigInteger) this.getRawData();
    }

    @Override
    public Timestamp asTimestamp()
    {
        if (this.getRawData() instanceof BigInteger) {
            BigInteger b = (BigInteger) this.getRawData();
            long l = b.longValue();
            return new Timestamp(l);
        }
        else {
            return new Timestamp((Long) this.getRawData());
        }
    }

    @Override
    public Long asLong()
    {
        BigInteger rawData = (BigInteger) this.getRawData();
        if (null == rawData) {
            return null;
        }

        OverFlowUtil.validateLongNotOverFlow(rawData);

        return rawData.longValue();
    }

    @Override
    public Double asDouble()
    {
        if (null == this.getRawData()) {
            return null;
        }

        BigDecimal decimal = this.asBigDecimal();
        OverFlowUtil.validateDoubleNotOverFlow(decimal);

        return decimal.doubleValue();
    }

    @Override
    public Boolean asBoolean()
    {
        if (null == this.getRawData()) {
            return null; //NOSONAR;
        }

        return this.asBigInteger().compareTo(BigInteger.ZERO) != 0;
    }

    @Override
    public BigDecimal asBigDecimal()
    {
        if (null == this.getRawData()) {
            return null;
        }

        return new BigDecimal(this.asBigInteger());
    }

    @Override
    public String asString()
    {
        if (null == this.getRawData()) {
            return null;
        }
        return this.getRawData().toString();
    }

    @Override
    public Date asDate()
    {
        if (null == this.getRawData()) {
            return null;
        }
        return new Date(this.asLong());
    }

    @Override
    public byte[] asBytes()
    {
        throw AddaxException.asAddaxException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Long type cannot be converted to Bytes.");
    }
}
