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
import com.wgzhao.addax.common.spi.ErrorCode;
import com.wgzhao.addax.common.util.OverFlowUtil;
import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Date;

/**
 * A Column implementation for handling long values.
 * This class supports various constructors to initialize the column with different data types.
 */
public class LongColumn
        extends Column
{

    /**
     * Creates a LongColumn with a string representation of the data.
     *
     * @param data The string data to initialize the column
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
             * When rawData is in the range [0-127], rawData.bitLength() < 8,
             * causing its byteSize to be 0. For simplicity, we assume its length is data.length().
             * super.setByteSize(rawData.bitLength() / 8)
             */
            super.setByteSize(data.length());
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    ErrorCode.CONVERT_NOT_SUPPORT, "Cannot convert the string '" + data + "' to Long.");
        }
    }

    /**
     * Creates a LongColumn with a Long value.
     *
     * @param data The Long data to initialize the column
     */
    public LongColumn(Long data)
    {
        this(null == data ? null : BigInteger.valueOf(data));
    }

    /**
     * Creates a LongColumn with an Integer value.
     *
     * @param data The Integer data to initialize the column
     */
    public LongColumn(Integer data)
    {
        this(null == data ? null : BigInteger.valueOf(data));
    }

    /**
     * Creates a LongColumn with a BigInteger value.
     *
     * @param data The BigInteger data to initialize the column
     */
    public LongColumn(BigInteger data)
    {
        this(data, null == data ? 0 : 8);
    }

    /**
     * Private constructor to initialize the column with BigInteger data and byte size.
     *
     * @param data The BigInteger data to initialize the column
     * @param byteSize The byte size of the data
     */
    private LongColumn(BigInteger data, int byteSize)
    {
        super(data, Column.Type.LONG, byteSize);
    }

    /**
     * Creates an empty LongColumn with null value.
     */
    public LongColumn()
    {
        this((BigInteger) null);
    }

    /**
     * Converts the column data to BigInteger.
     *
     * @return The BigInteger representation of the data
     */
    @Override
    public BigInteger asBigInteger()
    {
        if (null == this.getRawData()) {
            return null;
        }

        return (BigInteger) this.getRawData();
    }

    /**
     * Converts the column data to Timestamp.
     *
     * @return The Timestamp representation of the data
     */
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

    /**
     * Converts the column data to Long.
     *
     * @return The Long representation of the data
     */
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

    /**
     * Converts the column data to Double.
     *
     * @return The Double representation of the data
     */
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

    /**
     * Converts the column data to Boolean.
     *
     * @return The Boolean representation of the data
     */
    @Override
    public Boolean asBoolean()
    {
        if (null == this.getRawData()) {
            return null;
        }

        return this.asBigInteger().compareTo(BigInteger.ZERO) != 0;
    }

    /**
     * Converts the column data to BigDecimal.
     *
     * @return The BigDecimal representation of the data
     */
    @Override
    public BigDecimal asBigDecimal()
    {
        if (null == this.getRawData()) {
            return null;
        }

        return new BigDecimal(this.asBigInteger());
    }

    /**
     * Converts the column data to String.
     *
     * @return The String representation of the data
     */
    @Override
    public String asString()
    {
        if (null == this.getRawData()) {
            return null;
        }
        return this.getRawData().toString();
    }

    /**
     * Converts the column data to Date.
     *
     * @return The Date representation of the data
     */
    @Override
    public Date asDate()
    {
        if (null == this.getRawData()) {
            return null;
        }
        return new Date(this.asLong());
    }

    /**
     * Conversion to byte array is not supported.
     *
     * @throws AddaxException always
     */
    @Override
    public byte[] asBytes()
    {
        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT, "Long type cannot be converted to Bytes.");
    }
}