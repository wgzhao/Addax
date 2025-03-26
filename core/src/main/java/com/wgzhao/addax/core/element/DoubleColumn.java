/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.core.element;

import com.wgzhao.addax.core.spi.ErrorCode;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.OverFlowUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Date;

/**
 * A Column implementation for handling double values.
 * This class supports various constructors to initialize the column with different data types.
 */
public class DoubleColumn
        extends Column
{

    /**
     * Template for error messages when type conversion is not supported
     */
    private final String errorTemplate = "Double type cannot be converted to %s.";

    /**
     * Creates a DoubleColumn with a string representation of the data.
     *
     * @param data The string data to initialize the column
     */
    public DoubleColumn(String data)
    {
        this(data, null == data ? 0 : data.length());
        this.validate(data);
    }

    /**
     * Creates a DoubleColumn with a Long value.
     *
     * @param data The Long data to initialize the column
     */
    public DoubleColumn(Long data)
    {
        this(data == null ? null : String.valueOf(data));
    }

    /**
     * Creates a DoubleColumn with an Integer value.
     *
     * @param data The Integer data to initialize the column
     */
    public DoubleColumn(Integer data)
    {
        this(data == null ? null : String.valueOf(data));
    }

    /**
     * Creates a DoubleColumn with a Double value.
     * Note: Double cannot accurately represent decimal data, use String constructor instead.
     *
     * @param data The Double data to initialize the column
     */
    public DoubleColumn(Double data)
    {
        this(data == null ? null : new BigDecimal(String.valueOf(data)).toPlainString());
    }

    /**
     * Creates a DoubleColumn with a Float value.
     * Note: Float cannot accurately represent decimal data, use String constructor instead.
     *
     * @param data The Float data to initialize the column
     */
    public DoubleColumn(Float data)
    {
        this(data == null ? null : new BigDecimal(String.valueOf(data)).toPlainString());
    }

    /**
     * Creates a DoubleColumn with a BigDecimal value.
     *
     * @param data The BigDecimal data to initialize the column
     */
    public DoubleColumn(BigDecimal data)
    {
        this(null == data ? null : data.toPlainString());
    }

    /**
     * Creates a DoubleColumn with a BigInteger value.
     *
     * @param data The BigInteger data to initialize the column
     */
    public DoubleColumn(BigInteger data)
    {
        this(null == data ? null : data.toString());
    }

    /**
     * Creates an empty DoubleColumn with null value.
     */
    public DoubleColumn()
    {
        this((String) null);
    }

    /**
     * Private constructor to initialize the column with string data and byte size.
     *
     * @param data The string data to initialize the column
     * @param byteSize The byte size of the data
     */
    private DoubleColumn(String data, int byteSize)
    {
        super(data, Column.Type.DOUBLE, byteSize);
    }

    /**
     * Converts the column data to BigDecimal.
     *
     * @return The BigDecimal representation of the data
     * @throws AddaxException if conversion fails
     */
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
            throw AddaxException.asAddaxException(
                    ErrorCode.CONVERT_NOT_SUPPORT,
                    String.format("String[%s] cannot be converted to Double.", this.getRawData()));
        }
    }

    /**
     * Converts the column data to Double.
     *
     * @return The Double representation of the data
     * @throws AddaxException if conversion fails
     */
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

    /**
     * Converts the column data to Long.
     *
     * @throws AddaxException if conversion fails
     */
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

    /**
     * Converts the column data to BigInteger.
     *
     * @return The BigInteger representation of the data
     * @throws AddaxException if conversion fails
     */
    @Override
    public BigInteger asBigInteger()
    {
        if (null == this.getRawData()) {
            return null;
        }

        return this.asBigDecimal().toBigInteger();
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
        return (String) this.getRawData();
    }

    /**
     * Conversion to Boolean is not supported.
     *
     * @throws AddaxException always
     */
    @Override
    public Boolean asBoolean()
    {
        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT, String.format(errorTemplate, "Boolean"));
    }

    /**
     * Conversion to Date is not supported.
     *
     * @throws AddaxException always
     */
    @Override
    public Date asDate()
    {
        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT, String.format(errorTemplate, "Date"));
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
                ErrorCode.CONVERT_NOT_SUPPORT, String.format(errorTemplate, "Bytes"));
    }

    /**
     * Conversion to Timestamp is not supported.
     *
     * @throws AddaxException always
     */
    @Override
    public Timestamp asTimestamp()
    {
        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT, String.format(errorTemplate, "Timestamp"));
    }

    /**
     * Validates the string data to ensure it can be converted to a double.
     *
     * @param data The string data to validate
     * @throws AddaxException if validation fails
     */
    private void validate(String data)
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
            throw AddaxException.asAddaxException(
                    ErrorCode.CONVERT_NOT_SUPPORT,
                    String.format("String[%s] cannot be converted to Double.", data));
        }
    }
}