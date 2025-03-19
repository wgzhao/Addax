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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

/**
 * A Column implementation for handling date and time values in various formats.
 * This class supports DATE, TIME, and DATETIME types with optional precision and nanoseconds.
 */
public class DateColumn
        extends Column
{

    /**
     * The specific type of date value (DATE, TIME, or DATETIME)
     */
    private DateType subType = DateType.DATETIME;

    /**
     * Nanoseconds component of the time value
     */
    private int nanos = 0;

    /**
     * Precision for the time value, -1 if not specified
     */
    private int precision = -1;

    /**
     * Template for error messages when type conversion is not supported
     */
    private final String errorTemplate = "Date type cannot be converted to %s.";

    /**
     * Creates an empty DateColumn with null value.
     */
    public DateColumn()
    {
        this((Long) null);
    }

    /**
     * Creates a DateColumn with a timestamp value.
     *
     * @param stamp Unix timestamp in milliseconds
     */
    public DateColumn(Long stamp)
    {
        super(stamp, Column.Type.DATE, (null == stamp ? 0 : 8));
    }

    /**
     * Creates a DateColumn from a java.util.Date object.
     *
     * @param date The Date object to convert
     */
    public DateColumn(Date date)
    {
        this(date == null ? null : date.getTime());
    }

    /**
     * Creates a DateColumn from a java.sql.Date object.
     * Sets the subtype to DATE.
     *
     * @param date The SQL Date object to convert
     */
    public DateColumn(java.sql.Date date)
    {
        this(date == null ? null : date.getTime());
        this.setSubType(DateType.DATE);
    }

    /**
     * Creates a DateColumn from a java.sql.Time object.
     * Sets the subtype to TIME.
     *
     * @param time The SQL Time object to convert
     */
    public DateColumn(java.sql.Time time)
    {
        this(time == null ? null : time.getTime());
        this.setSubType(DateType.TIME);
    }

    /**
     * Creates a DateColumn from a java.sql.Timestamp object.
     * Sets the subtype to DATETIME.
     *
     * @param ts The SQL Timestamp object to convert
     */
    public DateColumn(java.sql.Timestamp ts)
    {
        this(ts == null ? null : ts.getTime());
        this.setSubType(DateType.DATETIME);
    }

    /**
     * Creates a DateColumn from a Time object with specified precision and nanoseconds.
     *
     * @param time The SQL Time object
     * @param nanos Nanoseconds component
     * @param jdbcPrecision nanos, int jdbcPrecision)
     */
    public DateColumn(Time time, int nanos, int jdbcPrecision)
    {
        this(time);
        if (time != null) {
            setNanos(nanos);
        }
        if (jdbcPrecision == 10) {
            setPrecision(0);
        }
        if (jdbcPrecision >= 12 && jdbcPrecision <= 17) {
            setPrecision(jdbcPrecision - 11);
        }
    }

    public long getNanos()
    {
        return nanos;
    }

    /**
     * Sets the nanoseconds component.
     *
     * @param nanos nanoseconds value to set
     */
    public void setNanos(int nanos)
    {
        this.nanos = nanos;
    }

    /**
     * Gets the precision value.
     *
     * @return precision value (-1 if not set)
     */
    public int getPrecision()
    {
        return precision;
    }

    /**
     * Sets the precision value.
     *
     * @param precision precision value to set
     */
    public void setPrecision(int precision)
    {
        this.precision = precision;
    }

    /**
     * Converts the date value to a Long timestamp.
     *
     * @return timestamp in milliseconds since epoch
     */
    @Override
    public Long asLong()
    {
        return (Long) this.getRawData();
    }

    /**
     * Converts the date value to a String representation.
     *
     * @return string representation of the date
     * @throws AddaxException if conversion fails
     */
    @Override
    public String asString()
    {
        try {
            return ColumnCast.date2String(this);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    ErrorCode.CONVERT_NOT_SUPPORT,
                    String.format("Date[%s] type cannot be converted to String .", this));
        }
    }

    /**
     * Converts the date value to a Date object.
     *
     * @return Date object, null if raw data is null
     */
    @Override
    public Date asDate()
    {
        if (null == this.getRawData()) {
            return null;
        }
        return new Date((Long) this.getRawData());
    }

    /**
     * Not supported conversion to bytes.
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
     * Not supported conversion to Boolean.
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
     * Not supported conversion to Double.
     *
     * @throws AddaxException always
     */
    @Override
    public Double asDouble()
    {
        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT, String.format(errorTemplate, "Double"));
    }

    /**
     * Not supported conversion to BigInteger.
     *
     * @throws AddaxException always
     */
    @Override
    public BigInteger asBigInteger()
    {
        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT, String.format(errorTemplate, "BigInteger"));
    }

    /**
     * Converts the date value to a Timestamp object.
     *
     * @return Timestamp object, null if raw data is null
     */
    @Override
    public Timestamp asTimestamp()
    {
        if (null == this.getRawData()) {
            return null;
        }
        return new Timestamp(this.asLong());
    }

    /**
     * Not supported conversion to BigDecimal.
     *
     * @throws AddaxException always
     */
    @Override
    public BigDecimal asBigDecimal()
    {
        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT, String.format(errorTemplate, "BigDecimal"));
    }

    /**
     * Gets the date subtype (DATE, TIME, or DATETIME).
     *
     * @return current DateType
     */
    public DateType getSubType()
    {
        return subType;
    }

    /**
     * Sets the date subtype.
     *
     * @param subType DateType to set
     */
    public void setSubType(DateType subType)
    {
        this.subType = subType;
    }

    /**
     * Enumeration of supported date types.
     */
    public enum DateType
    {
        DATE, TIME, DATETIME
    }
}