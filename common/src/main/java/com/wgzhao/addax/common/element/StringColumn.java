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

import com.wgzhao.addax.common.exception.ErrorCode;
import com.wgzhao.addax.common.exception.AddaxException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Created by jingxing on 14-8-24.
 */

public class StringColumn
        extends Column
{

    private final String errorTemplate = "String type cannot be converted to %s.";

    public StringColumn()
    {
        this(null);
    }

    public StringColumn(final String rawData)
    {
        super(rawData, Column.Type.STRING, (null == rawData ? 0 : rawData
                .length()));
    }

    @Override
    public String asString()
    {
        if (null == this.getRawData()) {
            return null;
        }

        return (String) this.getRawData();
    }

    private void validateDoubleSpecific(final String data)
    {
        if ("NaN".equals(data) || "Infinity".equals(data)
                || "-Infinity".equals(data)) {
            throw AddaxException.asAddaxException(
                    ErrorCode.CONVERT_NOT_SUPPORT,
                    String.format("['%s'] belongs to the special Double type and cannot be converted to other type.", data));
        }
    }

    @Override
    public BigInteger asBigInteger()
    {
        if (null == this.getRawData()) {
            return null;
        }

        this.validateDoubleSpecific((String) this.getRawData());

        try {
            return this.asBigDecimal().toBigInteger();
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    ErrorCode.CONVERT_NOT_SUPPORT, String.format(
                            "['%s'] cannot be converted to BigInteger.", this.asString()));
        }
    }

    @Override
    public Timestamp asTimestamp()
    {
        if (null == this.getRawData()) {
            return null;
        }
        return Timestamp.valueOf((String) this.getRawData());
    }

    @Override
    public Long asLong()
    {
        if (null == this.getRawData()) {
            return null;
        }

        this.validateDoubleSpecific((String) this.getRawData());

        try {
            BigInteger integer = this.asBigInteger();
            OverFlowUtil.validateLongNotOverFlow(integer);
            return integer.longValue();
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    ErrorCode.CONVERT_NOT_SUPPORT, String.format(errorTemplate, "Long"));
        }
    }

    @Override
    public BigDecimal asBigDecimal()
    {
        if (null == this.getRawData()) {
            return null;
        }

        this.validateDoubleSpecific((String) this.getRawData());

        try {
            return new BigDecimal(this.asString());
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    ErrorCode.CONVERT_NOT_SUPPORT, String.format(errorTemplate, "BigDecimal"));
        }
    }

    @Override
    public Double asDouble()
    {
        if (null == this.getRawData()) {
            return null;
        }

        String data = (String) this.getRawData();
        if ("NaN".equals(data)) {
            return Double.NaN;
        }

        if ("Infinity".equals(data)) {
            return Double.POSITIVE_INFINITY;
        }

        if ("-Infinity".equals(data)) {
            return Double.NEGATIVE_INFINITY;
        }

        BigDecimal decimal = this.asBigDecimal();
        OverFlowUtil.validateDoubleNotOverFlow(decimal);

        return decimal.doubleValue();
    }

    @Override
    public Boolean asBoolean()
    {
        if (null == this.getRawData()) {
            return null; //NOSONAR
        }

        if ("true".equalsIgnoreCase(this.asString())) {
            return true;
        }

        if ("false".equalsIgnoreCase(this.asString())) {
            return false;
        }

        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT, String.format(errorTemplate, "Boolean"));
    }

    @Override
    public Date asDate()
    {
        try {
            return ColumnCast.string2Date(this);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    ErrorCode.CONVERT_NOT_SUPPORT, String.format(errorTemplate, "Date"));
        }
    }

    @Override
    public byte[] asBytes()
    {
        try {
            return ColumnCast.string2Bytes(this);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    ErrorCode.CONVERT_NOT_SUPPORT, String.format(errorTemplate, "Bytes"));
        }
    }
}
