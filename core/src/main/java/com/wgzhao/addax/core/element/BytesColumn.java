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
import org.apache.commons.lang3.ArrayUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Created by jingxing on 14-8-24.
 */
public class BytesColumn
        extends Column
{

    public BytesColumn()
    {
        this(null);
    }

    public BytesColumn(byte[] bytes)
    {
        super(ArrayUtils.clone(bytes), Column.Type.BYTES, null == bytes ? 0
                : bytes.length);
    }

    @Override
    public byte[] asBytes()
    {
        if (null == this.getRawData()) {
            return new byte[0];
        }

        return (byte[]) this.getRawData();
    }

    @Override
    public String asString()
    {
        if (null == this.getRawData()) {
            return null;
        }

        try {
            return ColumnCast.bytes2String(this);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    ErrorCode.CONVERT_NOT_SUPPORT,
                    "Bytes[" + this + "] cannot be converted to String .");
        }
    }

    @Override
    public Long asLong()
    {
        long value = 0L;
        for (byte b : this.asBytes()) {
            // Shifting previous value 8 bits to right and
            // add it with next value
            value = (value << 8) + (b & 255);
        }
        return value;
    }

    @Override
    public BigDecimal asBigDecimal()
    {
        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT, "Bytes type cannot converted to BigDecimal.");
    }

    @Override
    public BigInteger asBigInteger()
    {
        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT, "Bytes type cannot converted to BigInteger.");
    }

    @Override
    public Timestamp asTimestamp()
    {
        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT, "Bytes type cannot converted to Timestamp.");
    }

    @Override
    public Double asDouble()
    {
        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT, "Bytes type cannot converted to Long.");
    }

    @Override
    public Date asDate()
    {
        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT, "Bytes type cannot converted to Date.");
    }

    @Override
    public Boolean asBoolean()
    {
        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT, "Bytes type cannot converted to Boolean.");
    }
}
