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

import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.spi.ErrorCode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Date;

public class TimestampColumn
    extends Column
{
    private final String errorTemplate = "Timestamp type cannot be converted to %s.";
    public TimestampColumn(Object object, Type type, int byteSize)
    {
        super(object, type, byteSize);
    }

    public TimestampColumn() {
        this((Timestamp)null);
    }

    public TimestampColumn(Timestamp ts)
    {
        super(ts, Type.TIMESTAMP, (ts == null ? 0 : 12));
    }

    public TimestampColumn(Long ts)
    {
        this(new Timestamp(ts));
    }

    public TimestampColumn(String ts)
    {
        this(Timestamp.valueOf(ts));
    }

    public TimestampColumn(Date ts)
    {
        this(Timestamp.from(ts.toInstant()));
    }

    @Override
    public Long asLong()
    {
        if (null == this.getRawData()) {
            return null;
        }

        return this.asTimestamp().getTime();
    }

    public Timestamp asTimestamp()
    {
        if (null == this.getRawData()) {
            return null;
        }
        return (Timestamp)  this.getRawData();
    }

    @Override
    public Double asDouble()
    {
       if (null == this.getRawData()) {
           return null;
       }
       return (Double) this.getRawData();
    }

    @Override
    public String asString()
    {
        if (null == this.getRawData()) {
            return null;
        }

        Timestamp ts = this.asTimestamp();
        return ts.toString();
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
        throw AddaxException.asAddaxException(ErrorCode.CONVERT_NOT_SUPPORT, String.format(errorTemplate, "Bytes"));
    }

    @Override
    public Boolean asBoolean()
    {
        throw AddaxException.asAddaxException(ErrorCode.CONVERT_NOT_SUPPORT, String.format(errorTemplate, "Boolean"));
    }

    @Override
    public BigDecimal asBigDecimal()
    {
        throw AddaxException.asAddaxException(ErrorCode.CONVERT_NOT_SUPPORT, String.format(errorTemplate, "BigDecimal"));
    }

    @Override
    public BigInteger asBigInteger()
    {
        throw AddaxException.asAddaxException(ErrorCode.CONVERT_NOT_SUPPORT, String.format(errorTemplate, "BigInteger"));
    }
}
