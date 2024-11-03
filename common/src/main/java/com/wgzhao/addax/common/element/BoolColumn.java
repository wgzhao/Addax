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

import com.wgzhao.addax.common.spi.ErrorCode;
import com.wgzhao.addax.common.exception.AddaxException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Created by jingxing on 14-8-24.
 */
public class BoolColumn
        extends Column
{

    public BoolColumn(Boolean bool)
    {
        super(bool, Column.Type.BOOL, 1);
    }

    public BoolColumn(final String data)
    {
        this(true);
        this.validate(data);
        if (null == data) {
            this.setRawData(null);
            this.setByteSize(0);
        }
        else {
            this.setRawData(Boolean.valueOf(data));
            this.setByteSize(1);
        }
    }

    public BoolColumn()
    {
        super(null, Column.Type.BOOL, 1);
    }

    @Override
    public Boolean asBoolean()
    {
        if (null == super.getRawData()) {
            return null; //NOSONAR
        }

        return (Boolean) super.getRawData();
    }

    @Override
    public Long asLong()
    {
        if (null == this.getRawData()) {
            return null;
        }

        return Boolean.TRUE.equals(this.asBoolean()) ? 1L : 0L;
    }

    @Override
    public Double asDouble()
    {
        if (null == this.getRawData()) {
            return null;
        }

        return Boolean.TRUE.equals(this.asBoolean()) ? 1.0d : 0.0d;
    }

    @Override
    public String asString()
    {
        if (null == super.getRawData()) {
            return null;
        }

        return Boolean.TRUE.equals(this.asBoolean()) ? "true" : "false";
    }

    @Override
    public BigInteger asBigInteger()
    {
        if (null == this.getRawData()) {
            return null;
        }

        return BigInteger.valueOf(this.asLong());
    }

    @Override
    public BigDecimal asBigDecimal()
    {
        if (null == this.getRawData()) {
            return null;
        }

        return BigDecimal.valueOf(this.asLong());
    }

    @Override
    public Date asDate()
    {
        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT, "Bool type cannot be converted to Date.");
    }

    @Override
    public byte[] asBytes()
    {
        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT, "Bool type cannot be converted to Bytes.");
    }

    @Override
    public Timestamp asTimestamp()
    {
        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT, "Bool type cannot be converted to Timestamp.");
    }

    private void validate(final String data)
    {
        if (null == data) {
            return;
        }

        if ("true".equalsIgnoreCase(data) || "false".equalsIgnoreCase(data)) {
            return;
        }

        throw AddaxException.asAddaxException(
                ErrorCode.CONVERT_NOT_SUPPORT,
                "String [" + data + "] cannot be converted to Bool .");
    }
}
