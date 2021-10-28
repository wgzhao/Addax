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

import com.wgzhao.addax.common.exception.CommonErrorCode;
import com.wgzhao.addax.common.exception.AddaxException;
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
                    CommonErrorCode.CONVERT_NOT_SUPPORT,
                    String.format("Bytes[%s]不能转为String .", this.toString()));
        }
    }

    @Override
    public Long asLong()
    {
        throw AddaxException.asAddaxException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Long .");
    }

    @Override
    public BigDecimal asBigDecimal()
    {
        throw AddaxException.asAddaxException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为BigDecimal .");
    }

    @Override
    public BigInteger asBigInteger()
    {
        throw AddaxException.asAddaxException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为BigInteger .");
    }

    @Override
    public Timestamp asTimestamp()
    {
        throw AddaxException.asAddaxException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Timestamp .");
    }

    @Override
    public Double asDouble()
    {
        throw AddaxException.asAddaxException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Long .");
    }

    @Override
    public Date asDate()
    {
        throw AddaxException.asAddaxException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Date .");
    }

    @Override
    public Boolean asBoolean()
    {
        throw AddaxException.asAddaxException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Boolean .");
    }
}
