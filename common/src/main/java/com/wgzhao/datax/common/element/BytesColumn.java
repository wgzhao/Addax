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

package com.wgzhao.datax.common.element;

import com.wgzhao.datax.common.exception.CommonErrorCode;
import com.wgzhao.datax.common.exception.DataXException;
import org.apache.commons.lang3.ArrayUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
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
            throw DataXException.asDataXException(
                    CommonErrorCode.CONVERT_NOT_SUPPORT,
                    String.format("Bytes[%s]不能转为String .", this.toString()));
        }
    }

    @Override
    public Long asLong()
    {
        throw DataXException.asDataXException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Long .");
    }

    @Override
    public BigDecimal asBigDecimal()
    {
        throw DataXException.asDataXException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为BigDecimal .");
    }

    @Override
    public BigInteger asBigInteger()
    {
        throw DataXException.asDataXException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为BigInteger .");
    }

    @Override
    public Double asDouble()
    {
        throw DataXException.asDataXException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Long .");
    }

    @Override
    public Date asDate()
    {
        throw DataXException.asDataXException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Date .");
    }

    @Override
    public Boolean asBoolean()
    {
        throw DataXException.asDataXException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Boolean .");
    }
}
