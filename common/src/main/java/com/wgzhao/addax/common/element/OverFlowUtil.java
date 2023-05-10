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

import java.math.BigDecimal;
import java.math.BigInteger;

public final class OverFlowUtil
{
    public static final BigInteger MAX_LONG = BigInteger
            .valueOf(Long.MAX_VALUE);
    public static final BigInteger MIN_LONG = BigInteger
            .valueOf(Long.MIN_VALUE);
    public static final BigDecimal MIN_DOUBLE_POSITIVE = new BigDecimal(
            String.valueOf(Double.MIN_VALUE));
    public static final BigDecimal MAX_DOUBLE_POSITIVE = new BigDecimal(
            String.valueOf(Double.MAX_VALUE));

    private OverFlowUtil() {}

    public static boolean isLongOverflow(final BigInteger integer)
    {
        return (integer.compareTo(OverFlowUtil.MAX_LONG) > 0 || integer
                .compareTo(OverFlowUtil.MIN_LONG) < 0);
    }

    public static void validateLongNotOverFlow(final BigInteger integer)
    {
        boolean isOverFlow = OverFlowUtil.isLongOverflow(integer);

        if (isOverFlow) {
            throw AddaxException.asAddaxException(
                    CommonErrorCode.CONVERT_OVER_FLOW,
                    String.format("An overflow occurred when converting [%s] to Long type.", integer));
        }
    }

    public static boolean isDoubleOverFlow(final BigDecimal decimal)
    {
        if (decimal.signum() == 0) {
            return false;
        }

        BigDecimal newDecimal = decimal;
        boolean isPositive = decimal.signum() == 1;
        if (!isPositive) {
            newDecimal = decimal.negate();
        }

        return (newDecimal.compareTo(MIN_DOUBLE_POSITIVE) < 0 || newDecimal
                .compareTo(MAX_DOUBLE_POSITIVE) > 0);
    }

    public static void validateDoubleNotOverFlow(final BigDecimal decimal)
    {
        boolean isOverFlow = OverFlowUtil.isDoubleOverFlow(decimal);
        if (isOverFlow) {
            throw AddaxException.asAddaxException(
                    CommonErrorCode.CONVERT_OVER_FLOW,
                    String.format("An overflow occurred when converting [%s] to Double type.",
                            decimal.toPlainString()));
        }
    }
}
