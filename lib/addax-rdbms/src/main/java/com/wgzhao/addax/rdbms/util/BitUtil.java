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

package com.wgzhao.addax.rdbms.util;

import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.exception.AddaxException;

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.wgzhao.addax.core.spi.ErrorCode.CONVERT_NOT_SUPPORT;

/**
 * Conversions between the two representations of a SQL {@code BIT(n)} value.
 * <p>
 * Addax carries a bit value as {@code ceil(n / 8)} packed bytes, most significant byte first,
 * which is the form most drivers return from {@link java.sql.ResultSet#getBytes}. Drivers that
 * return the printable 0/1 form instead (pgjdbc) convert through {@link #toPackedBytes} before
 * building a column, so that every reader hands the writers the same convention. The writers
 * read a value back through {@link #toValue(Column)}, which also accepts the forms a non database
 * source delivers: the printable 0/1 form for a text file, and the number itself for a source
 * whose column is numeric.
 */
public final class BitUtil
{
    private BitUtil() {}

    /**
     * Number of bytes that hold a {@code BIT(precision)} value.
     *
     * @param precision the declared width of the bit column
     * @return the length of the packed byte form
     */
    public static int byteLength(int precision)
    {
        return (precision + 7) / 8;
    }

    /**
     * Reads the unsigned value of a packed byte array.
     *
     * @param packed the packed byte form, most significant byte first
     * @return the value of the bit string
     */
    public static BigInteger toValue(byte[] packed)
    {
        return new BigInteger(1, packed);
    }

    /**
     * Reads the value a whole number stands for, so that a source that delivers numbers rather
     * than bit strings still reaches the bit column as the number it holds.
     *
     * @param number the value to read
     * @return the value of the bit string
     * @throws AddaxException if the number has a fractional part
     */
    public static BigInteger toValue(BigDecimal number)
    {
        try {
            return number.toBigIntegerExact();
        }
        catch (ArithmeticException e) {
            throw AddaxException.asAddaxException(CONVERT_NOT_SUPPORT,
                    "The value [" + number + "] has a fractional part and cannot be converted to a bit value.");
        }
    }

    /**
     * Reads a column as the value of a bit string, whichever form it carries: a reader delivers
     * bytes that hold the value packed, a file source the printable 0/1 form, a boolean a single
     * bit and a numeric source the number the bit pattern stands for.
     *
     * @param column the value to read
     * @return the value of the bit string
     * @throws AddaxException if the column cannot be read as a bit value
     */
    public static BigInteger toValue(Column column)
    {
        return switch (column.getType()) {
            case BOOL -> column.asBoolean() ? BigInteger.ONE : BigInteger.ZERO;
            case BYTES -> toValue(column.asBytes());
            case STRING -> parseBitString(column.asString());
            case INT, LONG, DOUBLE -> toValue(column.asBigDecimal());
            default -> throw AddaxException.asAddaxException(CONVERT_NOT_SUPPORT,
                    "The value type " + column.getType() + " cannot be converted to a bit value.");
        };
    }

    /**
     * Parses the printable form of a bit value.
     *
     * @param bits a string of 0 and 1 characters, leading zeros are kept
     * @return the value of the bit string
     * @throws AddaxException if the string holds any other character
     */
    public static BigInteger parseBitString(String bits)
    {
        if (bits == null) {
            throw AddaxException.asAddaxException(CONVERT_NOT_SUPPORT, "The bit value must not be null.");
        }
        String trimmed = bits.trim();
        if (trimmed.isEmpty() || !trimmed.chars().allMatch(c -> c == '0' || c == '1')) {
            throw AddaxException.asAddaxException(CONVERT_NOT_SUPPORT,
                    "The value [" + bits + "] is not a bit value, only the characters 0 and 1 are accepted.");
        }
        return new BigInteger(trimmed, 2);
    }

    /**
     * Renders a value in the printable form without leading zeros.
     *
     * @param value the value of the bit string
     * @return the shortest bit string that holds the value, {@code "0"} for zero
     */
    public static String toBitString(BigInteger value)
    {
        return value.toString(2);
    }

    /**
     * Renders a value in the printable form of exactly {@code precision} characters, which is
     * the form a {@code BIT(precision)} column has on the wire.
     *
     * @param value the value of the bit string
     * @param precision the declared width of the bit column
     * @return the left padded bit string
     * @throws AddaxException if the value needs more than {@code precision} bits
     */
    public static String toBitString(BigInteger value, int precision)
    {
        checkWidth(value, precision);
        String bits = value.toString(2);
        return "0".repeat(precision - bits.length()) + bits;
    }

    /**
     * Packs a value into the byte form of a {@code BIT(precision)} column.
     *
     * @param value the value of the bit string
     * @param precision the declared width of the bit column
     * @return the packed byte form, most significant byte first
     * @throws AddaxException if the value needs more than {@code precision} bits
     */
    public static byte[] toPackedBytes(BigInteger value, int precision)
    {
        checkWidth(value, precision);
        byte[] raw = value.toByteArray();
        int length = byteLength(precision);
        int copied = Math.min(length, raw.length);
        byte[] packed = new byte[length];
        // toByteArray() returns the two's complement form, so it may carry a leading sign byte
        // or be shorter than the declared width; align both forms on the least significant byte
        System.arraycopy(raw, raw.length - copied, packed, length - copied, copied);
        return packed;
    }

    private static void checkWidth(BigInteger value, int precision)
    {
        if (value.signum() < 0 || value.bitLength() > precision) {
            throw AddaxException.asAddaxException(CONVERT_NOT_SUPPORT,
                    "The value [" + value + "] does not fit into a BIT(" + precision + ") column.");
        }
    }
}
