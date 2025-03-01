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

package com.wgzhao.addax.common.util;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * support split range by BigInteger, long, and String.
 */
public final class RangeSplitUtil
{

    private RangeSplitUtil() {}

    public static String[] doAsciiStringSplit(String left, String right, int expectSliceNumber)
    {
        int radix = 128;

        BigInteger[] tempResult = doBigIntegerSplit(stringToBigInteger(left, radix),
                stringToBigInteger(right, radix), expectSliceNumber);
        String[] result = new String[tempResult.length];

        // handle the first string (because: when converting to a number, and then restoring it,
        // if the first character is just basic, it is not known how many basics should be added)
        result[0] = left;
        result[tempResult.length - 1] = right;

        for (int i = 1, len = tempResult.length - 1; i < len; i++) {
            result[i] = bigIntegerToString(tempResult[i], radix);
        }

        return result;
    }

    public static long[] doLongSplit(long left, long right, int expectSliceNumber)
    {
        BigInteger[] result = doBigIntegerSplit(BigInteger.valueOf(left),
                BigInteger.valueOf(right), expectSliceNumber);
        long[] returnResult = new long[result.length];
        for (int i = 0, len = result.length; i < len; i++) {
            returnResult[i] = result[i].longValue();
        }
        return returnResult;
    }

    public static BigInteger[] doBigIntegerSplit(BigInteger left, BigInteger right, int expectSliceNumber)
    {
        if (expectSliceNumber < 1) {
            throw new IllegalArgumentException(String.format(
                    "The number of splits cannot be less than 1, expectSliceNumber = [%s].", expectSliceNumber));
        }

        if (null == left || null == right) {
            throw new IllegalArgumentException(String.format(
                    "The range [%s, %s] is invalid for BigInteger.", left, right));
        }

        if (left.compareTo(right) == 0) {
            return new BigInteger[] {left, right};
        }
        else {
            // 调整大小顺序，确保 left < right
            if (left.compareTo(right) > 0) {
                BigInteger temp = left;
                left = right;
                right = temp;
            }

            //left < right
            BigInteger endAndStartGap = right.subtract(left);

            BigInteger step = endAndStartGap.divide(BigInteger.valueOf(expectSliceNumber));
            BigInteger remainder = endAndStartGap.remainder(BigInteger.valueOf(expectSliceNumber));

            // can not use step.intValue()==0, because it may overflow
            if (step.compareTo(BigInteger.ZERO) == 0) {
                expectSliceNumber = remainder.intValue();
            }

            BigInteger[] result = new BigInteger[expectSliceNumber + 1];
            result[0] = left;
            result[expectSliceNumber] = right;

            BigInteger lowerBound;
            BigInteger upperBound = left;
            for (int i = 1; i < expectSliceNumber; i++) {
                lowerBound = upperBound;
                upperBound = lowerBound.add(step);
                upperBound = upperBound.add((remainder.compareTo(BigInteger.valueOf(i)) >= 0)
                        ? BigInteger.ONE : BigInteger.ZERO);
                result[i] = upperBound;
            }

            return result;
        }
    }

    private static void checkIfBetweenRange(int value, int left, int right)
    {
        if (value < left || value > right) {
            throw new IllegalArgumentException(String.format("The value of parameter can not less than [%s] or greater than [%s].",
                    left, right));
        }
    }

    /**
     * convert string to BigInteger.
     * Note: radix and basic range are both [1,128], and the sum of radix and basic must also be in [1,128].
     *
     * @param aString the string to convert
     * @param radix the radix
     * @return the BigInteger
     */
    public static BigInteger stringToBigInteger(String aString, int radix)
    {
        if (null == aString) {
            throw new IllegalArgumentException("The parameter bigInteger cannot be null.");
        }

        checkIfBetweenRange(radix, 1, 128);

        BigInteger result = BigInteger.ZERO;
        BigInteger radixBigInteger = BigInteger.valueOf(radix);

        int tempChar;
        int k = 0;

        for (int i = aString.length() - 1; i >= 0; i--) {
            tempChar = aString.charAt(i);
            if (tempChar >= 128) {
                throw new IllegalArgumentException(
                        String.format("When split by string, only ASCII chars are supported, " +
                                "while the string: [%s] include  non-ASCII chars.", aString));
            }
            result = result.add(BigInteger.valueOf(tempChar).multiply(radixBigInteger.pow(k)));
            k++;
        }

        return result;
    }

    /**
     * convert BigInteger to string.
     * Note: radix and basic range are both [1,128], and the sum of radix and basic must also be in [1,128].
     *
     * @param bigInteger the BigInteger to convert
     * @param radix the radix
     * @return the string
     */
    private static String bigIntegerToString(BigInteger bigInteger, int radix)
    {
        if (null == bigInteger) {
            throw new IllegalArgumentException("The parameter bigInteger cannot be null.");
        }

        checkIfBetweenRange(radix, 1, 128);

        StringBuilder resultStringBuilder = new StringBuilder();

        List<Integer> list = new ArrayList<>();
        BigInteger radixBigInteger = BigInteger.valueOf(radix);
        BigInteger currentValue = bigInteger;

        BigInteger quotient = currentValue.divide(radixBigInteger);
        while (quotient.compareTo(BigInteger.ZERO) > 0) {
            list.add(currentValue.remainder(radixBigInteger).intValue());
            currentValue = currentValue.divide(radixBigInteger);
            quotient = currentValue;
        }
        Collections.reverse(list);

        if (list.isEmpty()) {
            list.add(0, bigInteger.remainder(radixBigInteger).intValue());
        }

        Map<Integer, Character> map = new HashMap<>();
        for (int i = 0; i < radix; i++) {
            map.put(i, (char) (i));
        }

        for (Integer aList : list) {
            resultStringBuilder.append(map.get(aList));
        }

        return resultStringBuilder.toString();
    }

    /**
     * Get the minimum and maximum characters in the string (based on ascii judgment).
     * The string must be non-empty and an ascii string.
     * The returned Pair, left=minimum character, right=maximum character.
     *
     * @param aString the string
     * @return pair
     */
    public static Pair<Character, Character> getMinAndMaxCharacter(String aString)
    {
        if (!isPureAscii(aString)) {
            throw new IllegalArgumentException(
                    String.format("When split by string, only ASCII chars are supported, " +
                            "while the string: [%s] include  non-ASCII chars.", aString));
        }

        char min = aString.charAt(0);
        char max = min;

        char temp;
        for (int i = 1, len = aString.length(); i < len; i++) {
            temp = aString.charAt(i);
            min = min < temp ? min : temp;
            max = max > temp ? max : temp;
        }

        return new ImmutablePair<>(min, max);
    }

    /**
     * Check if the string is pure ascii.
     *
     * @param aString the string
     * @return true if the string is pure ascii, otherwise false
     */
    private static boolean isPureAscii(String aString)
    {
        if (null == aString) {
            return false;
        }

        for (int i = 0, len = aString.length(); i < len; i++) {
            char ch = aString.charAt(i);
            if (ch >= 127) {
                return false;
            }
        }
        return true;
    }
}
