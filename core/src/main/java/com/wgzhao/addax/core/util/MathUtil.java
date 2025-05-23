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

package com.wgzhao.addax.core.util;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 数学计算工具类
 * 提供常用的数值加减乘除计算 , 及多个数值的求和 , 平均值 , 最大最小值计算
 */
public class MathUtil
{
    /**
     * 默认的除法精确度
     */
    private static final int DEF_DIV_SCALE = 2;

    private MathUtil() {}

    /**
     * 精确加法运算
     *
     * @param v1 被加数
     * @param v2 加数
     * @return 两个参数的和(BigDecimal)
     */
    public static BigDecimal add(BigDecimal v1, BigDecimal v2)
    {
        if (null == v1) {
            v1 = BigDecimal.ZERO;
        }
        if (null == v2) {
            v2 = BigDecimal.ZERO;
        }
        return v1.add(v2);
    }

    /**
     * 精确减法运算
     *
     * @param v1 被减数
     * @param v2 减数
     * @return 两个参数的差(BigDecimal)
     */
    public static BigDecimal subtract(BigDecimal v1, BigDecimal v2)
    {
        if (null == v1) {
            v1 = BigDecimal.ZERO;
        }
        if (null == v2) {
            v2 = BigDecimal.ZERO;
        }
        return v1.subtract(v2);
    }

    /**
     * 精确乘法运算
     *
     * @param v1 被乘数
     * @param v2 乘数
     * @return 两个参数的积(BigDecimal)
     */
    public static BigDecimal multiply(BigDecimal v1, BigDecimal v2)
    {
        if (null == v1) {
            v1 = BigDecimal.ONE;
        }
        if (null == v2) {
            v2 = BigDecimal.ONE;
        }
        return v1.multiply(v2);
    }

    /**
     * 幂运算
     * @param v1 底数
     * @param v2 指数
     * @return v1的v2次方
     */
    public static BigDecimal pow(BigDecimal v1, int v2)
    {
        if (null == v1) {
            v1 = BigDecimal.ONE;
        }
        return v1.pow(v2);
    }

    /**
     * ( 相对 )精确除法运算 , 当发生除不尽情况时 , 精确到 小数点以后2位 , 以后数字四舍五入
     *
     * @param v1 被除数
     * @param v2 除数
     * @return 两个参数的商(BigDecimal)
     */
    public static BigDecimal divide(BigDecimal v1, BigDecimal v2)
    {
        return v1.divide(v2, DEF_DIV_SCALE, RoundingMode.HALF_UP);
    }

    /**
     * ( 相对 )精确除法运算 . 当发生除不尽情况时 , 由scale参数指 定精度 , 以后数字四舍五入
     *
     * @param v1 被除数
     * @param v2 除数
     * @param scale 表示表示需要精确到小数点以后几位
     * @return 两个参数的商(BigDecimal)
     */
    public static BigDecimal divide(BigDecimal v1, BigDecimal v2, Integer scale)
    {
        if (null == v1) {
            return BigDecimal.ZERO;
        }
        if (null == v2) {
            v2 = BigDecimal.ONE;
        }

        if (v2.compareTo(BigDecimal.ZERO) == 0) {
            throw new IllegalArgumentException("The divisor cannot be zero.");
        }

        if (scale < 0) {
            throw new IllegalArgumentException("The scale cannot be less than zero.");
        }

        return v1.divide(v2, scale, RoundingMode.HALF_UP);
    }

    /**
     * 求余数操作
     *
     * @param v1 被除数
     * @param v2 除数
     * @return 两个参数的余数
     */
    public static BigDecimal mod(BigDecimal v1, BigDecimal v2)
    {
        if (null == v1) {
            return BigDecimal.ZERO;
        }
        if (null == v2) {
            v2 = BigDecimal.ONE;
        }

        if (v2.compareTo(BigDecimal.ZERO) == 0) {
            throw new IllegalArgumentException("The divisor cannot be zero.");
        }

        BigDecimal[] c = v1.divideAndRemainder(v2);
        return c[1];
    }

    /**
     * 精确加法运算
     *
     * @param v1 被加数
     * @param v2 加数
     * @return 两个参数的和(String)
     */
    public static String add(String v1, String v2)
    {
        if (isBlank(v1)) {
            v1 = "0";
        }
        if (isBlank(v2)) {
            v2 = "0";
        }
        BigDecimal b1 = new BigDecimal(v1.trim());
        BigDecimal b2 = new BigDecimal(v2.trim());
        return String.valueOf(add(b1, b2));
    }

    /**
     * 精确减法运算
     *
     * @param v1 被减数
     * @param v2 减数
     * @return 两个参数的差(String)
     */
    public static String subtract(String v1, String v2)
    {
        if (isBlank(v1)) {
            v1 = "0";
        }
        if (isBlank(v2)) {
            v2 = "0";
        }
        BigDecimal b1 = new BigDecimal(v1.trim());
        BigDecimal b2 = new BigDecimal(v2.trim());
        return String.valueOf(subtract(b1, b2));
    }

    /**
     * 精确乘法运算
     *
     * @param v1 被乘数
     * @param v2 乘数
     * @return 两个参数的积(String)
     */
    public static String multiply(String v1, String v2)
    {
        if (isBlank(v1)) {
            v1 = "1";
        }
        if (isBlank(v2)) {
            v2 = "1";
        }
        BigDecimal b1 = new BigDecimal(v1.trim());
        BigDecimal b2 = new BigDecimal(v2.trim());
        return String.valueOf(multiply(b1, b2));
    }

    /**
     * 幂运算
     * @param v1 底数
     * @param v2 指数
     * @return v1的v2次方值
     */
    public static String pow(String v1, String v2)
    {
        if (isBlank(v1)) {
            v1 = "1";
        }
        if (isBlank(v2)) {
            v2 = "0";
        }
        BigDecimal b1 = new BigDecimal(v1.trim());
        int b2 = Integer.parseInt(v2.trim());
        return String.valueOf(pow(b1, b2));
    }

    /**
     * ( 相对 )精确除法运算 , 当发生除不尽情况时 , 精确到 小数点以后2位 , 以后数字四舍五入
     *
     * @param v1 被除数
     * @param v2 除数
     * @return 两个参数的商(String)
     */
    public static String divide(String v1, String v2)
    {
        return divide(v1, v2, DEF_DIV_SCALE);
    }

    /**
     * ( 相对 )精确除法运算 . 当发生除不尽情况时 , 由scale参数指 定精度 , 以后数字四舍五入
     *
     * @param v1 被除数
     * @param v2 除数
     * @param scale 表示表示需要精确到小数点以后几位
     * @return 两个参数的商(String)
     */
    public static String divide(String v1, String v2, Integer scale)
    {
        if (null == v1) {
            return "0";
        }
        if (null == v2) {
            v2 = "1";
        }
        BigDecimal b1 = new BigDecimal(v1.trim());
        BigDecimal b2 = new BigDecimal(v2.trim());
        return String.valueOf(divide(b1, b2, scale));
    }

    /**
     * 求余数操作
     *
     * @param v1 被除数
     * @param v2 除数
     * @return 两个参数的余数
     */
    public static String mod(String v1, String v2)
    {
        if (null == v1) {
            return "0";
        }
        if (null == v2) {
            v2 = "1";
        }
        BigDecimal b1 = new BigDecimal(v1.trim());
        BigDecimal b2 = new BigDecimal(v2.trim());

        BigDecimal[] c = b1.divideAndRemainder(b2);
        return String.valueOf(c[1]);
    }

    /**
     * 精确加法运算 , 计算多个数值总和 , 若其中有null值则忽略
     *
     * @param v1 被加数
     * @param valList 被加数集合
     * @return 两个参数的和(BigDecimal)
     */
    public static BigDecimal sum(BigDecimal v1, BigDecimal... valList)
    {
        if (null == v1) {
            v1 = BigDecimal.ZERO;
        }
        if (null == valList) {
            return v1;
        }
        for (BigDecimal val : valList) {
            if (null != val) {
                v1 = v1.add(val);
            }
        }
        return v1;
    }

    /**
     * 精确加法运算 , 计算多个数值总和 , 若其中有null值则忽略
     *
     * @param v1 要计算的值
     * @param valList 被加数集合
     * @return 两个参数的和(String)
     */
    public static String sum(String v1, String... valList)
    {
        if (isBlank(v1)) {
            v1 = "0";
        }
        if (null == valList || valList.length == 0) {
            return v1;
        }
        BigDecimal b1 = new BigDecimal(v1.trim());
        for (String val : valList) {
            if (!isBlank(val)) {
                b1 = add(b1, new BigDecimal(val.trim()));
            }
        }
        return String.valueOf(b1);
    }

    /**
     * 平均数
     *
     * @param valList list of decimal
     * @return decimal
     */
    public static BigDecimal avg(BigDecimal... valList)
    {
        if (null != valList && valList.length != 0) {
            return divide(sum(BigDecimal.ZERO, valList), new BigDecimal(valList.length));
        }
        return BigDecimal.ZERO;
    }

    /**
     * 平均数
     *
     * @param valList list of string
     * @return string
     */
    public static String avg(String... valList)
    {
        if (null != valList && valList.length != 0) {
            return divide(sum("0", valList), String.valueOf(valList.length));
        }
        return "0";
    }

    /**
     * 最大值
     *
     * @param v1 decimal
     * @param valList list of decimal
     * @return decimal
     */
    public static BigDecimal max(BigDecimal v1, BigDecimal... valList)
    {
        BigDecimal max = v1;
        if (null == valList) {
            return max;
        }
        for (BigDecimal val : valList) {
            if (null != val && val.compareTo(max) > 0) {
                max = val;
            }
        }
        return max;
    }

    /**
     * 最大值
     *
     * @param valList list of decimal
     * @return decimal
     */
    public static BigDecimal maxArr(BigDecimal... valList)
    {
        if (null == valList || valList.length == 0) {
            return null;
        }

        return max(valList[0], valList);
    }

    /**
     * 最小值
     *
     * @param v1 decimal
     * @param valList list of string
     * @return decimal
     */
    public static BigDecimal min(BigDecimal v1, BigDecimal... valList)
    {
        BigDecimal min = v1;
        if (null == valList) {
            return min;
        }
        for (BigDecimal val : valList) {
            if (null != val && val.compareTo(min) < 0) {
                min = val;
            }
        }
        return min;
    }

    /**
     * 最小值
     *
     * @param valList list of string
     * @return decimal
     */
    public static BigDecimal minArr(BigDecimal... valList)
    {
        if (null == valList || valList.length == 0) {
            return null;
        }
        return min(valList[0], valList);
    }

    /**
     * 最大值
     *
     * @param v1 String
     * @param valList list of string
     * @return String
     */
    public static String max(String v1, String... valList)
    {
        if (isBlank(v1)) {
            return null;
        }
        if (null == valList || valList.length == 0) {
            return v1;
        }
        BigDecimal maxBd = new BigDecimal(v1.trim());

        for (String val : valList) {
            if (!isBlank(val) && new BigDecimal(val).compareTo(maxBd) > 0) {
                maxBd = new BigDecimal(val);
            }
        }
        return String.valueOf(maxBd);
    }

    /**
     * 最大值
     *
     * @param valList list of string
     * @return String
     */
    public static String maxArr(String... valList)
    {
        if (null == valList || valList.length == 0) {
            return null;
        }
        return max(valList[0], valList);
    }

    /**
     * 最小值
     *
     * @param v1 string
     * @param valList list of string
     * @return String
     */
    public static String min(String v1, String... valList)
    {
        if (isBlank(v1)) {
            return null;
        }
        if (null == valList || valList.length == 0) {
            return v1;
        }
        BigDecimal minBd = new BigDecimal(v1.trim());

        for (String val : valList) {
            if (!isBlank(val) && new BigDecimal(val).compareTo(minBd) < 0) {
                minBd = new BigDecimal(val);
            }
        }
        return String.valueOf(minBd);
    }

    /**
     * 最小值
     *
     * @param valList list of string
     * @return String
     */
    public static String minArr(String... valList)
    {
        if (null == valList || valList.length == 0) {
            return null;
        }
        return min(valList[0], valList);
    }

    /**
     * 判断字符串是否为空(不依赖第三方)
     *
     * @param str string
     * @return boolean
     */
    private static boolean isBlank(String str)
    {
        return null == str || str.trim().isEmpty();
    }
}
