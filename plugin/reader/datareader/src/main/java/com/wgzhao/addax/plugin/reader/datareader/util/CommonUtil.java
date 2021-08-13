/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.wgzhao.addax.plugin.reader.datareader.util;

import java.util.Random;

public class CommonUtil
{
    private static final Random RANDOM = new Random();

    /**
     * generate a random digital string with specified length
     * <pre>
     * randomDigitalString(10): 1928128281
     * </pre>
     *
     * @param length the number of digital
     * @return digital string with length, null will be returned if length less than 1
     */
    public static String randomDigitalString(int length)
    {
        return randomDigitalString(length, 0, 10);
    }

    public static String randomDigitalString(int length, int origin, int bound)
    {
        if (length < 1 || bound < origin) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        RANDOM.ints(length, origin, bound).forEach(sb::append);
        return sb.toString();
    }

    public static String randChoose(String[] container)
    {
        return container[RANDOM.nextInt(container.length)];
    }

    public static int randChoose(int[] container)
    {
        return container[RANDOM.nextInt(container.length)];
    }

    public static long randChoose(long[] container)
    {
        return container[RANDOM.nextInt(container.length)];
    }


    public static double randChoose(double[] container)
    {
        return container[RANDOM.nextInt(container.length)];
    }

    public static String[] randChoose(String[][] container)
    {
        return container[RANDOM.nextInt(container.length)];
    }
}
