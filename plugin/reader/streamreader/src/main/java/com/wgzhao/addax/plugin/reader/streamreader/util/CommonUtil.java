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

package com.wgzhao.addax.plugin.reader.streamreader.util;

import java.util.random.RandomGenerator;

/**
 * Common Util.
 * <p>
 * Every method takes the generator of its caller instead of keeping one of its own: the plugin
 * builds each record inside one of the concurrently running task threads, so a shared generator
 * would be both a thread safety problem and a source of correlated values.
 */
public final class CommonUtil
{
    private CommonUtil() {}

    /**
     * generate a random digital string with specified length
     * <pre>
     * randomDigitalString(10): 1928128281
     * </pre>
     *
     * @param rng the random generator of the caller
     * @param length the number of digital
     * @return digital string with length, null will be returned if length less than 1
     */
    public static String randomDigitalString(RandomGenerator rng, int length)
    {
        return randomDigitalString(rng, length, 0, 10);
    }

    /** Randomdigitalstring. */
    public static String randomDigitalString(RandomGenerator rng, int length, int origin, int bound)
    {
        if (length < 1 || bound < origin) {
            return null;
        }
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(rng.nextInt(origin, bound));
        }
        return sb.toString();
    }

    /** Randchoose. */
    public static String randChoose(RandomGenerator rng, String[] container)
    {
        return container[rng.nextInt(container.length)];
    }

    /** Randchoose. */
    public static int randChoose(RandomGenerator rng, int[] container)
    {
        return container[rng.nextInt(container.length)];
    }

    /** Randchoose. */
    public static long randChoose(RandomGenerator rng, long[] container)
    {
        return container[rng.nextInt(container.length)];
    }

    /** Randchoose. */
    public static double randChoose(RandomGenerator rng, double[] container)
    {
        return container[rng.nextInt(container.length)];
    }

    /** Randchoose. */
    public static String[] randChoose(RandomGenerator rng, String[][] container)
    {
        return container[rng.nextInt(container.length)];
    }
}
