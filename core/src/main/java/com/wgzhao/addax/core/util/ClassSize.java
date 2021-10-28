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

package com.wgzhao.addax.core.util;

/**
 * Created by liqiang on 15/12/12.
 */
public class ClassSize
{

    public static final int DEFAULT_RECORD_HEAD;
    public static final int COLUMN_HEAD;

    //objectHead的大小
    public static final int REFERENCE;
    public static final int OBJECT;
    public static final int ARRAY;
    public static final int ARRAYLIST;

    private ClassSize() {}

    public static int align(int num)
    {
        return (int) (align((long) num));
    }

    public static long align(long num)
    {
        //The 7 comes from that the alignSize is 8 which is the number of bytes
        //stored and sent together
        return ((num + 7) >> 3) << 3;
    }

    static {
        //only 64位
        REFERENCE = 8;

        OBJECT = 2 * REFERENCE;

        ARRAY = align(3 * REFERENCE);

        // 16+8+24+16
        ARRAYLIST = align(OBJECT + align(REFERENCE) + align(ARRAY) +
                (2 * Long.SIZE / Byte.SIZE));
        // 8+64+8
        DEFAULT_RECORD_HEAD = align(align(REFERENCE) + ClassSize.ARRAYLIST + 2 * Integer.SIZE / Byte.SIZE);
        //16+4
        COLUMN_HEAD = align(2 * REFERENCE + Integer.SIZE / Byte.SIZE);
    }
}
