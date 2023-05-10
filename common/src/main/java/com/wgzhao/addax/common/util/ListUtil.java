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

import com.wgzhao.addax.common.exception.CommonErrorCode;
import com.wgzhao.addax.common.exception.AddaxException;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 提供针对 Addax 中使用的 List 较为常见的一些封装。 比如：checkIfValueDuplicate 可以用于检查用户配置的 writer
 * 的列不能重复。makeSureNoValueDuplicate亦然，只是会严格报错。
 */
public final class ListUtil
{

    private static final String ERROR_MESSAGE = "Invalid configuration, the List cannot be empty.";

    private ListUtil() {}

    public static void makeSureNoValueDuplicate(List<String> aList,
            boolean caseSensitive)
    {
        if (null == aList || aList.isEmpty()) {
            throw new IllegalArgumentException(ERROR_MESSAGE);
        }

        if (1 != aList.size()) {
            List<String> list;
            if (!caseSensitive) {
                list = valueToLowerCase(aList);
            }
            else {
                list = new ArrayList<>(aList);
            }

            Collections.sort(list);

            for (int i = 0, len = list.size() - 1; i < len; i++) {
                if (list.get(i).equals(list.get(i + 1))) {
                    throw AddaxException
                            .asAddaxException(
                                    CommonErrorCode.CONFIG_ERROR,
                                    String.format(
                                            "您提供的作业配置信息有误, String:[%s] 不允许重复出现在列表中: [%s].",
                                            list.get(i),
                                            StringUtils.join(aList, ",")));
                }
            }
        }
    }

    public static boolean checkIfBInA(List<String> aList, List<String> bList,
            boolean caseSensitive)
    {
        if (null == aList || aList.isEmpty() || null == bList
                || bList.isEmpty()) {
            throw new IllegalArgumentException(ERROR_MESSAGE);
        }

        try {
            makeSureBInA(aList, bList, caseSensitive);
        }
        catch (Exception e) {
            return false;
        }
        return true;
    }

    public static void makeSureBInA(List<String> aList, List<String> bList,
            boolean caseSensitive)
    {
        if (null == aList || aList.isEmpty() || null == bList
                || bList.isEmpty()) {
            throw new IllegalArgumentException(ERROR_MESSAGE);
        }

        List<String> all;
        List<String> part;

        if (!caseSensitive) {
            all = valueToLowerCase(aList);
            part = valueToLowerCase(bList);
        }
        else {
            all = new ArrayList<>(aList);
            part = new ArrayList<>(bList);
        }

        for (String oneValue : part) {
            if (!all.contains(oneValue)) {
                throw AddaxException
                        .asAddaxException(
                                CommonErrorCode.CONFIG_ERROR,
                                String.format(
                                        "您提供的作业配置信息有误, String:[%s] 不存在于列表中:[%s].",
                                        oneValue, StringUtils.join(aList, ",")));
            }
        }
    }

    public static boolean checkIfValueSame(List<Boolean> aList)
    {
        if (null == aList || aList.isEmpty()) {
            throw new IllegalArgumentException(ERROR_MESSAGE);
        }

        if (1 != aList.size()) {
            Boolean firstValue = aList.get(0);
            for (int i = 1, len = aList.size(); i < len; i++) {
                if (firstValue.booleanValue() != aList.get(i).booleanValue()) {
                    return false;
                }
            }
        }
        return true;
    }

    public static List<String> valueToLowerCase(List<String> aList)
    {
        if (null == aList || aList.isEmpty()) {
            throw new IllegalArgumentException(ERROR_MESSAGE);
        }
        List<String> result = new ArrayList<>(aList.size());
        for (String oneValue : aList) {
            result.add(null != oneValue ? oneValue.toLowerCase() : null);
        }

        return result;
    }
}
