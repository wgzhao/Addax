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

package com.wgzhao.addax.plugin.reader.mongodbreader.util;

import com.wgzhao.addax.core.exception.AddaxException;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;

public final class CollectionExpandUtil
{
    private static final Pattern PATTERN = Pattern.compile("(\\w+)\\[(\\d+)-(\\d+)](.*)");

    private CollectionExpandUtil() {}

    public static List<String> expandCollectionNames(String collectionExpr)
    {
        if (collectionExpr == null || collectionExpr.trim().isEmpty()) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The configuration [connection.collection] is required.");
        }

        String expr = collectionExpr.trim();
        if (expr.contains(",")) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "The configuration [connection.collection] only supports a single collection or one wildcard range expression.");
        }

        Matcher matcher = PATTERN.matcher(expr);
        if (!matcher.matches()) {
            return List.of(expr);
        }

        String start = matcher.group(2).trim();
        String end = matcher.group(3).trim();

        if (Integer.parseInt(start) > Integer.parseInt(end)) {
            String temp = start;
            start = end;
            end = temp;
        }

        int paddingLength = start.length();
        String prefix = matcher.group(1).trim();
        String suffix = matcher.group(4).trim();

        List<String> collections = new ArrayList<>();
        for (int k = Integer.parseInt(start); k <= Integer.parseInt(end); k++) {
            String collection;
            if (start.startsWith("0")) {
                collection = prefix + String.format("%0" + paddingLength + "d", k) + suffix;
            }
            else {
                collection = prefix + k + suffix;
            }
            collections.add(collection);
        }
        return collections;
    }
}

