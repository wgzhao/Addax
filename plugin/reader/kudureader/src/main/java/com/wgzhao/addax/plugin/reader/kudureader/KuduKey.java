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

package com.wgzhao.addax.plugin.reader.kudureader;

import com.google.common.collect.ImmutableMap;
import com.wgzhao.addax.core.base.Key;
import org.apache.kudu.client.KuduPredicate;

import java.util.Map;

/**
 * Created by roy on 2019/12/12 1543.
 */
public final class KuduKey
        extends Key
{

    /** Kudu master addresses. */
    public static final String KUDU_MASTER_ADDRESSES = "masterAddress";

    /** Lower bound. */
    public static final String LOWER_BOUND = "lowerBound";

    /** Upper bound. */
    public static final String UPPER_BOUND = "upperBound";

    /** Split lower bound. */
    public static final String SPLIT_LOWER_BOUND = "splitLowerBound";

    /** Split upper bound. */
    public static final String SPLIT_UPPER_BOUND = "splitUpperBound";

    /** Socket read timeout. */
    public static final String SOCKET_READ_TIMEOUT = "readTimeout";

    /** Scan request timeout. */
    public static final String SCAN_REQUEST_TIMEOUT = "scanTimeout";

    /** The "=" key. */
    public static final Map<String, KuduPredicate.ComparisonOp> KUDU_OPERATORS = ImmutableMap.of(
            "=", KuduPredicate.ComparisonOp.EQUAL,
            ">", KuduPredicate.ComparisonOp.GREATER,
            ">=", KuduPredicate.ComparisonOp.GREATER_EQUAL,
            "<", KuduPredicate.ComparisonOp.LESS,
            "<=", KuduPredicate.ComparisonOp.LESS_EQUAL
    );
}
