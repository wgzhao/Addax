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

import org.apache.commons.lang3.RandomStringUtils;

public class StockUtil
{

    private static final String[] CODE_PREFIXES = {
            "68", "10", "16", "75", "42", "73", "07", "90", "87", "13", "14", "08", "11", "43", "19", "71", "01", "56", "70", "37", "76",
            "12", "88", "00", "51", "72", "60", "50", "03", "29", "83", "18", "02", "20", "40", "06", "15", "09", "38", "58", "17", "36",
            "30", "78"
    };

    public static String nextStockCode()
    {
        return CommonUtil.randChoose(CODE_PREFIXES) + RandomStringUtils.secure().nextNumeric(4);
    }

    public static String nextStockAccount()
    {
        return RandomStringUtils.secure().nextNumeric(10);
    }
}
