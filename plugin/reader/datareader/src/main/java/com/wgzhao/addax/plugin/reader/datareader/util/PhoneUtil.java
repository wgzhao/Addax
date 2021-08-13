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

public class PhoneUtil
{
    private static final String[] PHONE_NUMBER_PREFIXES = {"134", "135", "136", "137", "138", "139", "147", "150,151", "152", "157", "158",
            "159", "182", "187", "188", "130", "131", "132", "145", "155", "156", "185", "186", "145", "133", "153", "180", "181", "189"};


    public static String nextPhoneNumber()
    {
        return CommonUtil.randChoose(PHONE_NUMBER_PREFIXES) + RandomStringUtils.randomNumeric(8);
    }
}
