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
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;

public class EmailUtil {

    private static final String[] EMAIL_DOMAIN = {"gmail.com", "yahoo.com","aol.com","qq.com","163.com","sina.com",
                                    "sina.com.cn","proton.me","outlook.com","hotmail.com","icloud.com"};

    private static final UniformRandomProvider rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
    private static final int userLength = 10;

    public static String nextEmail() {
        return RandomStringUtils.randomAlphanumeric(
                                rng.nextInt(3, userLength)) + "@" + CommonUtil.randChoose(EMAIL_DOMAIN);
        
    }
}
