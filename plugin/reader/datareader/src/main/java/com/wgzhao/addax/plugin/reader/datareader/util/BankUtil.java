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

import java.security.SecureRandom;

/**
 * generate bank info
 * include bank account
 * bank name
 */
public class BankUtil
{
    private static final SecureRandom RANDOM = new SecureRandom();

    private static final String[] BANKS = {
            "北京银行", "天津银行", "沧州银行", "承德银行", "廊坊银行", "秦皇岛商行", "河北银行", "唐山商行", "张家口商行", "邢台商行", "保定商行",
            "邯郸商行", "大同商行", "晋商银行", "长治商行", "晋城商行", "晋中商行", "阳泉商行", "包商银行", "鄂尔多斯银行", "内蒙古银行", "乌海银行",
            "盛京银行", "鞍山商行", "抚顺商行", "丹东商行", "锦州银行", "长沙银行", "中信银行", "中国光大银行", "华夏银行", "广发银行", "深圳发展银行",
            "招商银行", "上海浦东发展银行", "兴业银行", "民生银行", "恒丰银行", "浙商银行", "渤海银行", "中国工商银行", "中国农业银行", "中国银行",
            "中国建设银行", "交通银行"};

    private static final String[] DEBIT_CARD_PREFIXES = {
            "621660", "621661", "621663", "621667", "621668", "621666", "456351", "601382",
            "621256", "621212", "621283", "620061", "621725", "621330", "621331", "621332",
            "621333", "621297", "621568", "621569", "623208", "621620", "621756", "621757",
            "621758", "621759", "621785", "621786", "621787", "621788", "621789", "621790",
            "621672", "621669", "621662", "623571", "623572", "623575", "623263", "623184",
            "623569", "623586", "623573", "621665", "627025", "627026", "627027", "627028",
            "621293", "621294", "621342", "621343", "621364", "621394", "621648", "621248",
            "621215", "621249", "622771", "622772", "622770", "622273", "622274", "621231",
            "621638", "621334", "621395", "621741", "623040", "621782", "623309", "622348",
            "621041"};

    private static final int DEBIT_CARD_LENGTH = 19;
    private static final int CREDIT_CARD_LENGTH = 16;

    private static final String[] CREDIT_CARD_PREFIXES = {
            "356833", "356835", "409665", "409666", "409668", "409669", "409670", "409671", "409672", "512315", "512316", "512411",
            "512412", "514957", "409667", "518378", "518379", "518474", "518475", "518476", "438088", "524865", "525745", "525746",
            "547766", "552742", "553131", "558868", "514958", "622752", "622753", "622755", "524864", "622757", "622758", "622759",
            "622760", "622761", "622762", "622763", "622756", "628388", "620514", "622754", "518377", "622788", "620040", "558869",
            "377677", "625905", "625906", "625907", "628313", "625333", "628312", "625337", "625338", "625568", "625834", "622764",
            "622765", "625908", "625909", "625910", "620025", "620026", "620531", "620210", "620211", "622479", "622480", "622380",
            "626200", "620019", "620035", "622789", "625140", "620513", "620202", "620203", "622346", "622347", "622790", "622789",
            "622789", "622789", "622789", "622789", "622789", "622789", "622789", "622789", "622789", "624405", "628448"};

    /**
     * generate a faker bank company name
     *
     * @return bank name
     */
    public static String nextBank()
    {
        return BANKS[RANDOM.nextInt(BANKS.length)];
    }

    public static String nextDebitCard()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(CommonUtil.randChoose(DEBIT_CARD_PREFIXES));
        sb.append(RandomStringUtils.secure().nextNumeric(DEBIT_CARD_LENGTH - sb.length()));
        return sb.toString();
    }

    public static String nextCreditCard()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(CommonUtil.randChoose(CREDIT_CARD_PREFIXES));
        sb.append(RandomStringUtils.secure().nextNumeric(CREDIT_CARD_LENGTH - sb.length()));
        return sb.toString();
    }
}
