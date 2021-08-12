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

public class CompanyUtil
{
    private static final String[] COMPANY_PREFIXES = {
            "超艺", "和泰", "九方", "鑫博腾飞", "戴硕电子", "济南亿次元", "海创", "创联世纪", "凌云", "泰麒麟", "彩虹", "兰金电子", "晖来计算机", "天益",
            "恒聪百汇", "菊风公司", "惠派国际公司", "创汇", "思优", "时空盒数字", "易动力", "飞海科技", "华泰通安", "盟新", "商软冠联", "图龙信息", "易动力",
            "华远软件", "创亿", "时刻", "开发区世创", "明腾", "良诺", "天开", "毕博诚", "快讯", "凌颖信息", "黄石金承", "恩悌", "雨林木风计算机", "双敏电子",
            "维旺明", "网新恒天", "数字100", "飞利信", "立信电子", "联通时科", "中建创业", "新格林耐特", "新宇龙信息", "浙大万朋", "MBP软件", "昂歌信息",
            "万迅电脑", "方正科技", "联软", "七喜", "南康", "银嘉", "巨奥", "佳禾", "国讯", "信诚致远", "浦华众城", "迪摩", "太极", "群英", "合联电子",
            "同兴万点", "襄樊地球村", "精芯", "艾提科信", "昊嘉", "鸿睿思博", "四通", "富罳", "商软冠联", "诺依曼软件", "东方峻景", "华成育卓", "趋势",
            "维涛", "通际名联"};
    private static final String[] COMPANY_TYPES = {"科技", "网络", "信息", "传媒"};

    public static String nextCompany()
    {
        return CommonUtil.randChoose(COMPANY_PREFIXES) + CommonUtil.randChoose(COMPANY_TYPES) + "有限公司";
    }

}
