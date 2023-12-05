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

import org.apache.commons.rng.simple.RandomSource;

public class PersonUtil
{
    private static final String[] FIRST_NAMES_MALE = {
            "伟", "强", "磊", "洋", "勇", "军", "杰", "涛", "超", "明", "刚", "平", "辉", "鹏", "华", "飞",
            "鑫", "波", "斌", "宇", "浩", "凯", "健", "俊", "帆", "帅", "旭", "宁", "龙", "林", "欢", "佳",
            "阳", "建华", "亮", "成", "建", "峰", "建国", "建军", "晨", "瑞", "志强", "兵", "雷", "东", "博",
            "彬", "坤", "想", "岩", "杨", "文", "利", "楠", "红霞", "建平"};

    private static final String[] FIRST_NAMES_FEMALE = {
            "芳", "娜", "敏", "静", "秀英", "丽", "艳", "娟", "霞", "秀兰", "燕", "玲", "桂英", "丹", "萍",
            "红", "玉兰", "桂兰", "英", "梅", "莉", "秀珍", "婷", "玉梅", "玉珍", "凤英", "晶", "玉英", "颖",
            "雪", "慧", "红梅", "倩", "琴", "兰英", "畅", "云", "洁", "柳", "淑珍", "春梅", "海燕", "冬梅",
            "秀荣", "桂珍", "莹", "秀云", "桂荣", "秀梅", "丽娟", "婷婷", "玉华", "琳", "雪梅", "淑兰", "丽丽",
            "玉", "秀芳", "欣", "淑英", "桂芳", "丽华", "丹丹", "桂香", "淑华", "荣", "秀华", "桂芝", "小红",
            "金凤", "瑜", "桂花", "璐", "凤兰"};

    private static final String[] LAST_NAMES = {"王", "李", "张", "刘", "陈", "杨", "黄", "吴", "赵", "周", "徐", "孙", "马", "朱", "胡", "林", "郭", "何",
            "高", "罗", "郑", "梁", "谢", "宋", "唐", "许", "邓", "冯", "韩", "曹", "曾", "彭", "萧", "蔡", "潘", "田", "董", "袁", "于", "余", "叶", "蒋", "杜",
            "苏", "魏", "程", "吕", "丁", "沈", "任", "姚", "卢", "傅", "钟", "姜", "崔", "谭", "廖", "范", "汪", "陆", "金", "石", "戴", "贾", "韦", "夏", "邱",
            "方", "侯", "邹", "熊", "孟", "秦", "白", "江", "阎", "薛", "尹", "段", "雷", "黎", "史", "龙", "陶", "贺", "顾", "毛", "郝", "龚", "邵", "万", "钱",
            "严", "赖", "覃", "洪", "武", "莫", "孔", "汤", "向", "常", "温", "康", "施", "文", "牛", "樊", "葛", "邢", "安", "齐", "易", "乔", "伍", "庞", "颜",
            "倪", "庄", "聂", "章", "鲁", "岳", "翟", "殷", "詹", "申", "欧", "耿", "关", "兰", "焦", "俞", "左", "柳", "甘", "祝", "包", "宁", "尚", "符", "舒",
            "阮", "柯", "纪", "梅", "童", "凌", "毕", "单", "季", "裴", "霍", "涂", "成", "苗", "谷", "盛", "曲", "翁", "冉", "骆", "蓝", "路", "游", "辛", "靳",
            "欧", "管", "柴", "蒙", "鲍", "华", "喻", "祁", "蒲", "房", "滕", "屈", "饶", "解", "牟", "艾", "尤", "阳", "时", "穆", "农", "司", "卓", "古", "吉",
            "缪", "简", "车", "项", "连", "芦", "麦", "褚", "娄", "窦", "戚", "岑", "景", "党", "宫", "费", "卜", "冷", "晏", "席", "卫", "米", "柏", "宗", "瞿",
            "桂", "全", "佟", "应", "臧", "闵", "苟", "邬", "边", "卞", "姬", "师", "和", "仇", "栾", "隋", "商", "刁", "沙", "荣", "巫", "寇", "桑", "郎", "甄",
            "丛", "仲", "虞", "敖", "巩", "明", "佘", "池", "查", "麻", "苑", "迟", "邝", "官", "封", "谈", "匡", "鞠", "惠", "荆", "乐", "冀", "郁", "胥", "南",
            "班", "储", "原", "栗", "燕", "楚", "鄢", "劳", "谌", "奚", "皮", "粟", "冼", "蔺", "楼", "盘", "满", "闻", "位", "厉", "伊", "仝", "区", "郜", "海",
            "阚", "花", "权", "强", "帅", "屠", "豆", "朴", "盖", "练", "廉", "禹", "井", "祖", "漆", "巴", "丰", "支", "卿", "国", "狄", "平", "计", "索", "宣",
            "晋", "相", "初", "门", "云", "容", "敬", "来", "扈", "晁", "芮", "都", "普", "阙", "浦", "戈", "伏", "鹿", "薄", "邸", "雍", "辜", "羊", "阿", "乌",
            "母", "裘", "亓", "修", "邰", "赫", "杭", "况", "那", "宿", "鲜", "印", "逯", "隆", "茹", "诸", "战", "慕", "危", "玉", "银", "亢", "嵇", "公", "哈",
            "湛", "宾", "戎", "勾", "茅", "利", "于", "呼", "居", "揭", "干", "但", "尉", "冶", "斯", "元", "束", "檀", "衣", "信", "展", "阴", "昝", "智", "幸",
            "奉", "植", "衡", "富", "尧", "闭", "由"};

    public static String nextName()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(CommonUtil.randChoose(LAST_NAMES));
        if (RandomSource.XO_SHI_RO_128_PP.create().nextInt(0, 1) < 0.4) {
            sb.append(CommonUtil.randChoose(FIRST_NAMES_FEMALE));
        } else {
            sb.append(CommonUtil.randChoose(FIRST_NAMES_MALE));
        }
        return sb.toString();
    }
}
