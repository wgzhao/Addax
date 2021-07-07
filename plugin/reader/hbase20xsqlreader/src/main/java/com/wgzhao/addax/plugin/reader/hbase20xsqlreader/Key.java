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

package com.wgzhao.addax.plugin.reader.hbase20xsqlreader;

public class Key
{
    /**
     * 【必选】writer要读取的表的表名
     */
    public static final String TABLE = "table";
    /**
     * 【必选】writer要读取哪些列
     */
    public static final String COLUMN = "column";
    /**
     * 【必选】Phoenix QueryServer服务地址
     */
    public static final String QUERYSERVER_ADDRESS = "queryServerAddress";
    /**
     * 【可选】序列化格式，默认为PROTOBUF
     */
    public static final String SERIALIZATION_NAME = "serialization";
    /**
     * 【可选】Phoenix表所属schema，默认为空
     */
    public static final String SCHEMA = "schema";
    /**
     * 【可选】读取数据时切分列
     */
    public static final String SPLIT_KEY = "splitKey";
    /**
     * 【可选】读取数据时切分点
     */
    public static final String SPLIT_POINT = "splitPoint";
    /**
     * 【可选】读取数据过滤条件配置
     */
    public static final String WHERE = "where";
    /**
     * 【可选】查询语句配置
     */
    public static final String QUERY_SQL = "querySql";

    private Key() {}
}
