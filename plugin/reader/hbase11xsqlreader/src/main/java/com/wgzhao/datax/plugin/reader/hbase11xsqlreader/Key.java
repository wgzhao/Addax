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

package com.wgzhao.datax.plugin.reader.hbase11xsqlreader;

import org.apache.hadoop.hbase.HConstants;

public final class Key
{

    private Key() {}

    public static final String MOCK_JOBID_IDENTIFIER = "phoenixreader";
    public static final int MOCK_JOBID = 1;
    public static final String SPLIT_KEY = "phoenixsplit";

    /**
     * 【必选】hbase集群配置，连接一个hbase集群需要的最小配置只有两个：zk和znode
     */
    public static final String HBASE_CONFIG = "hbaseConfig";
    public static final String HBASE_ZK_QUORUM = HConstants.ZOOKEEPER_QUORUM;
    public static final String HBASE_ZNODE_PARENT = HConstants.ZOOKEEPER_ZNODE_PARENT;

    /**
     * 【必选】writer要写入的表的表名
     */
    public static final String TABLE = "table";

    /**
     * 【必选】列配置
     */
    public static final String COLUMN = "column";
}
