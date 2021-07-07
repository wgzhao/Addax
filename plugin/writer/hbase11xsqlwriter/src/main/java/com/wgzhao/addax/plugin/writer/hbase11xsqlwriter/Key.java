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

package com.wgzhao.addax.plugin.writer.hbase11xsqlwriter;

import org.apache.hadoop.hbase.HConstants;

public final class Key
{

    /**
     * 【必选】hbase集群配置，连接一个hbase集群需要的最小配置只有两个：zk和znode
     */
    public static final String HBASE_CONFIG = "hbaseConfig";
    public static final String HBASE_ZK_QUORUM = HConstants.ZOOKEEPER_QUORUM;
    public static final String HBASE_ZNODE_PARENT = HConstants.ZOOKEEPER_ZNODE_PARENT;
    public static final String HBASE_THIN_CONNECT_URL = "hbase.thin.connect.url";
    public static final String HBASE_THIN_CONNECT_NAMESPACE = "hbase.thin.connect.namespace";
    public static final String HBASE_THIN_CONNECT_USERNAME = "hbase.thin.connect.username";
    public static final String HBASE_THIN_CONNECT_PASSWORD = "hbase.thin.connect.password";

    /**
     * 【必选】writer要写入的表的表名
     */
    public static final String TABLE = "table";

    /**
     * 【必选】列配置
     */
    public static final String COLUMN = "column";

    /**
     * 【可选】遇到空值默认跳过
     */
    public static final String NULL_MODE = "nullMode";

    /**
     * 【可选】
     * 在writer初始化的时候，是否清空目的表
     * 如果全局启动多个writer，则必须确保所有的writer都prepare之后，再开始导数据。
     */
    public static final String TRUNCATE = "truncate";

    public static final String THIN_CLIENT = "thinClient";

    /**
     * 【可选】批量写入的最大行数，默认100行
     */
    public static final String BATCH_SIZE = "batchSize";

    /**
     * 【可选】是否启用Kerberos 认证
     */
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";

    private Key() {}
}
