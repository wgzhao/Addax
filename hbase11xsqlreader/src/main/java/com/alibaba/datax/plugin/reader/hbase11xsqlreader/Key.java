package com.alibaba.datax.plugin.reader.hbase11xsqlreader;

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
