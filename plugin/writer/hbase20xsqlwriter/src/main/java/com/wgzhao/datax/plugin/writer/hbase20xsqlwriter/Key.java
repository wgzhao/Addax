package com.wgzhao.datax.plugin.writer.hbase20xsqlwriter;

public class Key
{

    /**
     * 【必选】writer要写入的表的表名
     */
    public static final String TABLE = "table";
    /**
     * 【必选】writer要写入哪些列
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
     * 【可选】批量写入的最大行数，默认100行
     */
    public static final String BATCHSIZE = "batchSize";

    /**
     * 【可选】遇到空值默认跳过
     */
    public static final String NULLMODE = "nullMode";
    /**
     * 【可选】Phoenix表所属schema，默认为空
     */
    public static final String SCHEMA = "schema";

    private Key() {}
}
