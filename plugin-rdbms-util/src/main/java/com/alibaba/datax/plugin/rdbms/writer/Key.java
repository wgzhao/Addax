package com.alibaba.datax.plugin.rdbms.writer;

public final class Key
{
    public static final String JDBC_URL = "jdbcUrl";

    public static final String USERNAME = "username";

    public static final String PASSWORD = "password";

    public static final String PASSFLAG = "passflag";

    public static final String TABLE = "table";

    public static final String COLUMN = "column";

    //可选值为：insert,replace，默认为 insert （mysql 支持，oracle 没用 replace 机制，只能 insert,oracle 可以不暴露这个参数）
    public static final String WRITE_MODE = "writeMode";

    public static final String PRE_SQL = "preSql";

    public static final String POST_SQL = "postSql";

    //默认值：2048
    public static final String BATCH_SIZE = "batchSize";

    //默认值：32m
    public static final String BATCH_BYTE_SIZE = "batchByteSize";

    public static final String EMPTY_AS_NULL = "emptyAsNull";

    public static final String DRYRUN = "dryRun";

    private Key() {}
}