package com.alibaba.datax.plugin.writer.kuduwriter;

public class Constant
{
    public static final String DEFAULT_ENCODING = "UTF-8";

    public static final String COMPRESSION = "DEFAULT_COMPRESSION";
    public static final String ENCODING = "AUTO_ENCODING";
    // unit: second
    public static final Long ADMIN_TIMEOUT = 60L;
    // unit second
    public static final Long SESSION_TIMEOUT = 60L;

    public static final String INSERT_MODE = "upsert";
    public static final long DEFAULT_WRITE_BATCH_SIZE = 512L;
    public static final long DEFAULT_MUTATION_BUFFER_SPACE = 3072L;
}
