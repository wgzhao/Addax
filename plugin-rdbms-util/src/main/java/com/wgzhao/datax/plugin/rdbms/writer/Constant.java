package com.wgzhao.datax.plugin.rdbms.writer;

/**
 * 用于插件解析用户配置时，需要进行标识（MARK）的常量的声明.
 */
public final class Constant
{
    public static final int DEFAULT_BATCH_SIZE = 2048;

    public static final int DEFAULT_BATCH_BYTE_SIZE = 32 * 1024 * 1024;

    public static final String TABLE_NAME_PLACEHOLDER = "@table";

    public static final String CONN_MARK = "connection";

    public static final String TABLE_NUMBER_MARK = "tableNumber";

    public static final String INSERT_OR_REPLACE_TEMPLATE_MARK = "insertOrReplaceTemplate";

    private Constant() {}
}
