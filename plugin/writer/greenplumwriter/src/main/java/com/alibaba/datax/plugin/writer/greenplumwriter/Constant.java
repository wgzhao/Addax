package com.alibaba.datax.plugin.writer.greenplumwriter;

public class Constant
{
    public static final int TIME_OUT_MS = 5000;

    // 字段分隔符
    public static final char DELIMTER = '\u0001';

    // 字段引用符号，这里没有使用常见的双引号，是考虑到json数据类型会包含该符号
    public static final char QUOTE_CHAR = '\u0002';

    public static final char NEWLINE = '\n';

    public static final char ESCAPE = '\\';

    // 因为 GPDB 服务端 对 COPY FROM 的 CSV 格式做了这样的限制，如果单个元组大于4 MB，只能使用 insert
    public static final int MAX_CSV_SIZE = 4194304;

    //  线程异步队列大小，增大此参数增加内存消耗，提升性能
    public static final int COPY_QUEUE_SIZE = 1000;

    // 用于进行格式化数据的线程数
    public static final int NUM_COPY_PROCESSOR = 4;

    // 写入数据库的并发数
    public static final int NUM_COPY_WRITER = 1;


}
