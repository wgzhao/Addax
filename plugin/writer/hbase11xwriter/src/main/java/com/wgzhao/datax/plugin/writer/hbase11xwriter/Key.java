package com.wgzhao.datax.plugin.writer.hbase11xwriter;

public final class Key
{

    public static final String HBASE_CONFIG = "hbaseConfig";

    public static final String TABLE = "table";

    /**
     * mode 可以取 normal 或者 multiVersionFixedColumn 或者 multiVersionDynamicColumn 三个值，无默认值。
     * <p>
     * normal 配合 column(Map 结构的)使用
     * <p>
     * multiVersion
     */
    public static final String MODE = "mode";

    public static final String ROWKEY_COLUMN = "rowkeyColumn";

    public static final String VERSION_COLUMN = "versionColumn";

    /**
     * 默认为 utf8
     */
    public static final String ENCODING = "encoding";

    public static final String COLUMN = "column";

    public static final String INDEX = "index";

    public static final String NAME = "name";

    public static final String TYPE = "type";

    public static final String VALUE = "value";

//    public static final String FORMAT = "format"

    /**
     * 默认为 EMPTY_BYTES
     */
    public static final String NULL_MODE = "nullMode";

    public static final String TRUNCATE = "truncate";

//    public static final String AUTO_FLUSH = "autoFlush"

    public static final String WAL_FLAG = "walFlag";

    public static final String WRITE_BUFFER_SIZE = "writeBufferSize";

    private Key() {}
    
}
