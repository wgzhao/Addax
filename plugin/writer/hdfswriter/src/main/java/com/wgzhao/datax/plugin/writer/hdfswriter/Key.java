package com.wgzhao.datax.plugin.writer.hdfswriter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by shf on 15/10/8.
 */
public class Key
{
    // must have
    public static final String PATH = "path";
    //must have
    public static final String DEFAULT_FS = "defaultFS";
    //must have
    public static final String FILE_TYPE = "fileType";
    // must have
    public static final String FILE_NAME = "fileName";
    // must have for column
    public static final String COLUMN = "column";
    public static final String NAME = "name";
    public static final String TYPE = "type";
//    public static final String DATE_FORMAT = "dateFormat"
    // must have
    public static final String WRITE_MODE = "writeMode";
    // must have
    public static final String FIELD_DELIMITER = "fieldDelimiter";
    // not must, default UTF-8
    public static final String ENCODING = "encoding";
    // not must, default no compress
    public static final String COMPRESS = "compress";
    // not must, not default \N
//    public static final String NULL_FORMAT = "nullFormat"
    // Kerberos
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";
    // hadoop config
    public static final String HADOOP_CONFIG = "hadoopConfig";

    // hdfs format

    protected static final Set<String> SUPPORT_FORMAT = new HashSet<>(Arrays.asList("ORC", "PARQUET", "TEXT"));

    private Key() {}
}
