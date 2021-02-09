package com.wgzhao.datax.plugin.reader.hdfsreader;

/**
 * 声明插件用到的需要插件使用者提供的配置项
 */
public final class Key
{

    public static final String PATH = "path";
    public static final String DEFAULT_FS = "defaultFS";
    public static final String FILETYPE = "fileType";
    public static final String HADOOP_CONFIG = "hadoopConfig";
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";

    private Key() {}
}
