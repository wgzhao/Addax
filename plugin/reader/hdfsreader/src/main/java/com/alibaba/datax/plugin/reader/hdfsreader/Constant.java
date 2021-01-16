package com.alibaba.datax.plugin.reader.hdfsreader;

import java.util.Arrays;
import java.util.List;

/**
 * Created by mingya.wmy on 2015/8/14.
 */
public class Constant
{

    public static final String SOURCE_FILES = "sourceFiles";
    public static final String TEXT = "TEXT";
    public static final String ORC = "ORC";
    public static final String CSV = "CSV";
    public static final String SEQ = "SEQ";
    public static final String RC = "RC";
    public static final String PARQUET = "PARQUET";
    protected static final List<String> SUPPORT_FILE_TYPE = Arrays.asList(Constant.CSV, Constant.ORC, Constant.RC, Constant.SEQ, Constant.TEXT, Constant.PARQUET);
    private Constant() {}
}
