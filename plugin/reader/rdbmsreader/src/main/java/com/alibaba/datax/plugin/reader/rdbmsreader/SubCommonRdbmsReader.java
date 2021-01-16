package com.alibaba.datax.plugin.reader.rdbmsreader;

import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;

public class SubCommonRdbmsReader
        extends CommonRdbmsReader
{
    public static class Job
            extends CommonRdbmsReader.Job
    {
        public Job(DataBaseType dataBaseType)
        {
            super(dataBaseType);
        }
    }

    public static class Task
            extends CommonRdbmsReader.Task
    {

        public Task(DataBaseType dataBaseType)
        {
            super(dataBaseType);
        }
    }

    static {
        DBUtil.loadDriverClass("reader", "rdbms");
    }
}
