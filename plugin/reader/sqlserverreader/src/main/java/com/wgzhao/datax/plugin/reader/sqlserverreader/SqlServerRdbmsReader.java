package com.wgzhao.datax.plugin.reader.sqlserverreader;

import com.wgzhao.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.wgzhao.datax.plugin.rdbms.util.DBUtil;
import com.wgzhao.datax.plugin.rdbms.util.DataBaseType;

public class SqlServerRdbmsReader
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

        public Task(DataBaseType dataBaseType, int taskGropuId, int taskId)
        {
            super(dataBaseType, taskGropuId, taskId);
        }
    }

    static {
        DBUtil.loadDriverClass("reader", "rdbms");
    }
}
