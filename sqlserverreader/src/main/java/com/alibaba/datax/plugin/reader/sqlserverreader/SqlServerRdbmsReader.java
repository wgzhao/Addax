package com.alibaba.datax.plugin.reader.sqlserverreader;

import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerRdbmsReader extends CommonRdbmsReader {
    static {
        DBUtil.loadDriverClass("reader", "rdbms");
    }

    public static class Job extends CommonRdbmsReader.Job {
        public Job(DataBaseType dataBaseType) {
            super(dataBaseType);
        }
    }

    public static class Task extends CommonRdbmsReader.Task {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        //private BASE64Encoder encoder = new BASE64Encoder();

        public Task(DataBaseType dataBaseType) {
            super(dataBaseType);
        }

        public Task(DataBaseType dataBaseType, int taskGropuId, int taskId) {
            super(dataBaseType, taskGropuId, taskId);
        }
    }
}
