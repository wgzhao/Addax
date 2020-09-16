package com.alibaba.datax.plugin.reader.rdbmswriter;

import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;

public class SubCommonRdbmsWriter extends CommonRdbmsWriter {
    static {
        DBUtil.loadDriverClass("writer", "rdbms");
    }

    public static class Job extends CommonRdbmsWriter.Job {
        public Job(DataBaseType dataBaseType) {
            super(dataBaseType);
        }
    }

    public static class Task extends CommonRdbmsWriter.Task {
        public Task(DataBaseType dataBaseType) {
            super(dataBaseType);
        }
    }
}
