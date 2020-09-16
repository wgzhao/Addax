package com.alibaba.datax.plugin.reader.rdbmsreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class SubCommonRdbmsReader extends CommonRdbmsReader {
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
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        public Task(DataBaseType dataBaseType) {
            super(dataBaseType);
        }

        @Override
        protected Record transportOneRecord(RecordSender recordSender,
                ResultSet rs, ResultSetMetaData metaData, int columnNumber,
                String mandatoryEncoding,
                TaskPluginCollector taskPluginCollector) {
            com.alibaba.datax.common.element.Record record = buildRecord(recordSender, rs,
                    metaData, columnNumber, mandatoryEncoding, taskPluginCollector);

            recordSender.sendToWriter(record);
            return record;
        }
    }
}
