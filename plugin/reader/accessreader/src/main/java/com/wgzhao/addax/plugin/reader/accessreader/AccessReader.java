package com.wgzhao.addax.plugin.reader.accessreader;

import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.reader.CommonRdbmsReader;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AccessReader extends Reader {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Access;

    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration configuration = null;

        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init() {
            this.configuration = getPluginJobConf();
            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.configuration = this.commonRdbmsReaderJob.init(this.configuration);
        }

        @Override
        public void preCheck() {
            this.commonRdbmsReaderJob.preCheck(this.configuration, DATABASE_TYPE);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderJob.destroy(this.configuration);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return this.commonRdbmsReaderJob.split(this.configuration, adviceNumber);
        }
    }

    public static class Task extends Reader.Task {

        private Configuration configuration = null;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;

        @Override
        public void startRead(RecordSender recordSender) {

            int fetchSize = this.configuration.getInt(Key.FETCH_SIZE, Constant.DEFAULT_FETCH_SIZE);
            this.commonRdbmsReaderTask.startRead(this.configuration, recordSender, this.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void init() {
            this.configuration = getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE, getTaskGroupId(), getTaskId());
            this.commonRdbmsReaderTask.init(this.configuration);
        }

        @Override
        public void destroy() {

            this.commonRdbmsReaderTask.destroy(this.configuration);

        }
    }
}
