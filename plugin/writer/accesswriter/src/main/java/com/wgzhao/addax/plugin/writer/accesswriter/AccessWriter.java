package com.wgzhao.addax.plugin.writer.accesswriter;


import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;

import java.util.List;

public class AccessWriter extends Writer {

    private static final DataBaseType DATABASE_TYPE = DataBaseType.Access;

    public static class Job extends Writer.Job {

        private Configuration originalConfig = null;

        private CommonRdbmsWriter.Job commonRdbmsWriterJob = null;
        @Override
        public void init() {
            this.originalConfig = getPluginJobConf();
            this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterJob.init(this.originalConfig);
        }

        @Override
        public void preCheck() {
            this.commonRdbmsWriterJob.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }
        @Override
        public void prepare() {
            this.commonRdbmsWriterJob.prepare(this.originalConfig);
        }

        @Override
        public void post() {
            this.commonRdbmsWriterJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterJob.destroy(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.commonRdbmsWriterJob.split(this.originalConfig, mandatoryNumber);
        }
    }

    public static class Task extends Writer.Task {

        private Configuration configuration;

        private CommonRdbmsWriter.Task commonRdbmsWriterTask;

        @Override
        public void init() {
            this.configuration = super.getPluginJobConf();
            this.commonRdbmsWriterTask = new CommonRdbmsWriter.Task(DATABASE_TYPE);
            this.commonRdbmsWriterTask.init(this.configuration);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterTask.prepare(this.configuration);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            this.commonRdbmsWriterTask.startWrite(lineReceiver, this.configuration, super.getTaskPluginCollector());
        }

        @Override
        public void post() {
            this.commonRdbmsWriterTask.post(configuration);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterTask.destroy(configuration);
        }
    }
}
