package com.wgzhao.addax.plugin.writer.databendwriter;

import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.plugin.writer.databendwriter.util.DatabendWriterUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DatabendWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Databend;

    public static class Job
            extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig;
        private CommonRdbmsWriter.Job commonRdbmsWriterMaster;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.commonRdbmsWriterMaster = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterMaster.init(this.originalConfig);
            // placeholder currently not supported by databend driver, needs special treatment
            DatabendWriterUtil.dealWriteMode(this.originalConfig);
        }

        @Override
        public void preCheck() {
            this.init();
            this.commonRdbmsWriterMaster.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterMaster.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.commonRdbmsWriterMaster.split(this.originalConfig, mandatoryNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsWriterMaster.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterMaster.destroy(this.originalConfig);
        }
    }


    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        private CommonRdbmsWriter.Task commonRdbmsWriterSlave;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();

            this.commonRdbmsWriterSlave = new CommonRdbmsWriter.Task(DataBaseType.Databend);
            this.commonRdbmsWriterSlave.init(this.writerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterSlave.destroy(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterSlave.prepare(this.writerSliceConfig);
        }

        @Override
        public void post() {
            this.commonRdbmsWriterSlave.post(this.writerSliceConfig);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            this.commonRdbmsWriterSlave.startWrite(lineReceiver, this.writerSliceConfig, this.getTaskPluginCollector());
        }

    }
}