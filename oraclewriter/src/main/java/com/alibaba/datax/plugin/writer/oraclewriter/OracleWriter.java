package com.alibaba.datax.plugin.writer.oraclewriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;

import java.util.List;

public class OracleWriter
        extends Writer
{
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Oracle;

    public static class Job
            extends Writer.Job
    {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterJob;

        @Override
        public void preCheck()
        {
            this.init();
            this.commonRdbmsWriterJob.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();

            String writeMode = this.originalConfig.getString(Key.WRITE_MODE);
            if (null != writeMode) {
                if (!"insert".equalsIgnoreCase(writeMode)
                        && !writeMode.startsWith("update"))   {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.CONF_ERROR,
                        String.format("写入模式(writeMode)配置错误. Oracle仅支持insert, update两种模式. %s 不支持",
                                        writeMode));
            }
            }

            this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(
                    DATABASE_TYPE);
            this.commonRdbmsWriterJob.init(this.originalConfig);
        }

        @Override
        public void prepare()
        {
            this.commonRdbmsWriterJob.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            return this.commonRdbmsWriterJob.split(this.originalConfig,
                    mandatoryNumber);
        }

        @Override
        public void post()
        {
            this.commonRdbmsWriterJob.post(this.originalConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsWriterJob.destroy(this.originalConfig);
        }
    }

    public static class Task
            extends Writer.Task
    {
        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Task commonRdbmsWriterTask;

        @Override
        public void init()
        {
            this.writerSliceConfig = getPluginJobConf();
            this.commonRdbmsWriterTask = new CommonRdbmsWriter.Task(DATABASE_TYPE);
            this.commonRdbmsWriterTask.init(this.writerSliceConfig);
        }

        @Override
        public void prepare()
        {
            this.commonRdbmsWriterTask.prepare(this.writerSliceConfig);
        }

        public void startWrite(RecordReceiver recordReceiver)
        {
            this.commonRdbmsWriterTask.startWrite(recordReceiver,
                    this.writerSliceConfig, getTaskPluginCollector());
        }

        @Override
        public void post()
        {
            this.commonRdbmsWriterTask.post(this.writerSliceConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsWriterTask.destroy(this.writerSliceConfig);
        }
    }
}
