package com.wgzhao.datax.plugin.reader.sqlserverreader;

import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.plugin.RecordSender;
import com.wgzhao.datax.common.spi.Reader;
import com.wgzhao.datax.common.util.Configuration;
import com.wgzhao.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.wgzhao.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.wgzhao.datax.plugin.rdbms.util.DataBaseType;

import java.util.List;

import static com.wgzhao.datax.plugin.rdbms.reader.Constant.FETCH_SIZE;

public class SqlServerReader
        extends Reader
{

    public static final int DEFAULT_FETCH_SIZE = 1024;
    private static final DataBaseType DATABASE_TYPE = DataBaseType.SQLServer;

    public static class Job
            extends Reader.Job
    {

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();
            int fetchSize = this.originalConfig.getInt(
                    FETCH_SIZE,
                    DEFAULT_FETCH_SIZE);
            if (fetchSize < 1) {
                throw DataXException
                        .asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
                                String.format("您配置的fetchSize有误，根据DataX的设计，fetchSize : [%d] 设置值不能小于 1.",
                                        fetchSize));
            }
            this.originalConfig.set(
                    FETCH_SIZE,
                    fetchSize);

            this.commonRdbmsReaderJob = new SqlServerRdbmsReader.Job(
                    DATABASE_TYPE);
            this.commonRdbmsReaderJob.init(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            return this.commonRdbmsReaderJob.split(this.originalConfig,
                    adviceNumber);
        }

        @Override
        public void post()
        {
            this.commonRdbmsReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsReaderJob.destroy(this.originalConfig);
        }
    }

    public static class Task
            extends Reader.Task
    {

        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;

        @Override
        public void init()
        {
            this.readerSliceConfig = getPluginJobConf();
            this.commonRdbmsReaderTask = new SqlServerRdbmsReader.Task(
                    DATABASE_TYPE, getTaskGroupId(), getTaskId());
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            int fetchSize = this.readerSliceConfig
                    .getInt(FETCH_SIZE);

            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig,
                    recordSender, getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post()
        {
            this.commonRdbmsReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsReaderTask.destroy(this.readerSliceConfig);
        }
    }
}
