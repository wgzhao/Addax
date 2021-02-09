package com.wgzhao.datax.plugin.reader.rdbmsreader;

import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.plugin.RecordSender;
import com.wgzhao.datax.common.spi.Reader;
import com.wgzhao.datax.common.util.Configuration;
import com.wgzhao.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.wgzhao.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.wgzhao.datax.plugin.rdbms.util.DataBaseType;

import java.util.List;

import static com.wgzhao.datax.plugin.rdbms.reader.Constant.FETCH_SIZE;

public class RdbmsReader
        extends Reader
{
    private static final DataBaseType DATABASE_TYPE = DataBaseType.RDBMS;

    public static class Job
            extends Reader.Job
    {

        private Configuration originalConfig;
        private CommonRdbmsReader.Job commonRdbmsReaderMaster;

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();
            int fetchSize = this.originalConfig.getInt(FETCH_SIZE, Constant.DEFAULT_FETCH_SIZE);
            if (fetchSize < 1) {
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.REQUIRED_VALUE,
                                String.format(
                                        "您配置的fetchSize有误，根据DataX的设计，fetchSize : [%d] 设置值不能小于 1.",
                                        fetchSize));
            }
            this.originalConfig.set(FETCH_SIZE, fetchSize);

            this.commonRdbmsReaderMaster = new SubCommonRdbmsReader.Job(DATABASE_TYPE);
            this.commonRdbmsReaderMaster.init(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            return this.commonRdbmsReaderMaster.split(this.originalConfig,
                    adviceNumber);
        }

        @Override
        public void post()
        {
            this.commonRdbmsReaderMaster.post(this.originalConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsReaderMaster.destroy(this.originalConfig);
        }
    }

    public static class Task
            extends Reader.Task
    {

        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderSlave;

        @Override
        public void init()
        {
            this.readerSliceConfig = getPluginJobConf();
            this.commonRdbmsReaderSlave = new SubCommonRdbmsReader.Task(
                    DATABASE_TYPE);
            this.commonRdbmsReaderSlave.init(this.readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            int fetchSize = this.readerSliceConfig
                    .getInt(FETCH_SIZE);

            this.commonRdbmsReaderSlave.startRead(this.readerSliceConfig,
                    recordSender, getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post()
        {
            this.commonRdbmsReaderSlave.post(this.readerSliceConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsReaderSlave.destroy(this.readerSliceConfig);
        }
    }
}
