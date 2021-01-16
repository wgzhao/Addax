package com.alibaba.datax.plugin.reader.influxdbreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class InfluxDBReader
        extends Reader
{

    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig = null;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public void preCheck()
        {
            init();
            originalConfig.getNecessaryValue(Key.ENDPOINT, InfluxDBReaderErrorCode.REQUIRED_VALUE);
            List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
            if (columns == null || columns.isEmpty()) {
                throw DataXException.asDataXException(
                        InfluxDBReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.COLUMN + "] is not set.");
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            Configuration readerSliceConfig = super.getPluginJobConf();
            List<Configuration> splittedConfigs = new ArrayList<>();
            splittedConfigs.add(readerSliceConfig);
            return splittedConfigs;
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            //
        }
    }

    public static class Task
            extends Reader.Task
    {

        private Configuration readerSliceConfig;
        private InfluxDBReaderTask influxDBReaderTask;

        @Override
        public void init()
        {
            this.readerSliceConfig = super.getPluginJobConf();
            this.influxDBReaderTask = new InfluxDBReaderTask(readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            this.influxDBReaderTask.startRead(this.readerSliceConfig, recordSender,
                    super.getTaskPluginCollector());
        }

        @Override
        public void post()
        {
            this.influxDBReaderTask.post();
        }

        @Override
        public void destroy()
        {
            this.influxDBReaderTask.destroy();
        }
    }
}
