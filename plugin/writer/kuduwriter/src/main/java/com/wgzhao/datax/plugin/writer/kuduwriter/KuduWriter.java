package com.wgzhao.datax.plugin.writer.kuduwriter;

import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.plugin.RecordReceiver;
import com.wgzhao.datax.common.spi.Writer;
import com.wgzhao.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class KuduWriter
        extends Writer
{
    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration config = null;
        private String writeMode;

        @Override
        public void init()
        {
            this.config = this.getPluginJobConf();
            this.validateParameter();
        }

        private void validateParameter()
        {
            config.getNecessaryValue(Key.KUDU_TABLE_NAME, KuduWriterErrorCode.REQUIRED_VALUE);
            config.getNecessaryValue(Key.KUDU_MASTER_ADDRESSES, KuduWriterErrorCode.REQUIRED_VALUE);
            config.getNecessaryValue(Key.KUDU_TABLE_NAME, KuduWriterErrorCode.REQUIRED_VALUE);

            // column check
            List<Configuration> columns = this.config.getListConfiguration(Key.COLUMN);
            if (null == columns || columns.isEmpty()) {
                throw DataXException.asDataXException(
                        KuduWriterErrorCode.REQUIRED_VALUE, "您需要指定 columns"
                );
            } else {
                for (Configuration eachColumnConf : columns) {
                    eachColumnConf.getNecessaryValue(Key.NAME, KuduWriterErrorCode.REQUIRED_VALUE);
                    eachColumnConf.getNecessaryValue(Key.TYPE, KuduWriterErrorCode.REQUIRED_VALUE);
                }
            }
            // writeMode check
            this.writeMode = this.config.getString(Key.WRITE_MODE, Constant.INSERT_MODE);
            this.config.set(Key.WRITE_MODE, this.writeMode);

            // timeout

        }
        @Override
        public void prepare()
        {
            Boolean truncate = config.getBool(Key.TRUNCATE, false);
            if (truncate) {
                KuduHelper.truncateTable(this.config);
            }

            if (!KuduHelper.isTableExists(config)) {
                //KuduHelper.createTable(config);
                // we DO NOT create table
                throw DataXException.asDataXException(KuduWriterErrorCode.TABLE_NOT_EXISTS,
                        KuduWriterErrorCode.TABLE_NOT_EXISTS.getDescription());
            }
        }

        @Override
        public List<Configuration> split(int i)
        {
            List<Configuration> splitResultConfigs = new ArrayList<>();
            for (int j = 0; j < i; j++) {
                splitResultConfigs.add(config.clone());
            }

            return splitResultConfigs;
        }

        @Override
        public void destroy()
        {
            //
        }
    }

    public static class Task
            extends Writer.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration taskConfig;
        private KuduWriterTask kuduTaskProxy;

        @Override
        public void init()
        {
            this.taskConfig = getPluginJobConf();
            this.kuduTaskProxy = new KuduWriterTask(this.taskConfig);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            this.kuduTaskProxy.startWriter(lineReceiver, getTaskPluginCollector());
        }

        @Override
        public void destroy()
        {
            try {
                if (kuduTaskProxy.session != null) {
                    kuduTaskProxy.session.close();
                }
            }
            catch (Exception e) {
                LOG.warn("The \"kudu session\" was not stopped gracefully !");
            }
            KuduHelper.closeClient(kuduTaskProxy.kuduClient);
        }
    }
}
