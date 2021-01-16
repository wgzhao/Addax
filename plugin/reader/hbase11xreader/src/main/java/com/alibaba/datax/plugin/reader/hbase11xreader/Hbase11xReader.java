package com.alibaba.datax.plugin.reader.hbase11xreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Hbase11xReader
 * Created by shf on 16/3/7.
 */
public class Hbase11xReader
        extends Reader
{
    public static class Job
            extends Reader.Job
    {
        private Configuration originConfig = null;

        @Override
        public void init()
        {
            this.originConfig = this.getPluginJobConf();
            Hbase11xHelper.validateParameter(this.originConfig);
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            return Hbase11xHelper.split(this.originConfig);
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
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private HbaseAbstractTask hbaseTaskProxy;

        @Override
        public void init()
        {
            Configuration taskConfig = super.getPluginJobConf();
            String mode = taskConfig.getString(Key.MODE);
            ModeType modeType = ModeType.getByTypeName(mode);

            switch (modeType) {
                case NORMAL:
                    this.hbaseTaskProxy = new NormalTask(taskConfig);
                    break;
                case MULTI_VERSION_FIXED_COLUMN:
                    this.hbaseTaskProxy = new MultiVersionFixedColumnTask(taskConfig);
                    break;
                default:
                    throw DataXException.asDataXException(Hbase11xReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 不支持此类模式:" + modeType);
            }
        }

        @Override
        public void prepare()
        {
            try {
                this.hbaseTaskProxy.prepare();
            }
            catch (Exception e) {
                throw DataXException.asDataXException(Hbase11xReaderErrorCode.PREPAR_READ_ERROR, e);
            }
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            com.alibaba.datax.common.element.Record record = recordSender.createRecord();
            boolean fetchOK = true;
            while (fetchOK) {
                try {
                    fetchOK = this.hbaseTaskProxy.fetchLine(record);
                }
                catch (Exception e) {
                    LOG.info("Exception", e);
                    super.getTaskPluginCollector().collectDirtyRecord(record, e);
                    record = recordSender.createRecord();
                    continue;
                }
                if (fetchOK) {
                    recordSender.sendToWriter(record);
                    record = recordSender.createRecord();
                }
            }
            recordSender.flush();
        }

        @Override
        public void post()
        {
            super.post();
        }

        @Override
        public void destroy()
        {
            if (this.hbaseTaskProxy != null) {
                this.hbaseTaskProxy.close();
            }
        }
    }
}
