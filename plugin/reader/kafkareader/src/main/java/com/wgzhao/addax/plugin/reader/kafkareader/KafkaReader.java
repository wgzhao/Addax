package com.wgzhao.addax.plugin.reader.kafkareader;

import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KafkaReader extends Reader
{
    public static class Job
            extends Reader.Job
    {
        private static final Logger log = LoggerFactory.getLogger(Job.class);
        private Configuration conf = null;

        @Override
        public void init()
        {
            this.conf = getPluginJobConf();
            conf.getNecessaryValue(KafkaKey.BROKER_LIST, KafkaReaderErrorCode.REQUIRED_VALUE);
            conf.getNecessaryValue(KafkaKey.TOPIC, KafkaReaderErrorCode.REQUIRED_VALUE);
        }

        @Override
        public void destroy()
        {

        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            return null;
        }
    }

    public static class Task
            extends Reader.Task
    {
        private static final Logger log = LoggerFactory.getLogger(Task.class);

        @Override
        public void init()
        {

        }

        @Override
        public void destroy()
        {

        }

        @Override
        public void startRead(RecordSender recordSender)
        {

        }
    }
}
