package com.wgzhao.addax.plugin.writer.tdenginewriter;

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;

public class TDengineWriter
        extends Writer
{
    private static final String PEER_PLUGIN_NAME = "peerPluginName";

    public static class Job
            extends Writer.Job
    {

        private Configuration originalConfig;
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
            this.originalConfig.set(PEER_PLUGIN_NAME, getPeerPluginName());

            // check user
            String user = this.originalConfig.getString(Key.USERNAME);
            if (StringUtils.isBlank(user)) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "The parameter ["
                        + Key.USERNAME + "] is not set.");
            }

            // check password
            String password = this.originalConfig.getString(Key.PASSWORD);
            if (StringUtils.isBlank(password)) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "The parameter ["
                        + Key.PASSWORD + "] is not set.");
            }

            Configuration conn = originalConfig.getConfiguration(Key.CONNECTION);
            String jdbcUrl = conn.getString(Key.JDBC_URL);
            if (StringUtils.isBlank(jdbcUrl)) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "The parameter ["
                        + Key.JDBC_URL + "] of connection is not set.");
            }

            // check column
        }

        @Override
        public void destroy()
        {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            List<Configuration> writerSplitConfigs = new ArrayList<>();

            Configuration conf = this.originalConfig.getConfiguration(Key.CONNECTION);
            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration clone = this.originalConfig.clone();
                String jdbcUrl = conf.getString(Key.JDBC_URL);
                clone.set(Key.JDBC_URL, jdbcUrl);
                clone.set(Key.TABLE, conf.getList(Key.TABLE));
                clone.remove(Key.CONNECTION);
                writerSplitConfigs.add(clone);
            }
            return writerSplitConfigs;
        }
    }

    public static class Task
            extends Writer.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        @Override
        public void init()
        {
            this.writerSliceConfig = getPluginJobConf();
        }

        @Override
        public void destroy()
        {

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            String peerPluginName = this.writerSliceConfig.getString(PEER_PLUGIN_NAME);
            LOG.debug("start to handle record from: {}", peerPluginName);

            DataHandler handler;
            if (peerPluginName.equals("opentsdbreader")) {
                handler = new OpentsdbDataHandler(this.writerSliceConfig);
            }
            else {
                handler = new DefaultDataHandler(this.writerSliceConfig);
            }

            long records = handler.handle(lineReceiver, getTaskPluginCollector());
            LOG.debug("handle data finished, records: {}", records);
        }
    }
}
