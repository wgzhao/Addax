/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.wgzhao.addax.plugin.writer.tdenginewriter;

import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

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
