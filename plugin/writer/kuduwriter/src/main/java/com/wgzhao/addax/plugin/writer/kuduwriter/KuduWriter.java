/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.plugin.writer.kuduwriter;

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
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
                throw AddaxException.asAddaxException(
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
                throw AddaxException.asAddaxException(KuduWriterErrorCode.TABLE_NOT_EXISTS,
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
