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
import org.apache.kudu.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;

public class KuduWriter
        extends Writer
{
    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private static final String INSERT_MODE = "upsert";

        private static final int DEFAULT_TIME_OUT = 100;

        private Configuration config = null;

        @Override
        public void init()
        {
            this.config = this.getPluginJobConf();
            this.validateParameter();
        }

        private void validateParameter()
        {
            String tableName = config.getNecessaryValue(KuduKey.TABLE, REQUIRED_VALUE);
            String masterAddress = config.getNecessaryValue(KuduKey.KUDU_MASTER_ADDRESSES, REQUIRED_VALUE);
            long timeout = config.getInt(KuduKey.KUDU_TIMEOUT, DEFAULT_TIME_OUT) * 1000L;
            // write back default value with ms unit
            this.config.set(KuduKey.KUDU_TIMEOUT, timeout);

            LOG.info("Try to connect kudu with {}", masterAddress);
            KuduHelper kuduHelper = new KuduHelper(masterAddress, timeout);
            // check table exists or not
            if (!kuduHelper.isTableExists(tableName)) {
                throw AddaxException.asAddaxException(CONFIG_ERROR, "table '" + tableName + "' does not exists");
            }

            // column check
            List<String> columns = this.config.getList(KuduKey.COLUMN, String.class);
            if (null == columns || columns.isEmpty()) {
                throw AddaxException.asAddaxException(
                        REQUIRED_VALUE, "the configuration 'column' must be specified"
                );
            }

            if (columns.size() == 1 && "*".equals(columns.get(0))) {
                // get all columns
                LOG.info("Take the columns of table '{}' as writing columns", tableName);
                columns = kuduHelper.getAllColumns(tableName);
                this.config.set(KuduKey.COLUMN, columns);
            }
            else {
                // check column exists or not
                final Schema schema = kuduHelper.getSchema(tableName);
                for (String column : columns) {
                    if (schema.getColumn(column) == null) {
                        throw AddaxException.asAddaxException(CONFIG_ERROR, "column '" + column + "' does not exists");
                    }
                }
            }
            // writeMode check
            String writeMode = this.config.getString(KuduKey.WRITE_MODE, INSERT_MODE);
            this.config.set(KuduKey.WRITE_MODE, writeMode);
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
        private KuduWriterTask kuduTaskProxy;

        @Override
        public void init()
        {
            Configuration taskConfig = getPluginJobConf();
            this.kuduTaskProxy = new KuduWriterTask(taskConfig);
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
                LOG.warn("The kudu session was not closed gracefully !");
            }
            kuduTaskProxy.close();
        }
    }
}
