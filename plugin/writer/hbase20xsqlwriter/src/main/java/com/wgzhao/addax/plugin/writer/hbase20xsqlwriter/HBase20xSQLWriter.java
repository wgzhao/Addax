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

package com.wgzhao.addax.plugin.writer.hbase20xsqlwriter;

import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;

import java.util.ArrayList;
import java.util.List;

public class HBase20xSQLWriter
        extends Writer
{

    public static class Job
            extends Writer.Job
    {

        private Configuration config = null;

        @Override
        public void init()
        {
            this.config = this.getPluginJobConf();
            HBase20xSQLHelper.validateParameter(this.config);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            List<Configuration> splitResultConfigs = new ArrayList<>();
            for (int j = 0; j < mandatoryNumber; j++) {
                splitResultConfigs.add(config.clone());
            }
            return splitResultConfigs;
        }

        @Override
        public void destroy()
        {
            //doNothing
        }
    }

    public static class Task
            extends Writer.Task
    {
        private HBase20xSQLWriterTask writerTask;

        @Override
        public void init()
        {
            Configuration taskConfig = super.getPluginJobConf();
            this.writerTask = new HBase20xSQLWriterTask(taskConfig);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            this.writerTask.startWriter(lineReceiver, super.getTaskPluginCollector());
        }

        @Override
        public void destroy()
        {
            // 不需要close
        }
    }
}