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

package com.wgzhao.datax.plugin.reader.hbase20xsqlreader;

import com.wgzhao.datax.common.plugin.RecordSender;
import com.wgzhao.datax.common.spi.Reader;
import com.wgzhao.datax.common.util.Configuration;

import java.util.List;

public class HBase20xSQLReader
        extends Reader
{

    public static class Job
            extends Reader.Job
    {
        private Configuration originalConfig;
        private HBase20SQLReaderHelper readerHelper;

        @Override
        public void init()
        {
            this.originalConfig = this.getPluginJobConf();
            this.readerHelper = new HBase20SQLReaderHelper(this.originalConfig);
            readerHelper.validateParameter();
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            return readerHelper.doSplit(adviceNumber);
        }

        @Override
        public void destroy()
        {
            // do nothing
        }
    }

    public static class Task
            extends Reader.Task
    {
        private Configuration readerConfig;
        private HBase20xSQLReaderTask hbase20xSQLReaderTask;

        @Override
        public void init()
        {
            this.readerConfig = super.getPluginJobConf();
            hbase20xSQLReaderTask = new HBase20xSQLReaderTask(readerConfig, super.getTaskGroupId(), super.getTaskId());
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            hbase20xSQLReaderTask.readRecord(recordSender);
        }

        @Override
        public void destroy()
        {
            // do nothing
        }
    }
}
