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

package com.wgzhao.addax.plugin.reader.hbase11xsqlreader;

import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HbaseSQLReader
        extends Reader
{
    public static class Job
            extends Reader.Job
    {
        private HbaseSQLReaderConfig readerConfig;

        @Override
        public void init()
        {
            readerConfig = HbaseSQLHelper.parseConfig(this.getPluginJobConf());
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            return HbaseSQLHelper.split(readerConfig);
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
        private static Logger log = LoggerFactory.getLogger(Task.class);
        private HbaseSQLReaderTask hbase11SQLReaderTask;

        @Override
        public void init()
        {
            hbase11SQLReaderTask = new HbaseSQLReaderTask(this.getPluginJobConf());
            this.hbase11SQLReaderTask.init();
        }

        @Override
        public void prepare()
        {
            hbase11SQLReaderTask.prepare();
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            long recordNum = 0L;
            Record record = recordSender.createRecord();
            boolean fetchOK = true;
            while (fetchOK) {
                try {
                    fetchOK = this.hbase11SQLReaderTask.readRecord(record);
                }
                catch (Exception e) {
                    log.info("Read record exception", e);
                    e.printStackTrace();
                    super.getTaskPluginCollector().collectDirtyRecord(record, e);
                    record = recordSender.createRecord();
                    continue;
                }
                if (fetchOK) {
                    recordSender.sendToWriter(record);
                    recordNum++;
                    if (recordNum % 10000 == 0) {
                        log.info("already read record num is {}", recordNum);
                    }
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
            this.hbase11SQLReaderTask.destroy();
        }
    }
}
