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

package com.wgzhao.addax.plugin.reader.accessreader;

import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.rdbms.reader.CommonRdbmsReader;
import com.wgzhao.addax.rdbms.util.DataBaseType;

import java.util.List;

public class AccessReader extends Reader {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Access;

    public static class Job extends Reader.Job {

        private Configuration configuration = null;

        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init() {
            this.configuration = getPluginJobConf();
            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.configuration = this.commonRdbmsReaderJob.init(this.configuration);
        }

        @Override
        public void preCheck() {
            this.commonRdbmsReaderJob.preCheck(this.configuration, DATABASE_TYPE);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderJob.destroy(this.configuration);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return this.commonRdbmsReaderJob.split(this.configuration, adviceNumber);
        }
    }

    public static class Task extends Reader.Task {

        private Configuration configuration = null;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;

        @Override
        public void startRead(RecordSender recordSender) {

            int fetchSize = this.configuration.getInt(Key.FETCH_SIZE, Constant.DEFAULT_FETCH_SIZE);
            this.commonRdbmsReaderTask.startRead(this.configuration, recordSender, this.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void init() {
            this.configuration = getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE, getTaskGroupId(), getTaskId());
            this.commonRdbmsReaderTask.init(this.configuration);
        }

        @Override
        public void destroy() {

            this.commonRdbmsReaderTask.destroy(this.configuration);

        }
    }
}
