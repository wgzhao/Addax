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

package com.wgzhao.datax.plugin.reader.tdenginereader;

import com.wgzhao.datax.common.plugin.RecordSender;
import com.wgzhao.datax.common.spi.Reader;
import com.wgzhao.datax.common.util.Configuration;
import com.wgzhao.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.wgzhao.datax.plugin.rdbms.reader.Constant;
import com.wgzhao.datax.plugin.rdbms.util.DBUtil;
import com.wgzhao.datax.plugin.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class TDengineReader
        extends Reader
{

    private static final DataBaseType DATABASE_TYPE = DataBaseType.TDengine;

    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();

            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.commonRdbmsReaderJob.init(this.originalConfig);
        }

        @Override
        public void preCheck()
        {
            init();
            this.commonRdbmsReaderJob.preCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            return this.commonRdbmsReaderJob.split(this.originalConfig, adviceNumber);
        }

        @Override
        public void post()
        {
            this.commonRdbmsReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsReaderJob.destroy(this.originalConfig);
        }
    }

    public static class Task
            extends Reader.Task
    {

        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;

        @Override
        public void init()
        {
            this.readerSliceConfig = getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE, getTaskGroupId(), getTaskId());
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            // TDengine does not support fetch size
            int fetchSize = this.readerSliceConfig.getInt(Constant.FETCH_SIZE);
            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender, getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post()
        {
            this.commonRdbmsReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsReaderTask.destroy(this.readerSliceConfig);
        }
    }
}
