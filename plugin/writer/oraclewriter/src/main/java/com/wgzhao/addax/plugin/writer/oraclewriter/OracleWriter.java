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

package com.wgzhao.addax.plugin.writer.oraclewriter;

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtilErrorCode;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;

import java.util.List;

public class OracleWriter
        extends Writer
{
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Oracle;

    public static class Job
            extends Writer.Job
    {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterJob;

        @Override
        public void preCheck()
        {
            this.init();
            this.commonRdbmsWriterJob.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();

            String writeMode = originalConfig.getString(Key.WRITE_MODE);
            if (null != writeMode) {
                if (!"insert".equalsIgnoreCase(writeMode) && !writeMode.startsWith("update")) {
                    throw AddaxException.asAddaxException(DBUtilErrorCode.CONF_ERROR,
                            String.format("写入模式(writeMode)配置错误. Oracle仅支持insert, update两种模式. %s 不支持", writeMode));
                }
            }

            commonRdbmsWriterJob = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            commonRdbmsWriterJob.init(originalConfig);
        }

        @Override
        public void prepare()
        {
            commonRdbmsWriterJob.prepare(originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            return commonRdbmsWriterJob.split(originalConfig, mandatoryNumber);
        }

        @Override
        public void post()
        {
            commonRdbmsWriterJob.post(originalConfig);
        }

        @Override
        public void destroy()
        {
            commonRdbmsWriterJob.destroy(originalConfig);
        }
    }

    public static class Task
            extends Writer.Task
    {
        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Task commonRdbmsWriterTask;

        @Override
        public void init()
        {
            this.writerSliceConfig = getPluginJobConf();
            this.commonRdbmsWriterTask = new CommonRdbmsWriter.Task(DATABASE_TYPE);
            commonRdbmsWriterTask.init(writerSliceConfig);
        }

        @Override
        public void prepare()
        {
            commonRdbmsWriterTask.prepare(writerSliceConfig);
        }

        public void startWrite(RecordReceiver recordReceiver)
        {
            commonRdbmsWriterTask.startWrite(recordReceiver, writerSliceConfig, getTaskPluginCollector());
        }

        @Override
        public void post()
        {
            commonRdbmsWriterTask.post(writerSliceConfig);
        }

        @Override
        public void destroy()
        {
            commonRdbmsWriterTask.destroy(writerSliceConfig);
        }
    }
}
