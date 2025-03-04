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

package com.wgzhao.addax.plugin.writer.sybasewriter;

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;

import java.util.List;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;

public class SybaseWriter
        extends Writer
{
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Sybase;

    public static class Job
            extends Writer.Job
    {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterJob;

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();

            String writeMode = this.originalConfig.getString(Key.WRITE_MODE);
            if (null != writeMode) {
                if (!"insert".equalsIgnoreCase(writeMode) && !writeMode.startsWith("update")) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR, "The write mode " + writeMode + " is not supported by Sybase writer.");
                }
            }

            this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterJob.init(this.originalConfig);
        }

        @Override
        public void prepare()
        {
            this.commonRdbmsWriterJob.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            return this.commonRdbmsWriterJob.split(this.originalConfig, mandatoryNumber);
        }

        @Override
        public void post()
        {
            this.commonRdbmsWriterJob.post(this.originalConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsWriterJob.destroy(this.originalConfig);
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

            this.commonRdbmsWriterTask.init(this.writerSliceConfig);
        }

        @Override
        public void prepare()
        {
            this.commonRdbmsWriterTask.prepare(this.writerSliceConfig);
        }

        public void startWrite(RecordReceiver recordReceiver)
        {
            this.commonRdbmsWriterTask.startWrite(recordReceiver,
                    this.writerSliceConfig, getTaskPluginCollector());
        }

        @Override
        public void post()
        {
            this.commonRdbmsWriterTask.post(this.writerSliceConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsWriterTask.destroy(this.writerSliceConfig);
        }
    }
}
