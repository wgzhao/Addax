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

package com.wgzhao.addax.plugin.writer.rdbmswriter;

import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.wgzhao.addax.core.base.Key.JDBC_DRIVER;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

public class RdbmsWriter
        extends Writer
{
    private static final Logger LOG = LoggerFactory.getLogger(Writer.class);
    private static final DataBaseType DATABASE_TYPE = DataBaseType.RDBMS_WRITER;

    public static class Job
            extends Writer.Job
    {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterJob;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();

            // warn：not like mysql, only support insert mode, don't use
            String writeMode = this.originalConfig.getString(Key.WRITE_MODE);
            if (null != writeMode) {
                LOG.warn("The writeMode is not supported in RdbmsWriter, ignore it.");
            }
            String jdbcDriver = this.originalConfig.getString(JDBC_DRIVER, null);
            if (jdbcDriver == null || StringUtils.isBlank(jdbcDriver)) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "config 'driver' is required and must not be empty");
            }
            // use special jdbc driver class
            DATABASE_TYPE.setDriverClassName(jdbcDriver);
            this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(DATABASE_TYPE);
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
            this.writerSliceConfig = super.getPluginJobConf();
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
            this.commonRdbmsWriterTask.startWrite(recordReceiver, writerSliceConfig, super.getTaskPluginCollector());
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

    static {
        DBUtil.loadDriverClass("writer", "rdbms");
    }
}
