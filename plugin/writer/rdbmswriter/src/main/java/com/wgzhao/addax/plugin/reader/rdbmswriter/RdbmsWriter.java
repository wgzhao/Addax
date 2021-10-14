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

package com.wgzhao.addax.plugin.reader.rdbmswriter;

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DBUtilErrorCode;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

import static com.wgzhao.addax.common.base.Key.JDBC_DRIVER;

public class RdbmsWriter
        extends Writer
{
    private static final DataBaseType DATABASE_TYPE = DataBaseType.RDBMS;

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
                throw AddaxException.asAddaxException(DBUtilErrorCode.CONF_ERROR,
                        String.format("写入模式(writeMode)配置有误. 因为不支持配置参数项 writeMode: %s, 仅使用insert sql 插入数据. 请检查您的配置并作出修改.",
                                writeMode));
            }
            String jdbcDriver = this.originalConfig.getString(JDBC_DRIVER, null);
            if (jdbcDriver == null || StringUtils.isBlank(jdbcDriver)) {
                throw AddaxException.asAddaxException(DBUtilErrorCode.REQUIRED_VALUE, "config 'driver' is required and must not be empty");
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
        // 加载插件下面配置的驱动类
        DBUtil.loadDriverClass("writer", "rdbms");
    }
}