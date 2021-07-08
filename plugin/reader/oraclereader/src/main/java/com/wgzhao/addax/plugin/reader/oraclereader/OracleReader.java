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

package com.wgzhao.addax.plugin.reader.oraclereader;

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.plugin.rdbms.reader.CommonRdbmsReader;
import com.wgzhao.addax.plugin.rdbms.reader.Key;
import com.wgzhao.addax.plugin.rdbms.reader.util.HintUtil;
import com.wgzhao.addax.plugin.rdbms.util.DBUtilErrorCode;
import com.wgzhao.addax.plugin.rdbms.util.DataBaseType;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

import static com.wgzhao.addax.plugin.rdbms.reader.Constant.FETCH_SIZE;
import static com.wgzhao.addax.plugin.rdbms.reader.Constant.IS_TABLE_MODE;

public class OracleReader
        extends Reader
{

    private static final DataBaseType DATABASE_TYPE = DataBaseType.Oracle;

    public static class Job
            extends Reader.Job
    {
        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();

            dealFetchSize(this.originalConfig);

            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(
                    DATABASE_TYPE);
            this.originalConfig = this.commonRdbmsReaderJob.init(this.originalConfig);

            // 注意：要在 this.commonRdbmsReaderJob.init(this.originalConfig); 之后执行，这样可以直接快速判断是否是querySql 模式
            dealHint(this.originalConfig);
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
            return this.commonRdbmsReaderJob.split(this.originalConfig,
                    adviceNumber);
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

        private void dealFetchSize(Configuration originalConfig)
        {
            int fetchSize = originalConfig.getInt(
                    FETCH_SIZE,
                    Constant.DEFAULT_FETCH_SIZE);
            if (fetchSize < 1) {
                throw AddaxException
                        .asAddaxException(DBUtilErrorCode.REQUIRED_VALUE,
                                String.format("您配置的 fetchSize 有误，fetchSize:[%d] 值不能小于 1.",
                                        fetchSize));
            }
            originalConfig.set(
                    FETCH_SIZE,
                    fetchSize);
        }

        private void dealHint(Configuration originalConfig)
        {
            String hint = originalConfig.getString(Key.HINT);
            if (StringUtils.isNotBlank(hint)) {
                boolean isTableMode = originalConfig.getBool(IS_TABLE_MODE);
                if (!isTableMode) {
                    throw AddaxException.asAddaxException(OracleReaderErrorCode.HINT_ERROR, "当且仅当非 querySql 模式读取 oracle 时才能配置 HINT.");
                }
                HintUtil.initHintConf(DATABASE_TYPE, originalConfig);
            }
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
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(
                    DATABASE_TYPE, getTaskGroupId(), getTaskId());
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            int fetchSize = this.readerSliceConfig.getInt(FETCH_SIZE);

            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig,
                    recordSender, getTaskPluginCollector(), fetchSize);
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
