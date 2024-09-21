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

package com.wgzhao.addax.plugin.writer.hbase11xwriter;

import com.wgzhao.addax.common.base.HBaseKey;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;

import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.exception.ErrorCode.ILLEGAL_VALUE;

/**
 * Hbase11xWriter
 * Created by shf on 16/3/17.
 */
public class Hbase11xWriter
        extends Writer
{
    public static class Job
            extends Writer.Job
    {
        private Configuration originConfig = null;

        @Override
        public void init()
        {
            this.originConfig = this.getPluginJobConf();
            Hbase11xHelper.validateParameter(this.originConfig);
        }

        @Override
        public void prepare()
        {
            boolean truncate = originConfig.getBool(HBaseKey.TRUNCATE, false);
            if (truncate) {
                Hbase11xHelper.truncateTable(this.originConfig);
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            List<Configuration> splitResultConfigs = new ArrayList<>();
            for (int j = 0; j < mandatoryNumber; j++) {
                splitResultConfigs.add(originConfig.clone());
            }
            return splitResultConfigs;
        }

        @Override
        public void destroy()
        {
            //
        }
    }

    public static class Task
            extends Writer.Task
    {
        private HbaseAbstractTask hbaseTaskProxy;

        @Override
        public void init()
        {
            Configuration taskConfig = super.getPluginJobConf();
            String mode = taskConfig.getString(HBaseKey.MODE);
            ModeType modeType = ModeType.getByTypeName(mode);

            if (modeType == ModeType.NORMAL) {
                this.hbaseTaskProxy = new NormalTask(taskConfig);
            }
            else {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The mode " + modeType + "is unsupported");
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            this.hbaseTaskProxy.startWriter(lineReceiver, super.getTaskPluginCollector());
        }

        @Override
        public void destroy()
        {
            if (this.hbaseTaskProxy != null) {
                this.hbaseTaskProxy.close();
            }
        }
    }
}
