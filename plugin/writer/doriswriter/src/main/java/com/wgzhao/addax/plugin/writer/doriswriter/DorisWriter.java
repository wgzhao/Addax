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

package com.wgzhao.addax.plugin.writer.doriswriter;

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;

import java.util.ArrayList;
import java.util.List;

public class DorisWriter
        extends Writer
{
    public static class Job
            extends Writer.Job
    {

        private Configuration originalConfig = null;

        private DorisWriterEmitter dorisWriterEmitter;

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();
            List<Object> connList = originalConfig.getList(Key.CONNECTION);
            Configuration conn = Configuration.from(connList.get(0).toString());
            conn.getNecessaryValue(Key.TABLE, DorisWriterErrorCode.REQUIRED_VALUE);
            conn.getNecessaryValue(Key.ENDPOINT, DorisWriterErrorCode.REQUIRED_VALUE);
            conn.getNecessaryValue(Key.DATABASE, DorisWriterErrorCode.REQUIRED_VALUE);
             this.dorisWriterEmitter = new DorisWriterEmitter(originalConfig);
        }

        @Override
        public void prepare()
        {
            //
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            List<Configuration> splitResultConfigs = new ArrayList<>();
            for (int j = 0; j < mandatoryNumber; j++) {
                splitResultConfigs.add(this.originalConfig.clone());
            }
            return splitResultConfigs;
        }

        @Override
        public void post()
        {
            //
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
        private DorisWriterTask DorisWriterTask;

        @Override
        public void init()
        {
            Configuration writerSliceConfig = getPluginJobConf();
            this.DorisWriterTask = new DorisWriterTask(writerSliceConfig);
            this.DorisWriterTask.init();
        }

        @Override
        public void prepare()
        {
            //
        }

        public void startWrite(RecordReceiver recordReceiver)
        {
            this.DorisWriterTask.startWrite(recordReceiver, getTaskPluginCollector());
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            //
        }
    }
}
