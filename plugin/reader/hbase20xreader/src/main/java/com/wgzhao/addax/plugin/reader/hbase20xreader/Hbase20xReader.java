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

package com.wgzhao.addax.plugin.reader.hbase20xreader;

import com.wgzhao.addax.core.base.HBaseKey;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

/**
 * Hbase11xReader
 * Created by shf on 16/3/7.
 */
public class Hbase20xReader
        extends Reader
{
    public static class Job
            extends Reader.Job
    {
        private Configuration originConfig = null;

        @Override
        public void init()
        {
            this.originConfig = this.getPluginJobConf();
            Hbase20xHelper.validateParameter(this.originConfig);
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            return Hbase20xHelper.split(this.originConfig);
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
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private HbaseAbstractTask hbaseTaskProxy;

        @Override
        public void init()
        {
            Configuration taskConfig = super.getPluginJobConf();
            String mode = taskConfig.getString(HBaseKey.MODE);
            ModeType modeType = ModeType.getByTypeName(mode);

            switch (modeType) {
                case NORMAL:
                    this.hbaseTaskProxy = new NormalTask(taskConfig);
                    break;
                case MULTI_VERSION_FIXED_COLUMN:
                    this.hbaseTaskProxy = new MultiVersionFixedColumnTask(taskConfig);
                    break;
                default:
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The mode '" + modeType + "' is not supported");
            }
        }

        @Override
        public void prepare()
        {
            try {
                this.hbaseTaskProxy.prepare();
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
            }
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            Record record = recordSender.createRecord();
            boolean fetchOK;
            while (true) {
                try {
                    fetchOK = this.hbaseTaskProxy.fetchLine(record);
                }
                catch (Exception e) {
                    LOG.info("Exception", e);
                    super.getTaskPluginCollector().collectDirtyRecord(record, e);
                    record = recordSender.createRecord();
                    continue;
                }
                if (fetchOK) {
                    recordSender.sendToWriter(record);
                    record = recordSender.createRecord();
                }
                else {
                    break;
                }
            }
            recordSender.flush();
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
