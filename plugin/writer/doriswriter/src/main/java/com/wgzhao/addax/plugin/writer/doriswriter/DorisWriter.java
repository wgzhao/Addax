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

import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.plugin.writer.doriswriter.codec.DorisCodec;
import com.wgzhao.addax.plugin.writer.doriswriter.codec.DorisCodecFactory;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;

/**
 * doris data writer
 */
public class DorisWriter
        extends Writer
{

    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig = null;
        private DorisKey options;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
            options = new DorisKey(this.originalConfig);
            options.doPretreatment();
            preCheck();
        }

        @Override
        public void preCheck()
        {
            DorisUtil.preCheckPrePareSQL(options);
            DorisUtil.preCheckPostSQL(options);
        }

        @Override
        public void prepare()
        {
            String username = options.getUsername();
            String password = options.getPassword();
            String jdbcUrl = options.getJdbcUrl();
            List<String> renderedPreSqls = DorisUtil.renderPreOrPostSqls(options.getPreSqlList(), options.getTable());
            // check username and password is valid
            Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);
            if (!renderedPreSqls.isEmpty()) {
                LOG.info("Begin to execute preSqls:[{}]. context info:{}.", String.join(";", renderedPreSqls), jdbcUrl);
                DorisUtil.executeSqls(conn, renderedPreSqls);
                DBUtil.closeDBResources(null, null, conn);
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            List<Configuration> configurations = new ArrayList<>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(originalConfig);
            }
            return configurations;
        }

        @Override
        public void post()
        {
            String username = options.getUsername();
            String password = options.getPassword();
            String jdbcUrl = options.getJdbcUrl();
            List<String> renderedPostSqls = DorisUtil.renderPreOrPostSqls(options.getPostSqlList(), options.getTable());
            if (!renderedPostSqls.isEmpty()) {
                Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);
                LOG.info("Start to execute preSqls:[{}]. context info:{}.", String.join(";", renderedPostSqls), jdbcUrl);
                DorisUtil.executeSqls(conn, renderedPostSqls);
                DBUtil.closeDBResources(null, null, conn);
            }
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
        private DorisWriterManager writerManager;
        private DorisKey options;
        private DorisCodec rowCodec;

        @Override
        public void init()
        {
            options = new DorisKey(super.getPluginJobConf());
            writerManager = new DorisWriterManager(options);
            rowCodec = DorisCodecFactory.createCodec(options);
        }

        public void startWrite(RecordReceiver recordReceiver)
        {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                if (record.getColumnNumber() != options.getColumns().size()) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR,
                            String.format(
                                    "The number of columns(%d) in the record does not match the number of columns(%d) in the configuration",
                                    record.getColumnNumber(),
                                    options.getColumns().size()));
                }
                writerManager.writeRecord(rowCodec.codec(record));
            }
        }

        @Override
        public void destroy()
        {
            writerManager.close();
        }
    }
}
