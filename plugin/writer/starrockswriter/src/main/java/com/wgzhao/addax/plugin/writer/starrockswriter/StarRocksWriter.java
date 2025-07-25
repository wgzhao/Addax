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

package com.wgzhao.addax.plugin.writer.starrockswriter;

import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.plugin.writer.starrockswriter.manager.StarRocksWriterManager;
import com.wgzhao.addax.plugin.writer.starrockswriter.row.StarRocksISerializer;
import com.wgzhao.addax.plugin.writer.starrockswriter.row.StarRocksSerializerFactory;
import com.wgzhao.addax.plugin.writer.starrockswriter.util.StarRocksWriterUtil;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;

public class StarRocksWriter
        extends Writer
{

    public static class Job
            extends Writer.Job
    {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig = null;
        private StarRocksWriterOptions options;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
            options = new StarRocksWriterOptions(super.getPluginJobConf());
            options.doPretreatment();
        }

        @Override
        public void preCheck()
        {
            this.init();
            StarRocksWriterUtil.preCheckPrePareSQL(options);
            StarRocksWriterUtil.preCheckPostSQL(options);
        }

        @Override
        public void prepare()
        {
            String username = options.getUsername();
            String password = options.getPassword();
            String jdbcUrl = options.getJdbcUrl();
            List<String> renderedPreSqls = StarRocksWriterUtil.renderPreOrPostSqls(options.getPreSqlList(), options.getTable());
            if (!renderedPreSqls.isEmpty()) {
                Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);
                LOG.info("Begin to execute preSqls:[{}]. context info:{}.", String.join(";", renderedPreSqls), jdbcUrl);
                StarRocksWriterUtil.executeSqls(conn, renderedPreSqls);
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
            List<String> renderedPostSqls = StarRocksWriterUtil.renderPreOrPostSqls(options.getPostSqlList(), options.getTable());
            if (!renderedPostSqls.isEmpty()) {
                Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);
                LOG.info("Begin to execute postSqls:[{}]. context info:{}.", String.join(";", renderedPostSqls), jdbcUrl);
                StarRocksWriterUtil.executeSqls(conn, renderedPostSqls);
                DBUtil.closeDBResources(null, null, conn);
            }
        }

        @Override
        public void destroy()
        {
        }
    }

    public static class Task
            extends Writer.Task
    {
        private StarRocksWriterManager writerManager;
        private StarRocksWriterOptions options;
        private StarRocksISerializer rowSerializer;

        @Override
        public void init()
        {
            options = new StarRocksWriterOptions(super.getPluginJobConf());
            if (options.isWildcardColumn()) {
                Connection conn = DBUtil.getConnection(DataBaseType.MySql, options.getJdbcUrl(), options.getUsername(), options.getPassword());
                List<String> columns = StarRocksWriterUtil.getStarRocksColumns(conn, options.getDatabase(), options.getTable());
                options.setInfoCchemaColumns(columns);
            }
            writerManager = new StarRocksWriterManager(options);
            rowSerializer = StarRocksSerializerFactory.createSerializer(options);
        }

        @Override
        public void prepare()
        {
        }

        public void startWrite(RecordReceiver recordReceiver)
        {
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if (record.getColumnNumber() != options.getColumns().size()) {
                        throw AddaxException
                                .asAddaxException(
                                        CONFIG_ERROR,
                                        "The record's column number must be equal to the column number in the writer configuration file");
                    }
                    writerManager.writeRecord(rowSerializer.serialize(record));
                }
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL, e);
            }
        }

        @Override
        public void post()
        {
            try {
                writerManager.close();
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL, e);
            }
        }

        @Override
        public void destroy() {}

        @Override
        public boolean supportFailOver()
        {
            return false;
        }
    }
}
