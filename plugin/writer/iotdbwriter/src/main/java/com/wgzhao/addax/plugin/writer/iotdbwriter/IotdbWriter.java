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

package com.wgzhao.addax.plugin.writer.iotdbwriter;

import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.iotdb.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;

/** Apache IoTDB writer. */
public class IotdbWriter
        extends Writer
{
    /** Job. */
    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig;
        private IotdbWriterConfig config;

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();
            this.config = IotdbWriterConfig.from(this.originalConfig);
        }

        @Override
        public void prepare()
        {
            if (this.config.tree()) {
                prepareTree();
            }
            else {
                prepareTable();
            }
        }

        private void prepareTree()
        {
            try (Session session = IotdbTreeWriter.openSession(this.config)) {
                for (String sql : this.config.preSqls()) {
                    String rendered = IotdbWriterUtil.renderSql(sql, this.config.target());
                    if (rendered != null) {
                        LOG.info("Executing preSql: {}", rendered);
                        session.executeNonQueryStatement(rendered);
                    }
                }
            }
            catch (AddaxException e) {
                throw e;
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL,
                        "Failed to connect to IoTDB or execute preSql for device " + this.config.device(), e);
            }
        }

        private void prepareTable()
        {
            IotdbWriterUtil.loadDriver();
            try (Connection connection = DriverManager.getConnection(
                    this.config.jdbcUrl(), this.config.username(), this.config.password());
                    Statement statement = connection.createStatement()) {
                try (ResultSet ignored = statement.executeQuery("SELECT 1")) {
                    LOG.debug("Verified that the target supports the IoTDB table model.");
                }
                for (String sql : this.config.preSqls()) {
                    String rendered = IotdbWriterUtil.renderSql(sql, this.config.target());
                    if (rendered != null) {
                        LOG.info("Executing preSql: {}", rendered);
                        statement.execute(rendered);
                    }
                }
            }
            catch (SQLException e) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "The IoTDB table model requires a 2.x server and a reachable jdbcUrl ("
                                + e.getMessage() + "). Check the jdbcUrl, credentials, server version and preSql.", e);
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            List<Configuration> slices = new ArrayList<>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                slices.add(this.originalConfig.clone());
            }
            return slices;
        }

        @Override
        public void post()
        {
            if (this.config.postSqls().isEmpty()) {
                return;
            }
            if (this.config.tree()) {
                postTree();
            }
            else {
                postTable();
            }
        }

        private void postTree()
        {
            try (Session session = IotdbTreeWriter.openSession(this.config)) {
                for (String sql : this.config.postSqls()) {
                    String rendered = IotdbWriterUtil.renderSql(sql, this.config.target());
                    if (rendered != null) {
                        LOG.info("Executing postSql: {}", rendered);
                        session.executeNonQueryStatement(rendered);
                    }
                }
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL,
                        "Failed to execute postSql for device " + this.config.device(), e);
            }
        }

        private void postTable()
        {
            IotdbWriterUtil.loadDriver();
            try (Connection connection = DriverManager.getConnection(
                    this.config.jdbcUrl(), this.config.username(), this.config.password());
                    Statement statement = connection.createStatement()) {
                for (String sql : this.config.postSqls()) {
                    String rendered = IotdbWriterUtil.renderSql(sql, this.config.target());
                    if (rendered != null) {
                        LOG.info("Executing postSql: {}", rendered);
                        statement.execute(rendered);
                    }
                }
            }
            catch (SQLException e) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL,
                        "Failed to execute postSql for table " + this.config.table(), e);
            }
        }

        @Override
        public void destroy()
        {
            // no job-level resources
        }
    }

    /** Task. */
    public static class Task
            extends Writer.Task
    {
        private IotdbWriterConfig config;

        @Override
        public void init()
        {
            this.config = IotdbWriterConfig.from(getPluginJobConf());
            if (!this.config.tree()) {
                IotdbWriterUtil.loadDriver();
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            if (this.config.tree()) {
                new IotdbTreeWriter(this.config).write(lineReceiver, getTaskPluginCollector());
            }
            else {
                new IotdbTableWriter(this.config).write(lineReceiver, getTaskPluginCollector());
            }
        }

        @Override
        public boolean supportFailOver()
        {
            // IoTDB rows are keyed by device/tags + time, so replaying a failed task is idempotent
            return true;
        }

        @Override
        public void post()
        {
            // resources are closed inside startWrite
        }

        @Override
        public void destroy()
        {
            // resources are closed inside startWrite
        }
    }
}
