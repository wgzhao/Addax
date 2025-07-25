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

package com.wgzhao.addax.plugin.writer.accesswriter;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.ErrorCode;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.wgzhao.addax.core.base.Key.CONNECTION;
import static com.wgzhao.addax.core.base.Key.JDBC_URL;

public class AccessWriter
        extends Writer
{

    private static final DataBaseType DATABASE_TYPE = DataBaseType.Access;

    public static class Job
            extends Writer.Job
    {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class.getName());

        private Configuration originalConfig = null;

        private CommonRdbmsWriter.Job commonRdbmsWriterJob = null;

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();
            // check the mdb file exists
            checkFile();
            this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterJob.init(this.originalConfig);
        }

        private void checkFile() {
            // get the jdbc url
            String jdbcUrl = this.originalConfig.getString(CONNECTION + "." + JDBC_URL);
            // extract mdb file path from jdbcUrl
            String mdbFilePath = jdbcUrl.substring(jdbcUrl.indexOf("jdbc:ucanaccess://") + "jdbc:ucanaccess://".length());
            if (mdbFilePath.contains(";")) {
                mdbFilePath = mdbFilePath.substring(0, mdbFilePath.indexOf(";"));
            }
            // check the mdb file exists
            File file = new File(mdbFilePath);
            if (! file.exists()) {
                LOG.warn("The mdb file({}) does not exist, creating a new one.", mdbFilePath);
                try {
                    Database tables = DatabaseBuilder.create(Database.FileFormat.V2016, file);
                    tables.close();
                }
                catch (IOException e) {
                    LOG.error("Failed to create mdb file({}).", mdbFilePath);
                    throw AddaxException.asAddaxException(ErrorCode.IO_ERROR, e);
                }
            } else {
                // is valid mdb file ?
                try {
                    Database tables = DatabaseBuilder.open(file);
                    tables.close();
                }
                catch (IOException e) {
                    LOG.error("The mdb file({}) is not a valid mdb file.", mdbFilePath);
                    throw AddaxException.asAddaxException(ErrorCode.ILLEGAL_VALUE, e);
                }
            }
        }

        @Override
        public void preCheck()
        {
            this.commonRdbmsWriterJob.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
        public void prepare()
        {
            this.commonRdbmsWriterJob.prepare(this.originalConfig);
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

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            return this.commonRdbmsWriterJob.split(this.originalConfig, mandatoryNumber);
        }
    }

    public static class Task
            extends Writer.Task
    {

        private Configuration configuration;

        private CommonRdbmsWriter.Task commonRdbmsWriterTask;

        @Override
        public void init()
        {
            this.configuration = super.getPluginJobConf();
            this.commonRdbmsWriterTask = new CommonRdbmsWriter.Task(DATABASE_TYPE);
            this.commonRdbmsWriterTask.init(this.configuration);
        }

        @Override
        public void prepare()
        {
            this.commonRdbmsWriterTask.prepare(this.configuration);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            this.commonRdbmsWriterTask.startWrite(lineReceiver, this.configuration, super.getTaskPluginCollector());
        }

        @Override
        public void post()
        {
            this.commonRdbmsWriterTask.post(configuration);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsWriterTask.destroy(configuration);
        }
    }
}
