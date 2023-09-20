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

package com.wgzhao.addax.plugin.reader.hivereader;

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.TimestampColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.reader.CommonRdbmsReader;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import static com.wgzhao.addax.common.base.Constant.DEFAULT_FETCH_SIZE;
import static com.wgzhao.addax.common.base.Key.FETCH_SIZE;
import static com.wgzhao.addax.common.base.Key.HAVE_KERBEROS;
import static com.wgzhao.addax.common.base.Key.KERBEROS_KEYTAB_FILE_PATH;
import static com.wgzhao.addax.common.base.Key.KERBEROS_PRINCIPAL;

public class HiveReader
        extends Reader {

    private static final DataBaseType DATABASE_TYPE = DataBaseType.Hive;

    public static class Job
            extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init() {
            this.originalConfig = getPluginJobConf();

            boolean haveKerberos = originalConfig.getBool(HAVE_KERBEROS, false);
            if (haveKerberos) {
                LOG.info("Try to login Hadoop via kerberos");
                org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
                String kerberosKeytabFilePath = originalConfig.getString(KERBEROS_KEYTAB_FILE_PATH);
                String kerberosPrincipal = originalConfig.getString(KERBEROS_PRINCIPAL);
                hadoopConf.set("hadoop.security.authentication", "kerberos");
                kerberosAuthentication(kerberosPrincipal, kerberosKeytabFilePath, hadoopConf);
            }
            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.originalConfig = commonRdbmsReaderJob.init(originalConfig);
        }

        @Override
        public void preCheck() {
            this.commonRdbmsReaderJob.preCheck(originalConfig, DATABASE_TYPE);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return this.commonRdbmsReaderJob.split(originalConfig, adviceNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderJob.post(originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderJob.destroy(originalConfig);
        }

        private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath, org.apache.hadoop.conf.Configuration hadoopConf) {
            if (StringUtils.isNotBlank(kerberosPrincipal) && StringUtils.isNotBlank(kerberosKeytabFilePath)) {
                UserGroupInformation.setConfiguration(hadoopConf);
                try {
                    UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
                } catch (Exception e) {
                    String message = String.format("Auth failure with kerberos, Please check " +
                                    "kerberosKeytabFilePath[%s] and kerberosPrincipal[%s]",
                            kerberosKeytabFilePath, kerberosPrincipal);
                    throw AddaxException.asAddaxException(HiveReaderErrorCode.KERBEROS_LOGIN_ERROR, message, e);
                }
            }
        }
    }

    public static class Task
            extends Reader.Task {

        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;

        @Override
        public void init() {
            this.readerSliceConfig = getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE, getTaskGroupId(), getTaskId()) {

                @Override
                protected Column createColumn(ResultSet rs, ResultSetMetaData metaData, int i)
                        throws SQLException, UnsupportedEncodingException {
                    if (metaData.getColumnType(i) == Types.TIMESTAMP ) {
                        // hive HiveBaseResultSet#getTimestamp(String columnName, Calendar cal) not support
                        return new TimestampColumn(rs.getTimestamp(i));
                    }
                    return super.createColumn(rs, metaData, i);
                }

            };

            this.commonRdbmsReaderTask.init(this.readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = this.readerSliceConfig.getInt(FETCH_SIZE, DEFAULT_FETCH_SIZE);

            this.commonRdbmsReaderTask.startRead(readerSliceConfig, recordSender, getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderTask.post(readerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderTask.destroy(readerSliceConfig);
        }
    }
}
