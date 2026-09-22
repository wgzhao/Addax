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

import com.wgzhao.addax.core.element.BytesColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.element.TimestampColumn;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.rdbms.reader.CommonRdbmsReader;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import static com.wgzhao.addax.core.base.Constant.DEFAULT_FETCH_SIZE;
import static com.wgzhao.addax.core.base.Key.FETCH_SIZE;
import static com.wgzhao.addax.core.base.Key.HAVE_KERBEROS;
import static com.wgzhao.addax.core.base.Key.KERBEROS_KEYTAB_FILE_PATH;
import static com.wgzhao.addax.core.base.Key.KERBEROS_PRINCIPAL;
import static com.wgzhao.addax.core.base.Key.SPLIT_PK;

/** Hive Reader. */
public class HiveReader
        extends Reader
{

    private static final DataBaseType DATABASE_TYPE = DataBaseType.Hive;

    /** Job. */
    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init()
        {
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
        public void preCheck()
        {
            this.commonRdbmsReaderJob.preCheck(originalConfig, DATABASE_TYPE);
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            // A split key means range queries. Hive has no indexes, so every range reads the whole
            // table (and plans a new job for it) instead of seeking into one, which is the opposite
            // of what the option buys on a row store. Say so once instead of silently multiplying
            // the scan cost by the channel count.
            if (StringUtils.isNotBlank(originalConfig.getString(SPLIT_PK, ""))) {
                LOG.warn("splitPk is set for a Hive table: every channel runs its own range query, "
                        + "and Hive scans the whole table for each one. Leave splitPk empty and let "
                        + "HiveServer2 parallelize the single query, or slice the data with 'where' "
                        + "instead.");
            }
            return this.commonRdbmsReaderJob.split(originalConfig, adviceNumber);
        }

        @Override
        public void post()
        {
            this.commonRdbmsReaderJob.post(originalConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsReaderJob.destroy(originalConfig);
        }

        private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath, org.apache.hadoop.conf.Configuration hadoopConf)
        {
            if (StringUtils.isNotBlank(kerberosPrincipal) && StringUtils.isNotBlank(kerberosKeytabFilePath)) {
                UserGroupInformation.setConfiguration(hadoopConf);
                try {
                    UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /** Task. */
    public static class Task
            extends Reader.Task
    {

        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;

        @Override
        public void init()
        {
            this.readerSliceConfig = getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE, getTaskGroupId(), getTaskId())
            {

                @Override
                protected Column createColumn(ResultSet rs, ResultSetMetaData metaData, int i)
                        throws SQLException, UnsupportedEncodingException
                {
                    // columnType() reads the type from the per-ResultSet cache: Hive resolves it by
                    // lower-casing and matching the type name, which is too much work per cell
                    switch (columnType(metaData, i)) {
                        case Types.TIMESTAMP:
                            // HiveBaseResultSet#getTimestamp(int, Calendar) throws "Method not supported"
                            return new TimestampColumn(rs.getTimestamp(i));
                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.BLOB:
                        case Types.LONGVARBINARY: {
                            // HiveBaseResultSet#getBytes throws "Method not supported", and rebuilding the
                            // bytes out of getString would round trip them through the platform charset.
                            // getObject hands back the byte[] the server sent.
                            Object value = rs.getObject(i);
                            if (value == null) {
                                return new BytesColumn((byte[]) null);
                            }
                            return new BytesColumn(value instanceof byte[] bytes
                                    ? bytes : value.toString().getBytes(StandardCharsets.UTF_8));
                        }
                        case Types.ARRAY:
                        case Types.STRUCT:
                        case Types.JAVA_OBJECT:
                            // HiveBaseResultSet#getArray throws "Method not supported"; ARRAY, MAP and
                            // STRUCT all arrive as the text form the server serialized them to
                            return new StringColumn(rs.getString(i));
                        default:
                            return super.createColumn(rs, metaData, i);
                    }
                }
            };

            commonRdbmsReaderTask.init(this.readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            int fetchSize = readerSliceConfig.getInt(FETCH_SIZE, DEFAULT_FETCH_SIZE);
            commonRdbmsReaderTask.startRead(readerSliceConfig, recordSender, getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post()
        {
            this.commonRdbmsReaderTask.post(readerSliceConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsReaderTask.destroy(readerSliceConfig);
        }
    }
}
