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

package com.wgzhao.addax.plugin.reader.ftpreader;

import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.storage.reader.StorageReaderUtil;
import com.wgzhao.addax.storage.util.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.wgzhao.addax.core.base.Key.SOURCE_FILES;
import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.PERMISSION_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;
import static com.wgzhao.addax.plugin.reader.ftpreader.FtpConstant.DEFAULT_FTP_CONNECT_PATTERN;
import static com.wgzhao.addax.plugin.reader.ftpreader.FtpConstant.DEFAULT_MAX_TRAVERSAL_LEVEL;
import static com.wgzhao.addax.plugin.reader.ftpreader.FtpKey.CONNECT_PATTERN;
import static com.wgzhao.addax.plugin.reader.ftpreader.FtpKey.KEY_PATH;
import static com.wgzhao.addax.plugin.reader.ftpreader.FtpKey.MAX_TRAVERSAL_LEVEL;
import static com.wgzhao.addax.plugin.reader.ftpreader.FtpKey.PROTOCOL;
import static com.wgzhao.addax.plugin.reader.ftpreader.FtpKey.USE_KEY;

/** Ftp Reader. */
public class FtpReader
        extends Reader
{
    /** Job. */
    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;

        private List<String> path = null;

        private Set<String> sourceFiles;

        private int maxTraversalLevel;

        private static final String DEFAULT_PRIVATE_KEY = "~/.ssh/id_rsa";

        private FtpHelper ftpHelper = null;

        @Override
        public void init()
        {
            this.originConfig = this.getPluginJobConf();
            this.sourceFiles = new HashSet<>();

            FtpProtocol protocol = this.validateParameter();
            StorageReaderUtil.validateParameter(this.originConfig);
            this.ftpHelper = protocol.connect(FtpConnection.from(this.originConfig, protocol));
        }

        private FtpProtocol validateParameter()
        {
            FtpProtocol protocol = FtpProtocol.of(this.originConfig.getNecessaryValue(PROTOCOL, REQUIRED_VALUE));
            this.maxTraversalLevel = originConfig.getInt(MAX_TRAVERSAL_LEVEL, DEFAULT_MAX_TRAVERSAL_LEVEL);

            //path check
            String pathInString = this.originConfig.getNecessaryValue(Key.PATH, REQUIRED_VALUE);
            if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
                path = new ArrayList<>();
                path.add(pathInString);
            }
            else {
                path = this.originConfig.getList(Key.PATH, String.class);
                if (null == path || path.isEmpty()) {
                    throw AddaxException.asAddaxException(REQUIRED_VALUE, "the path is required");
                }
            }

            // A relative path is resolved against the session's working directory, which the ftp
            // helper moves while probing whether a path is a directory, so the same path can be
            // listed and then read from two different places. Only absolute paths are unambiguous.
            for (String eachPath : path) {
                if (!eachPath.startsWith("/")) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "The path must be an absolute path, but the configured path is " + eachPath);
                }
            }

            if (protocol == FtpProtocol.FTP) {
                String connectPattern = this.originConfig.getUnnecessaryValue(CONNECT_PATTERN, DEFAULT_FTP_CONNECT_PATTERN);
                if (!"PORT".equals(connectPattern) && !"PASV".equals(connectPattern)) {
                    throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                            "Only PORT and PASV are accepted, the " + connectPattern + " is not supported.");
                }
                this.originConfig.set(CONNECT_PATTERN, connectPattern);
            }
            else if (originConfig.getBool(USE_KEY, false)) {
                String privateKey = originConfig.getString(KEY_PATH, DEFAULT_PRIVATE_KEY)
                        .replaceFirst("^~", System.getProperty("user.home"));
                // check privateKey does exist or not
                File file = new File(privateKey);
                if (!file.isFile()) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR,
                            "The private ssh key " + privateKey + " does not exist.");
                }
                else if (!file.canRead()) {
                    throw AddaxException.asAddaxException(PERMISSION_ERROR,
                            "The private ssh key " + privateKey + " is not readable.");
                }
                this.originConfig.set(KEY_PATH, privateKey);
            }

            return protocol;
        }

        @Override
        public void prepare()
        {
            LOG.debug("prepare() begin...");

            this.sourceFiles = ftpHelper.getAllFiles(path, 0, maxTraversalLevel);
            if (sourceFiles.isEmpty()) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "Cannot find any file in path: " + path + ", check that the path(s) exist and are readable");
            }
            LOG.info("{} file(s) to be read", this.sourceFiles.size());
        }

        @Override
        public void destroy()
        {
            try {
                this.ftpHelper.logoutFtpServer();
            }
            catch (Exception e) {
                LOG.error("Failed to logout (s)Ftp Server", e);
            }
        }

        // warn: 如果源目录为空会报错，拖空目录意图=>空文件显示指定此意图
        @Override
        public List<Configuration> split(int adviceNumber)
        {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<>();

            int splitNumber = Math.min(sourceFiles.size(), adviceNumber);
            List<List<String>> splitSourceFiles = FileHelper.splitSourceFiles(new ArrayList<>(sourceFiles), splitNumber);
            for (List<String> files : splitSourceFiles) {
                Configuration splitConfig = this.originConfig.clone();
                splitConfig.set(SOURCE_FILES, files);
                readerSplitConfigs.add(splitConfig);
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }
    }

    /** Task. */
    public static class Task
            extends Reader.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;
        private List<String> sourceFiles;

        private FtpHelper ftpHelper = null;

        @Override
        public void init()
        {
            this.readerSliceConfig = getPluginJobConf();
            this.sourceFiles = readerSliceConfig.getList(SOURCE_FILES, String.class);
            FtpProtocol protocol = FtpProtocol.of(readerSliceConfig.getNecessaryValue(PROTOCOL, REQUIRED_VALUE));
            this.ftpHelper = protocol.connect(FtpConnection.from(this.readerSliceConfig, protocol));
        }

        @Override
        public void destroy()
        {
            try {
                this.ftpHelper.logoutFtpServer();
            }
            catch (Exception e) {
                LOG.error("Failed to close connection", e);
            }
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            LOG.debug("start read source files...");
            InputStream inputStream;
            for (String fileName : sourceFiles) {
                LOG.info("reading file : {}", fileName);
                inputStream = ftpHelper.getInputStream(fileName);
                StorageReaderUtil.readFromStream(inputStream, fileName, readerSliceConfig,
                        recordSender, getTaskPluginCollector());
                recordSender.flush();
            }
            LOG.debug("end read source files...");
        }
    }
}
