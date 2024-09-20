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

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.storage.reader.StorageReaderUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static com.wgzhao.addax.common.exception.CommonErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.exception.CommonErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.exception.CommonErrorCode.REQUIRED_VALUE;

public class FtpReader
        extends Reader
{
    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;

        private List<String> path = null;

        private HashSet<String> sourceFiles;

        // ftp链接参数
        private String protocol;
        private String host;
        private int port;
        private String username;
        private String password;
        private int timeout;
        private String connectPattern;
        private int maxTraversalLevel;

        private static final String DEFAULT_PRIVATE_KEY = "~/.ssh/id_rsa";

        private FtpHelper ftpHelper = null;

        @Override
        public void init()
        {
            this.originConfig = this.getPluginJobConf();
            this.sourceFiles = new HashSet<>();

            this.validateParameter();
            StorageReaderUtil.validateParameter(this.originConfig);
            String keyPath = this.originConfig.getString(FtpKey.KEY_PATH, null);
            String keyPass = this.originConfig.getString(FtpKey.KEY_PASS, null);

            if ("sftp".equals(protocol)) {
                //sftp协议
                this.port = originConfig.getInt(FtpKey.PORT, FtpConstant.DEFAULT_SFTP_PORT);
                this.ftpHelper = new SftpHelper();
            }
            else if ("ftp".equals(protocol)) {
                // ftp 协议
                this.port = originConfig.getInt(FtpKey.PORT, FtpConstant.DEFAULT_FTP_PORT);
                this.ftpHelper = new StandardFtpHelper();
            }
            ftpHelper.loginFtpServer(host, username, password, port, keyPath, keyPass, timeout, connectPattern);
        }

        private void validateParameter()
        {
            this.protocol = this.originConfig.getNecessaryValue(FtpKey.PROTOCOL, REQUIRED_VALUE).toLowerCase();
            boolean protocolTag = "ftp".equals(this.protocol) || "sftp".equals(this.protocol);
            if (!protocolTag) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("仅支持 ftp和sftp 传输协议 , 不支持您配置的传输协议: [%s]", protocol));
            }
            this.host = this.originConfig.getNecessaryValue(FtpKey.HOST, REQUIRED_VALUE);
            this.username = this.originConfig.getNecessaryValue(FtpKey.USERNAME, REQUIRED_VALUE);
            this.password = this.originConfig.getString(FtpKey.PASSWORD, null);
            this.timeout = originConfig.getInt(FtpKey.TIME_OUT, FtpConstant.DEFAULT_TIMEOUT);
            this.maxTraversalLevel = originConfig.getInt(FtpKey.MAX_TRAVERSAL_LEVEL, FtpConstant.DEFAULT_MAX_TRAVERSAL_LEVEL);

            // only support connect pattern
            this.connectPattern = this.originConfig.getUnnecessaryValue(FtpKey.CONNECT_PATTERN, FtpConstant.DEFAULT_FTP_CONNECT_PATTERN);
            boolean connectPatternTag = "PORT".equals(connectPattern) || "PASV".equals(connectPattern);
            if (!connectPatternTag) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("不支持您配置的ftp传输模式: [%s]", connectPattern));
            }
            else {
                this.originConfig.set(FtpKey.CONNECT_PATTERN, connectPattern);
            }

            //path check
            String pathInString = this.originConfig.getNecessaryValue(Key.PATH, REQUIRED_VALUE);
            if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
                path = new ArrayList<>();
                path.add(pathInString);
            }
            else {
                path = this.originConfig.getList(Key.PATH, String.class);
                if (null == path || path.isEmpty()) {
                    throw AddaxException.asAddaxException(REQUIRED_VALUE, "您需要指定待读取的源目录或文件");
                }
                for (String eachPath : path) {
                    if (!eachPath.startsWith("/")) {
                        String message = String.format("请检查参数path:[%s],需要配置为绝对路径", eachPath);
                        LOG.error(message);
                        throw AddaxException.asAddaxException(ILLEGAL_VALUE, message);
                    }
                }
            }
            if ("sftp".equals(protocol)) {
                // use ssh private key or not ?
                boolean useKey = this.originConfig.getBool(FtpKey.USE_KEY, false);
                if (useKey) {
                    String privateKey = this.originConfig.getString(FtpKey.KEY_PATH, DEFAULT_PRIVATE_KEY);
                    // check privateKey does exist or not
                    if (privateKey.startsWith("~")) {
                        // expand home directory
                        privateKey = privateKey.replaceFirst("^~", System.getProperty("user.home"));
                        // does it exist?
                        boolean isFile = new File(privateKey).isFile();
                        if (isFile) {
                            this.originConfig.set(FtpKey.KEY_PATH, privateKey);
                        }
                        else {
                            String msg = "You have configured to use the key, but neither the configured key file nor the default file(" +
                                    DEFAULT_PRIVATE_KEY + " exists";
                            throw AddaxException.asAddaxException(ILLEGAL_VALUE, msg);
                        }
                    }
                }
            }
        }

        @Override
        public void prepare()
        {
            LOG.debug("prepare() begin...");

            this.sourceFiles = (HashSet<String>) ftpHelper.getAllFiles(path, 0, maxTraversalLevel);

            LOG.info("您即将读取的文件数为: [{}]", this.sourceFiles.size());
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            try {
                this.ftpHelper.logoutFtpServer();
            }
            catch (Exception e) {
                String message = String.format(
                        "关闭与ftp服务器连接失败: [%s] host=%s, username=%s, port=%s",
                        e.getMessage(), host, username, port);
                LOG.error(message, e);
            }
        }

        // warn: 如果源目录为空会报错，拖空目录意图=>空文件显示指定此意图
        @Override
        public List<Configuration> split(int adviceNumber)
        {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<>();

            // warn:每个slice拖且仅拖一个文件,
            // int splitNumber = adviceNumber;
            int splitNumber = this.sourceFiles.size();
            if (0 == splitNumber) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        String.format("未能找到待读取的文件,请确认您的配置项path: %s", this.originConfig.getString(Key.PATH)));
            }

            List<List<String>> splitedSourceFiles = this.splitSourceFiles(new ArrayList(this.sourceFiles), splitNumber);
            for (List<String> files : splitedSourceFiles) {
                Configuration splitedConfig = this.originConfig.clone();
                splitedConfig.set(FtpKey.SOURCE_FILES, files);
                readerSplitConfigs.add(splitedConfig);
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }

        private <T> List<List<T>> splitSourceFiles(final List<T> sourceList, int adviceNumber)
        {
            List<List<T>> splitedList = new ArrayList<>();
            int averageLength = sourceList.size() / adviceNumber;
            averageLength = averageLength == 0 ? 1 : averageLength;

            for (int begin = 0, end; begin < sourceList.size(); begin = end) {
                end = begin + averageLength;
                if (end > sourceList.size()) {
                    end = sourceList.size();
                }
                splitedList.add(sourceList.subList(begin, end));
            }
            return splitedList;
        }
    }

    public static class Task
            extends Reader.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private String host;
        private int port;
        private String username;
        private String connectPattern;

        private Configuration readerSliceConfig;
        private List<String> sourceFiles;

        private FtpHelper ftpHelper = null;

        @Override
        public void init()
        {//连接重试
            /* for ftp connection */
            this.readerSliceConfig = this.getPluginJobConf();
            this.host = readerSliceConfig.getString(FtpKey.HOST);
            String protocol = readerSliceConfig.getString(FtpKey.PROTOCOL);
            this.username = readerSliceConfig.getString(FtpKey.USERNAME);
            String password = readerSliceConfig.getString(FtpKey.PASSWORD);
            int timeout = readerSliceConfig.getInt(FtpKey.TIME_OUT, FtpConstant.DEFAULT_TIMEOUT);
            String keyPath = readerSliceConfig.getString(FtpKey.KEY_PATH, null);
            String keyPass = readerSliceConfig.getString(FtpKey.KEY_PASS, null);
            this.sourceFiles = this.readerSliceConfig.getList(FtpKey.SOURCE_FILES, String.class);

            if ("sftp".equals(protocol)) {
                //sftp协议
                this.port = readerSliceConfig.getInt(FtpKey.PORT, FtpConstant.DEFAULT_SFTP_PORT);
                this.ftpHelper = new SftpHelper();
            }
            else if ("ftp".equals(protocol)) {
                // ftp 协议
                this.port = readerSliceConfig.getInt(FtpKey.PORT, FtpConstant.DEFAULT_FTP_PORT);
                this.connectPattern = readerSliceConfig.getString(FtpKey.CONNECT_PATTERN, FtpConstant.DEFAULT_FTP_CONNECT_PATTERN);// 默认为被动模式
                this.ftpHelper = new StandardFtpHelper();
            }
            ftpHelper.loginFtpServer(host, username, password, port, keyPath, keyPass, timeout, connectPattern);
        }

        @Override
        public void prepare()
        {

        }

        @Override
        public void post()
        {

        }

        @Override
        public void destroy()
        {
            try {
                this.ftpHelper.logoutFtpServer();
            }
            catch (Exception e) {
                String message = String.format(
                        "关闭与ftp服务器连接失败: [%s] host=%s, username=%s, port=%s",
                        e.getMessage(), host, username, port);
                LOG.error(message, e);
            }
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            LOG.debug("start read source files...");
            for (String fileName : this.sourceFiles) {
                LOG.info(String.format("reading file : [%s]", fileName));
                InputStream inputStream;

                inputStream = ftpHelper.getInputStream(fileName);

                StorageReaderUtil.readFromStream(inputStream, fileName, this.readerSliceConfig,
                        recordSender, this.getTaskPluginCollector());
                recordSender.flush();
            }

            LOG.debug("end read source files...");
        }
    }
}
