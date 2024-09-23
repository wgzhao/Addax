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

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.storage.reader.StorageReaderUtil;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.RUNTIME_ERROR;

public class SftpHelper
        extends FtpHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(SftpHelper.class);

    Session session = null;
    ChannelSftp channelSftp = null;
    HashSet<String> sourceFiles = new HashSet<>();

    @Override
    public void loginFtpServer(String host, String username, String password, int port, String keyPath, String keyPass, int timeout,
            String connectMode)
    {
        JSch jsch = new JSch(); // 创建JSch对象
        if (keyPath != null) {
            try {
                if (keyPass != null) {
                    jsch.addIdentity(keyPath, keyPass);
                }
                else {
                    jsch.addIdentity(keyPath);
                }
            }
            catch (JSchException e) {
                throw AddaxException.asAddaxException(CONFIG_ERROR, "Failed to use private key", e);
            }
        }
        try {
            session = jsch.getSession(username, host, port);
            // 根据用户名，主机ip，端口获取一个Session对象
            // 如果服务器连接不上，则抛出异常
            if (session == null) {
                throw AddaxException.asAddaxException(CONNECT_ERROR,
                        "Failed to connect server " + host + ":" + port + " with user " + username);
            }

            if (!StringUtils.isBlank(password)) {
                session.setPassword(password); // 设置密码
            }
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config); // 为Session对象设置properties
            session.setTimeout(timeout); // 设置timeout时间
            session.connect(); // 通过Session建立链接

            channelSftp = (ChannelSftp) session.openChannel("sftp"); // 打开SFTP通道
            channelSftp.connect(); // 建立SFTP通道的连接

            //设置命令传输编码
            //String fileEncoding = System.getProperty("file.encoding")
            //channelSftp.setFilenameEncoding(fileEncoding)
        }
        catch (JSchException e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR,
                    "Failed to connect server " + host + ":" + port + " with user " + username, e
            );
        }
    }

    @Override
    public void logoutFtpServer()
    {
        if (channelSftp != null) {
            channelSftp.disconnect();
        }
        if (session != null) {
            session.disconnect();
        }
    }

    private boolean isDirectory(String directoryPath)
    {
        try {
            SftpATTRS sftpATTRS = channelSftp.lstat(directoryPath);
            return sftpATTRS.isDir();
        }
        catch (SftpException e) {
            return false;
        }
    }

    @Override
    public HashSet<String> getListFiles(String directoryPath, int parentLevel, int maxTraversalLevel)
    {
        String parentDir;
        if (isDirectory(directoryPath)) {
            parentDir = directoryPath;
        } else {
            parentDir = Paths.get(directoryPath).getParent().toString();
        }
        try {
                ArrayList<Object> vector = new ArrayList<>(channelSftp.ls(directoryPath));
                for (Object o : vector) {
                    LsEntry le = (LsEntry) o;
                    // the long name format is like
                    // drwxr-xr-x    2 root     root         4096 Mar  1  2010 bin
                    String strName = le.getLongname();
                    if (strName.startsWith("-")) {
                        // 是文件
                        sourceFiles.add(parentDir + IOUtils.DIR_SEPARATOR + le.getFilename());
                    }
                } // end for vector
            }
            catch (SftpException e) {
                LOG.error("Failed to retrieve file(s) from {}", directoryPath, e);
            }
            return sourceFiles;
    }

    @Override
    public InputStream getInputStream(String filePath)
    {
        try {
            return channelSftp.get(filePath);
        }
        catch (SftpException e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR,
                    "Failed to read file: " + filePath, e);
        }
    }
}
