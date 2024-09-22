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

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.storage.reader.StorageReaderUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.RUNTIME_ERROR;
import static org.apache.commons.net.ftp.FTP.BINARY_FILE_TYPE;

public class StandardFtpHelper
        extends FtpHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(StandardFtpHelper.class);
    FTPClient ftpClient = null;
    HashSet<String> sourceFiles = new HashSet<>();

    @Override
    public void loginFtpServer(String host, String username, String password, int port, String keyPath, String keyPass, int timeout,
            String connectMode)
    {
        ftpClient = new FTPClient();
        try {
            // 连接
            ftpClient.connect(host, port);
            // 登录
            ftpClient.login(username, password);
            // 不需要写死ftp server的OS TYPE,FTPClient getSystemType()方法会自动识别
            // ftpClient.configure(new FTPClientConfig(FTPClientConfig.SYS_UNIX))
            ftpClient.setConnectTimeout(timeout);
            ftpClient.setDataTimeout(timeout);
            if ("PASV".equals(connectMode)) {
                ftpClient.enterRemotePassiveMode();
                ftpClient.enterLocalPassiveMode();
            }
            else if ("PORT".equals(connectMode)) {
                ftpClient.enterLocalActiveMode();
                // ftpClient.enterRemoteActiveMode(host, port)
            }
            int reply = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftpClient.disconnect();
                String message = String.format("与ftp服务器建立连接失败,请检查用户名和密码是否正确: [%s]",
                        "message:host =" + host + ",username = " + username + ",port =" + port);
                LOG.error(message);
                throw AddaxException.asAddaxException(CONNECT_ERROR, message);
            }
            //设置命令传输编码
            String fileEncoding = System.getProperty("file.encoding");
            ftpClient.setControlEncoding(fileEncoding);
            // always use binary transfer model
            ftpClient.setFileType(BINARY_FILE_TYPE);
        }
        catch (UnknownHostException e) {
            String message = String.format("请确认ftp服务器地址是否正确，无法连接到地址为: [%s] 的ftp服务器", host);
            LOG.error(message);
            throw AddaxException.asAddaxException(CONNECT_ERROR, message, e);
        }
        catch (IllegalArgumentException e) {
            String message = String.format("请确认连接ftp服务器端口是否正确，错误的端口: [%s] ", port);
            LOG.error(message);
            throw AddaxException.asAddaxException(CONNECT_ERROR, message, e);
        }
        catch (Exception e) {
            String message = String.format("与ftp服务器建立连接失败 : [%s]",
                    "message:host =" + host + ",username = " + username + ",port =" + port);
            LOG.error(message);
            throw AddaxException.asAddaxException(CONNECT_ERROR, message, e);
        }
    }

    @Override
    public void logoutFtpServer()
    {
        if (ftpClient.isConnected()) {
            try {
                // ftpClient.completePendingCommand();//打开流操作之后必须，原因还需要深究
                ftpClient.logout();
            }
            catch (IOException e) {
                String message = "与ftp服务器断开连接失败";
                LOG.error(message);
                throw AddaxException.asAddaxException(CONNECT_ERROR, message, e);
            }
        }
    }

    @Override
    public boolean isDirectory(String directoryPath)
    {
        try {
            return ftpClient.changeWorkingDirectory(new String(directoryPath.getBytes(), StandardCharsets.ISO_8859_1));
        }
        catch (IOException e) {
            LOG.error("Failed to check whether the directory exists", e);
            return false;
        }
    }

    @Override
    public boolean isFile(String filePath)
    {
        boolean isExitFlag = false;
        try {
            FTPFile[] ftpFiles = ftpClient.listFiles(new String(filePath.getBytes(), StandardCharsets.ISO_8859_1));
            if (ftpFiles.length == 1 && ftpFiles[0].isFile()) {
                isExitFlag = true;
            }
        }
        catch (IOException e) {
            String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", filePath);
            LOG.error(message);
            throw AddaxException.asAddaxException(IO_ERROR, message, e);
        }
        return isExitFlag;
    }

    @Override
    public boolean isSymbolicLink(String filePath)
    {
        boolean isExitFlag = false;
        try {
            FTPFile[] ftpFiles = ftpClient.listFiles(new String(filePath.getBytes(), StandardCharsets.ISO_8859_1));
            if (ftpFiles.length == 1 && ftpFiles[0].isSymbolicLink()) {
                isExitFlag = true;
            }
        }
        catch (IOException e) {
            String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", filePath);
            LOG.error(message);
            throw AddaxException.asAddaxException(IO_ERROR, message, e);
        }
        return isExitFlag;
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
            FTPFile[] fs = ftpClient.listFiles(directoryPath);
            for (FTPFile ff : fs) {
                if (ff.isFile()) {
                    // 是文件
                    sourceFiles.add(parentDir + IOUtils.DIR_SEPARATOR + ff.getName());
                }
            } // end for vector
        }
        catch (IOException e) {
            LOG.error("Failed to retrieve file(s) from {}", directoryPath, e);
        }
        return sourceFiles;
    }

    @Override
    public InputStream getInputStream(String filePath)
    {
        try {
            return ftpClient.retrieveFileStream(new String(filePath.getBytes(), StandardCharsets.ISO_8859_1));
        }
        catch (IOException e) {
            String message = String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", filePath, filePath);
            LOG.error(message);
            throw AddaxException.asAddaxException(IO_ERROR, message);
        }
    }
}
