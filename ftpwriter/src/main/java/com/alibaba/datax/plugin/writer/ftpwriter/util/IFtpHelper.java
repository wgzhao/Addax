package com.alibaba.datax.plugin.writer.ftpwriter.util;

import java.io.OutputStream;
import java.util.Set;

public interface IFtpHelper
{

    //使用被动方式
    void loginFtpServer(String host, String username, String password, int port, int timeout);

    void logoutFtpServer();

    /**
     * warn: 不支持递归创建, 比如 mkdir -p
     */
    void mkdir(String directoryPath);

    /**
     * 支持目录递归创建
     */
    void mkDirRecursive(String directoryPath);

    OutputStream getOutputStream(String filePath);

    String getRemoteFileContent(String filePath);

    Set<String> getAllFilesInDir(String dir, String prefixFileName);

    /**
     * warn: 不支持文件夹删除, 比如 rm -rf
     */
    void deleteFiles(Set<String> filesToDelete);

    void completePendingCommand();
}
