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

package com.wgzhao.addax.plugin.writer.hdfswriter;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;

public class HdfsHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(HdfsHelper.class);
    private static final String HADOOP_SECURITY_AUTHENTICATION_KEY = "hadoop.security.authentication";
    private static final String HDFS_DEFAULT_FS_KEY = "fs.defaultFS";

    protected FileSystem fileSystem = null;
    protected JobConf conf = null;
    protected org.apache.hadoop.conf.Configuration hadoopConf = null;

    protected void getFileSystem(Configuration taskConfig)
    {
        hadoopConf = new org.apache.hadoop.conf.Configuration();
        String defaultFS = taskConfig.getString(Key.DEFAULT_FS);
        Configuration hadoopSiteParams = taskConfig.getConfiguration(Key.HADOOP_CONFIG);
        JSONObject hadoopSiteParamsAsJsonObject = JSON.parseObject(taskConfig.getString(Key.HADOOP_CONFIG));
        if (null != hadoopSiteParams) {
            Set<String> paramKeys = hadoopSiteParams.getKeys();
            for (String each : paramKeys) {
                hadoopConf.set(each, hadoopSiteParamsAsJsonObject.getString(each));
            }
        }
        hadoopConf.set(HDFS_DEFAULT_FS_KEY, defaultFS);

        //是否有Kerberos认证
        boolean haveKerberos = taskConfig.getBool(Key.HAVE_KERBEROS, false);
        if (haveKerberos) {
            String kerberosKeytabFilePath = taskConfig.getString(Key.KERBEROS_KEYTAB_FILE_PATH);
            String kerberosPrincipal = taskConfig.getString(Key.KERBEROS_PRINCIPAL);
            hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION_KEY, "kerberos");
            // fix Failed to specify server's Kerberos principal name
            if (Objects.equals(hadoopConf.get("dfs.namenode.kerberos.principal", ""), "")) {
                // get REALM
                String serverPrincipal = "nn/_HOST@" + Iterables.get(Splitter.on('@').split(kerberosPrincipal), 1);
                hadoopConf.set("dfs.namenode.kerberos.principal", serverPrincipal);
            }
            kerberosAuthentication(kerberosPrincipal, kerberosKeytabFilePath);
        }

        conf = new JobConf(hadoopConf);
        try {
            this.fileSystem = FileSystem.get(conf);
        }
        catch (IOException e) {
            String message = String.format("Network IO exception occurred while obtaining Filesystem with defaultFS: [%s]",
                    defaultFS);
            LOG.error(message);
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        catch (Exception e) {
            String message = String.format("Failed to obtain Filesystem with defaultFS: [%s]", defaultFS);
            LOG.error(message);
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
    }

    private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath)
    {
        if (StringUtils.isNotBlank(kerberosPrincipal) && StringUtils.isNotBlank(kerberosKeytabFilePath)) {
            UserGroupInformation.setConfiguration(hadoopConf);
            try {
                UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
            }
            catch (Exception e) {
                String message = String.format("kerberos authentication failed, keytab file: [%s], principal: [%s]",
                        kerberosKeytabFilePath, kerberosPrincipal);
                LOG.error(message);
                throw AddaxException.asAddaxException(HdfsWriterErrorCode.KERBEROS_LOGIN_ERROR, e);
            }
        }
    }

    /**
     * 获取指定目录下的文件列表
     *
     * @param dir 需要搜索的目录
     * @return 文件数组，文件是全路径，
     * eg：hdfs://10.101.204.12:9000/user/hive/warehouse/writer.db/text/test.txt
     */
    public Path[] hdfsDirList(String dir)
    {
        Path path = new Path(dir);
        Path[] files;
        try {
            FileStatus[] status = fileSystem.listStatus(path);
            files = new Path[status.length];
            for (int i = 0; i < status.length; i++) {
                files[i] = status[i].getPath();
            }
        }
        catch (IOException e) {
            String message = String.format("Network IO exception occurred while fetching file list for directory [%s]", dir);
            LOG.error(message);
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return files;
    }

    public boolean isPathExists(String filePath)
    {
        Path path = new Path(filePath);
        boolean exist;
        try {
            exist = fileSystem.exists(path);
        }
        catch (IOException e) {
            LOG.error("Network IO exception occurred while checking if file path [{}] exists", filePath);
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return exist;
    }

    public boolean isPathDir(String filePath)
    {
        Path path = new Path(filePath);
        boolean isDir;
        try {
            isDir = fileSystem.getFileStatus(path).isDirectory();
        }
        catch (IOException e) {
            LOG.error("Network IO exception occurred while checking if path [{}] is directory or not.", filePath);
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return isDir;
    }

    public void deleteFilesFromDir(Path dir, boolean skipTrash)
    {
        try {
            final RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(dir, false);
            if (skipTrash) {
                while (files.hasNext()) {
                    final LocatedFileStatus next = files.next();
                    LOG.info("Delete file [{}]", next.getPath());
                    fileSystem.delete(next.getPath(), false);
                }
            }
            else {
                if (hadoopConf.getInt(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, 0) == 0) {
                    hadoopConf.set(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, "10080"); // 7 days
                }
                final Trash trash = new Trash(hadoopConf);
                while (files.hasNext()) {
                    final LocatedFileStatus next = files.next();
                    LOG.info("Move file [{}] to Trash", next.getPath());
                    trash.moveToTrash(next.getPath());
                }
            }
        }
        catch (FileNotFoundException fileNotFoundException) {
            throw new AddaxException(HdfsWriterErrorCode.FILE_NOT_FOUND, fileNotFoundException.getMessage());
        }
        catch (IOException ioException) {
            throw new AddaxException(HdfsWriterErrorCode.IO_ERROR, ioException.getMessage());
        }
    }

    public void deleteDir(Path path)
    {
        LOG.info("Begin to delete temporary dir [{}] .", path);
        try {
            if (isPathExists(path.toString())) {
                fileSystem.delete(path, true);
            }
        }
        catch (Exception e) {
            LOG.error("IO exception occurred while delete temporary directory [{}].", path);
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        LOG.info("Finish deleting temporary dir [{}] .", path);
    }

    /**
     * move all files in sourceDir to targetDir
     *
     * @param sourceDir the source directory
     * @param targetDir the target directory
     */
    public void moveFilesToDest(Path sourceDir, Path targetDir)
    {
        try {
            final FileStatus[] fileStatuses = fileSystem.listStatus(sourceDir);
            for (FileStatus file : fileStatuses) {
                if (file.isFile() && file.getLen() > 0) {
                    LOG.info("Begin to move file from [{}] to [{}].", file.getPath(), targetDir.getName());
                    fileSystem.rename(file.getPath(), new Path(targetDir, file.getPath().getName()));
                }
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.IO_ERROR, e);
        }
        LOG.info("Finish move file(s).");
    }

    //关闭FileSystem
    public void closeFileSystem()
    {
        try {
            fileSystem.close();
        }
        catch (IOException e) {
            LOG.error("IO exception occurred while closing Filesystem.");
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
    }

    // compress 已经转为大写
    public Class<? extends CompressionCodec> getCompressCodec(String compress)
    {
        compress = compress.toUpperCase();
        Class<? extends CompressionCodec> codecClass;
        switch (compress) {
            case "GZIP":
                codecClass = org.apache.hadoop.io.compress.GzipCodec.class;
                break;
            case "BZIP2":
                codecClass = org.apache.hadoop.io.compress.BZip2Codec.class;
                break;
            case "SNAPPY":
                codecClass = org.apache.hadoop.io.compress.SnappyCodec.class;
                break;
            case "LZ4":
                codecClass = org.apache.hadoop.io.compress.Lz4Codec.class;
                break;
            case "ZSTD":
                codecClass = org.apache.hadoop.io.compress.ZStandardCodec.class;
                break;
            case "DEFLATE":
            case "ZLIB":
                codecClass = org.apache.hadoop.io.compress.DeflateCodec.class;
                break;
            default:
                throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("The compress mode [%s} is unsupported yet.", compress));
        }
        return codecClass;
    }
}
