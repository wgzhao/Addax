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
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import static com.wgzhao.addax.core.base.Key.HAVE_KERBEROS;
import static com.wgzhao.addax.core.base.Key.HDFS_SITE_PATH;
import static com.wgzhao.addax.core.base.Key.KERBEROS_KEYTAB_FILE_PATH;
import static com.wgzhao.addax.core.base.Key.KERBEROS_PRINCIPAL;
import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.LOGIN_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

public class HdfsHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(HdfsHelper.class);

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

        if (taskConfig.getString(HDFS_SITE_PATH, null) !=null) {
            hadoopConf.addResource(new Path(taskConfig.getString(HDFS_SITE_PATH)));
        }

        hadoopConf.set("fs.defaultFS", defaultFS);

        //是否有Kerberos认证
        boolean haveKerberos = taskConfig.getBool(HAVE_KERBEROS, false);
        if (haveKerberos) {
            String kerberosKeytabFilePath = taskConfig.getString(KERBEROS_KEYTAB_FILE_PATH);
            String kerberosPrincipal = taskConfig.getString(KERBEROS_PRINCIPAL);
            hadoopConf.set("hadoop.security.authentication", "kerberos");
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
            throw AddaxException.asAddaxException(IO_ERROR, e);
        }
        catch (Exception e) {
            String message = String.format("Failed to obtain Filesystem with defaultFS: [%s]", defaultFS);
            LOG.error(message);
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
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
                throw AddaxException.asAddaxException(LOGIN_ERROR, e);
            }
        }
    }

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
            throw AddaxException.asAddaxException(IO_ERROR, e);
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
            throw AddaxException.asAddaxException(IO_ERROR, e);
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
            throw AddaxException.asAddaxException(IO_ERROR, e);
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
                    LOG.info("Delete the file [{}]", next.getPath());
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
                    LOG.info("Move the file [{}] to Trash", next.getPath());
                    trash.moveToTrash(next.getPath());
                }
            }
        }
        catch (FileNotFoundException fileNotFoundException) {
            throw new AddaxException(CONFIG_ERROR, fileNotFoundException.getMessage());
        }
        catch (IOException ioException) {
            throw new AddaxException(IO_ERROR, ioException.getMessage());
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
        catch (IOException e) {
            LOG.error("IO exception occurred while delete temporary directory [{}].", path);
            throw AddaxException.asAddaxException(IO_ERROR, e);
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
                    LOG.info("Begin to move the file [{}] to [{}].", file.getPath(), targetDir);
                    fileSystem.rename(file.getPath(), new Path(targetDir, file.getPath().getName()));
                }
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR, e);
        }
        LOG.info("Finish moving file(s).");
    }

    public void closeFileSystem()
    {
        try {
            fileSystem.close();
        }
        catch (IOException e) {
            LOG.error("IO exception occurred while closing Filesystem.");
            throw AddaxException.asAddaxException(IO_ERROR, e);
        }
    }

    public Class<? extends CompressionCodec> getCompressCodec(String compress)
    {
        compress = compress.toUpperCase();
        Class<? extends CompressionCodec> codecClass = switch (compress) {
            case "GZIP" -> org.apache.hadoop.io.compress.GzipCodec.class;
            case "BZIP2" -> org.apache.hadoop.io.compress.BZip2Codec.class;
            case "SNAPPY" -> org.apache.hadoop.io.compress.SnappyCodec.class;
            case "LZ4" -> org.apache.hadoop.io.compress.Lz4Codec.class;
            case "ZSTD" -> org.apache.hadoop.io.compress.ZStandardCodec.class;
            case "DEFLATE", "ZLIB" -> org.apache.hadoop.io.compress.DeflateCodec.class;
            default -> throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                    String.format("The compress mode [%s} is unsupported yet.", compress));
        };
        return codecClass;
    }

    public boolean isPathWritable(String path) {
        try {
            Path p = new Path(path);
            Path tempFile = new Path(p, "._write_test_" + System.currentTimeMillis());
            fileSystem.create(tempFile, true).close();
            fileSystem.delete(tempFile, false);
            return true;
        } catch (IOException | IllegalArgumentException e) {
            return false;
        }
    }
}
