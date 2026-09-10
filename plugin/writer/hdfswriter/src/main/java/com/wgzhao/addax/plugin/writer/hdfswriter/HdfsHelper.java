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
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.wgzhao.addax.core.base.Key.HAVE_KERBEROS;
import static com.wgzhao.addax.core.base.Key.HDFS_SITE_PATH;
import static com.wgzhao.addax.core.base.Key.KERBEROS_KEYTAB_FILE_PATH;
import static com.wgzhao.addax.core.base.Key.KERBEROS_PRINCIPAL;
import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.LOGIN_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

/** Hdfs Helper. */
public class HdfsHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(HdfsHelper.class);

    protected FileSystem fileSystem = null;
    protected JobConf conf = null;
    protected org.apache.hadoop.conf.Configuration hadoopConf = null;
    private static final double DEFAULT_BLOOM_FILTER_FPP = 0.05d;

    // Column rendering has to agree with Column.asString(), which formats in this same zone,
    // otherwise a TIME value is written one offset away from every other representation of it
    private static final String COLUMN_TIME_ZONE = "common.column.timeZone";
    private static final String DEFAULT_COLUMN_TIME_ZONE = "GMT+8";
    private static final long MILLIS_PER_DAY = 86_400_000L;
    private static final long NANOS_PER_MILLISECOND = 1_000_000L;

    /** The zone used to render DATE/TIME columns. */
    protected TimeZone columnTimeZone = TimeZone.getTimeZone(DEFAULT_COLUMN_TIME_ZONE);

    record BloomFilterConfig(String columns, double fpp)
    {
    }

    protected void getFileSystem(Configuration taskConfig)
    {
        hadoopConf = new org.apache.hadoop.conf.Configuration();
        String defaultFS = taskConfig.getString(Key.DEFAULT_FS);
        this.columnTimeZone = TimeZone.getTimeZone(
                taskConfig.getString(COLUMN_TIME_ZONE, DEFAULT_COLUMN_TIME_ZONE));
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

    /** Hdfsdirlist. */
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

    /** Checks whether the pathexists condition holds. */
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

    /** Checks whether the pathdir condition holds. */
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

    /**
     * Delete one file, failing loudly when the filesystem reports that it could not.
     *
     * @param path the file to delete
     * @throws IOException if the deletion was refused
     */
    private void remove(Path path, boolean recursive)
            throws IOException
    {
        if (!fileSystem.delete(path, recursive)) {
            throw new IOException(String.format("Failed to delete [%s]", path));
        }
    }

    /** Deletefilesfromdir. */
    public void deleteFilesFromDir(Path dir, boolean skipTrash)
    {
        try {
            final Trash trash;
            if (skipTrash) {
                trash = null;
            }
            else {
                if (hadoopConf.getInt(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, 0) == 0) {
                    hadoopConf.set(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, "10080"); // 7 days
                }
                trash = new Trash(hadoopConf);
            }

            // listStatus rather than listFiles(recursive=false): an overwrite has to clear the
            // sub-directories of a partition tree too, and a non-recursive file listing left them
            // and their contents behind next to the newly written files
            for (FileStatus entry : fileSystem.listStatus(dir)) {
                Path entryPath = entry.getPath();

                // hidden entries are this plugin's own staging directory, and the readers skip
                // them as well, so they are not part of the data being replaced
                if (entryPath.getName().startsWith(".")) {
                    continue;
                }

                if (trash == null) {
                    LOG.info("Delete the [{}]", entryPath);
                    remove(entryPath, entry.isDirectory());
                }
                else {
                    LOG.info("Move the [{}] to Trash", entryPath);
                    // the call reports failure by returning false rather than by throwing, and the
                    // caller writes the new files right afterwards, so an unchecked false would
                    // silently leave the previous run's data in place
                    if (!trash.moveToTrash(entryPath)) {
                        throw AddaxException.asAddaxException(IO_ERROR,
                                String.format("Failed to move [%s] to Trash", entryPath));
                    }
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

    /** Deletedir. */
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

    /** Createpath. */
    public boolean createPath(String path)
    {
        try {
            return fileSystem.mkdirs(new Path(path));
        }
        catch (IOException e) {
            String message = String.format("Network IO exception occurred while mkdir [%s]", path);
            LOG.error(message);
            throw AddaxException.asAddaxException(IO_ERROR, e);
        }
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
                    Path dest = new Path(targetDir, file.getPath().getName());
                    // rename() reports several failures by returning false instead of throwing on
                    // HDFS, but on the local and other FileSystem implementations it silently
                    // replaces an existing name, and the caller deletes the staging directory
                    // right afterwards either way
                    if (fileSystem.exists(dest)) {
                        throw AddaxException.asAddaxException(IO_ERROR, String.format(
                                "Refusing to move [%s]: the destination [%s] already exists.",
                                file.getPath(), dest));
                    }
                    LOG.info("Begin to move the file [{}] to [{}].", file.getPath(), dest);
                    if (!fileSystem.rename(file.getPath(), dest)) {
                        throw AddaxException.asAddaxException(IO_ERROR,
                                String.format("Failed to move the file [%s] to [%s]", file.getPath(), dest));
                    }
                }
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR, e);
        }
        LOG.info("Finish moving file(s).");
    }

    /** Closefilesystem. */
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

    /**
     * The codecs {@link #getCompressCodec} can instantiate, so that validation and execution
     * consult one list instead of two that have to be kept in sync by hand.
     * <p>
     * LZO is absent because writing it needs the hadoop-lzo native library, and ZSTD because
     * hadoop's {@code ZStandardCodec} is built on native libhadoop rather than the zstd-jni jar:
     * both fail at task runtime with "native library not available" if they are let through.
     */
    private static final Map<String, Class<? extends CompressionCodec>> COMPRESS_CODECS = Map.of(
            "GZIP", org.apache.hadoop.io.compress.GzipCodec.class,
            "BZIP2", org.apache.hadoop.io.compress.BZip2Codec.class,
            "SNAPPY", org.apache.hadoop.io.compress.SnappyCodec.class,
            "LZ4", org.apache.hadoop.io.compress.Lz4Codec.class,
            "DEFLATE", org.apache.hadoop.io.compress.DeflateCodec.class,
            "ZLIB", org.apache.hadoop.io.compress.DeflateCodec.class);

    /** The names of the codecs the text writer can emit. */
    public static Set<String> compressCodecNames()
    {
        return COMPRESS_CODECS.keySet();
    }

    public Class<? extends CompressionCodec> getCompressCodec(String compress)
    {
        String normalized = compress.trim().toUpperCase(Locale.ROOT);
        Class<? extends CompressionCodec> codecClass = COMPRESS_CODECS.get(normalized);
        if (codecClass == null) {
            throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                    String.format("The compress mode [%s] is unsupported yet.", normalized));
        }
        return codecClass;
    }

    /** Checks whether the pathwritable condition holds. */
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

    /** Validatebloomfilterconfiguration. */
    public void validateBloomFilterConfiguration(Configuration config, List<Configuration> columns)
    {
        resolveBloomFilterConfiguration(config, columns);
    }

    /** Resolvebloomfilterconfiguration. */
    public BloomFilterConfig resolveBloomFilterConfiguration(Configuration config, List<Configuration> columns)
    {
        List<String> bloomFilterColumns = config.getList(Key.BLOOM_FILTER_COLUMNS, String.class);
        if (bloomFilterColumns == null || bloomFilterColumns.isEmpty()) {
            return null;
        }

        Set<String> availableColumns = columns.stream()
                .map(column -> column.getString(Key.NAME).trim())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        List<String> normalizedBloomColumns = new ArrayList<>();
        for (String bloomColumn : bloomFilterColumns) {
            if (bloomColumn == null || bloomColumn.trim().isEmpty()) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The item [bloomColumns] contains empty column name.");
            }

            String normalizedColumn = bloomColumn.trim();
            if (!availableColumns.contains(normalizedColumn)) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("The item [bloomColumns] contains unknown column [%s].", normalizedColumn));
            }

            if (!normalizedBloomColumns.contains(normalizedColumn)) {
                normalizedBloomColumns.add(normalizedColumn);
            }
        }

        if (normalizedBloomColumns.isEmpty()) {
            return null;
        }

        double fpp = config.getDouble(Key.BLOOM_FILTER_FPP, DEFAULT_BLOOM_FILTER_FPP);
        if (fpp <= 0.0d || fpp >= 1.0d) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    String.format("The item [bloomFpp] must be between 0 and 1, but got [%s].", fpp));
        }

        return new BloomFilterConfig(String.join(",", normalizedBloomColumns), fpp);
    }

    /**
     * Format a TIME column to string with full nanosecond precision.
     * <p>
     * When the column is a DateColumn with TIME subtype and has non-zero nanos,
     * produces ISO-8601 format like {@code HH:mm:ss.SSSSSS} with trailing zeros trimmed.
     * When nanos is zero, delegates to the default {@code column.asString()} ({@code HH:mm:ss}).
     *
     * @param column the input column to format
     * @param timeZone the zone the time-of-day is expressed in
     * @return formatted time string with full precision when applicable
     */
    protected static String formatTimeWithNanos(Column column, TimeZone timeZone)
    {
        if (column.getType() == Column.Type.DATE && ((DateColumn) column).getSubType() == DateColumn.DateType.TIME) {
            long nanos = ((DateColumn) column).getNanos();
            if (nanos > 0) {
                long timeMs = (Long) column.getRawData();
                // The raw value is an instant, not a wall clock reading: apply the same zone the
                // default asString() path uses, and floorMod so pre-1970 values stay in range
                long millisOfDay = Math.floorMod(timeMs + timeZone.getOffset(timeMs), MILLIS_PER_DAY);
                long totalNanosOfDay = millisOfDay * NANOS_PER_MILLISECOND + nanos % NANOS_PER_MILLISECOND;
                return LocalTime.ofNanoOfDay(totalNanosOfDay).toString();
            }
        }
        return column.asString();
    }
}
