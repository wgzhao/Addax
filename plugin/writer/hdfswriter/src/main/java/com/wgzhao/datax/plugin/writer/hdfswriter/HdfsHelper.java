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

package com.wgzhao.datax.plugin.writer.hdfswriter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.wgzhao.datax.common.element.Column;
import com.wgzhao.datax.common.element.Record;
import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.plugin.RecordReceiver;
import com.wgzhao.datax.common.plugin.TaskPluginCollector;
import com.wgzhao.datax.common.util.Configuration;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class HdfsHelper
{
    public static final Logger LOG = LoggerFactory.getLogger(HdfsHelper.class);
    public static final String HADOOP_SECURITY_AUTHENTICATION_KEY = "hadoop.security.authentication";
    public static final String HDFS_DEFAULT_FS_KEY = "fs.defaultFS";
    private FileSystem fileSystem = null;
    private JobConf conf = null;
    private org.apache.hadoop.conf.Configuration hadoopConf = null;
    // Kerberos
    private boolean haveKerberos = false;
    private String kerberosKeytabFilePath;
    private String kerberosPrincipal;
    private static final int DECIMAL_DEFAULT_PRECISION = 38;
    private static final int DECIMAL_DEFAULT_SCALE = 10;

    public static MutablePair<Text, Boolean> transportOneRecord(
            Record record, char fieldDelimiter, List<Configuration> columnsConfiguration, TaskPluginCollector taskPluginCollector)
    {
        MutablePair<List<Object>, Boolean> transportResultList = transportOneRecord(record, columnsConfiguration, taskPluginCollector);
        //保存<转换后的数据,是否是脏数据>
        MutablePair<Text, Boolean> transportResult = new MutablePair<>();
        transportResult.setRight(false);
        Text recordResult = new Text(StringUtils.join(transportResultList.getLeft(), fieldDelimiter));
        transportResult.setRight(transportResultList.getRight());
        transportResult.setLeft(recordResult);
        return transportResult;
    }

    public static MutablePair<List<Object>, Boolean> transportOneRecord(
            Record record, List<Configuration> columnsConfiguration,
            TaskPluginCollector taskPluginCollector)
    {

        MutablePair<List<Object>, Boolean> transportResult = new MutablePair<>();
        transportResult.setRight(false);
        List<Object> recordList = Lists.newArrayList();
        int recordLength = record.getColumnNumber();
        if (0 != recordLength) {
            Column column;
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                if (null != column.getRawData()) {
                    String rowData = column.getRawData().toString();
                    SupportHiveDataType columnType = SupportHiveDataType.valueOf(
                            columnsConfiguration.get(i).getString(Key.TYPE).toUpperCase());
                    //根据writer端类型配置做类型转换
                    try {
                        switch (columnType) {
                            case TINYINT:
                                recordList.add(Byte.valueOf(rowData));
                                break;
                            case SMALLINT:
                                recordList.add(Short.valueOf(rowData));
                                break;
                            case INT:
                            case INTEGER:
                                recordList.add(Integer.valueOf(rowData));
                                break;
                            case BIGINT:
                                recordList.add(column.asLong());
                                break;
                            case FLOAT:
                                recordList.add(Float.valueOf(rowData));
                                break;
                            case DOUBLE:
                                recordList.add(column.asDouble());
                                break;
                            case STRING:
                            case VARCHAR:
                            case CHAR:
                                recordList.add(column.asString());
                                break;
                            case DECIMAL:
                                recordList.add(HiveDecimal.create(column.asBigDecimal()));
                                break;
                            case BOOLEAN:
                                recordList.add(column.asBoolean());
                                break;
                            case DATE:
                                recordList.add(org.apache.hadoop.hive.common.type.Date.valueOf(column.asString()));
                                break;
                            case TIMESTAMP:
                                recordList.add(Timestamp.valueOf(column.asString()));
                                break;
                            case BINARY:
                                recordList.add(column.asBytes());
                                break;
                            default:
                                throw DataXException
                                        .asDataXException(
                                                HdfsWriterErrorCode.ILLEGAL_VALUE,
                                                String.format(
                                                        "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
                                                        columnsConfiguration.get(i).getString(Key.NAME),
                                                        columnsConfiguration.get(i).getString(Key.TYPE)));
                        }
                    }
                    catch (Exception e) {
                        // warn: 此处认为脏数据
                        e.printStackTrace();
                        String message = String.format(
                                "字段类型转换错误：你目标字段为[%s]类型，实际字段值为[%s].",
                                columnsConfiguration.get(i).getString(Key.TYPE), column.getRawData());
                        taskPluginCollector.collectDirtyRecord(record, message);
                        transportResult.setRight(true);
                        break;
                    }
                }
                else {
                    // warn: it's all ok if nullFormat is null
                    recordList.add(null);
                }
            }
        }
        transportResult.setLeft(recordList);
        return transportResult;
    }

    public static GenericRecord transportParRecord(
            Record record, List<Configuration> columnsConfiguration,
            TaskPluginCollector taskPluginCollector, GenericRecordBuilder builder)
    {

        int recordLength = record.getColumnNumber();
        if (0 != recordLength) {
            Column column;
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                String colname = columnsConfiguration.get(i).getString(Key.NAME);
                String typename = columnsConfiguration.get(i).getString(Key.TYPE);
                if (column.getRawData() == null) {
                    builder.set(colname, null);
                }
                else {
                    String rowData = column.getRawData().toString();
                    SupportHiveDataType columnType = SupportHiveDataType.valueOf(typename);
                    //根据writer端类型配置做类型转换
                    try {
                        switch (columnType) {
                            case INT:
                            case INTEGER:
                                builder.set(colname, Integer.valueOf(rowData));
                                break;
                            case LONG:
                                builder.set(colname, column.asLong());
                                break;
                            case FLOAT:
                                builder.set(colname, Float.valueOf(rowData));
                                break;
                            case DOUBLE:
                                builder.set(colname, column.asDouble());
                                break;
                            case STRING:
                                builder.set(colname, column.asString());
                                break;
                            case DECIMAL:
                                builder.set(colname, new BigDecimal(column.asString()).setScale(columnsConfiguration.get(i).getInt(Key.SCALE), BigDecimal.ROUND_HALF_UP));
                                break;
                            case BOOLEAN:
                                builder.set(colname, column.asBoolean());
                                break;
                            case BINARY:
                                builder.set(colname, column.asBytes());
                                break;
                            default:
                                throw DataXException
                                        .asDataXException(
                                                HdfsWriterErrorCode.ILLEGAL_VALUE,
                                                String.format(
                                                        "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
                                                        columnsConfiguration.get(i).getString(Key.NAME),
                                                        columnsConfiguration.get(i).getString(Key.TYPE)));
                        }
                    }
                    catch (Exception e) {
                        // warn: 此处认为脏数据
                        String message = String.format(
                                "字段类型转换错误：目标字段为[%s]类型，实际字段值为[%s].",
                                columnsConfiguration.get(i).getString(Key.TYPE), column.getRawData());
                        taskPluginCollector.collectDirtyRecord(record, message);
                        break;
                    }
                }
            }
        }
        return builder.build();
    }

    public void getFileSystem(String defaultFS, Configuration taskConfig)
    {
        hadoopConf = new org.apache.hadoop.conf.Configuration();

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
        this.haveKerberos = taskConfig.getBool(Key.HAVE_KERBEROS, false);
        if (haveKerberos) {
            this.kerberosKeytabFilePath = taskConfig.getString(Key.KERBEROS_KEYTAB_FILE_PATH);
            this.kerberosPrincipal = taskConfig.getString(Key.KERBEROS_PRINCIPAL);
            hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION_KEY, "kerberos");
        }
        this.kerberosAuthentication(this.kerberosPrincipal, this.kerberosKeytabFilePath);
        conf = new JobConf(hadoopConf);
        try {
            fileSystem = FileSystem.get(conf);
        }
        catch (IOException e) {
            String message = String.format("获取FileSystem时发生网络IO异常,请检查您的网络是否正常!HDFS地址：[message:defaultFS = %s]",
                    defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        catch (Exception e) {
            String message = String.format("获取FileSystem失败,请检查HDFS地址是否正确: [%s]",
                    "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }

        if (null == fileSystem) {
            String message = String.format("获取FileSystem失败,请检查HDFS地址是否正确: [message:defaultFS = %s]",
                    defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, message);
        }
    }

    private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath)
    {
        if (haveKerberos && StringUtils.isNotBlank(this.kerberosPrincipal) && StringUtils.isNotBlank(this.kerberosKeytabFilePath)) {
            UserGroupInformation.setConfiguration(this.hadoopConf);
            try {
                UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
            }
            catch (Exception e) {
                String message = String.format("kerberos认证失败,请确定kerberosKeytabFilePath[%s]和kerberosPrincipal[%s]填写正确",
                        kerberosKeytabFilePath, kerberosPrincipal);
                LOG.error(message);
                throw DataXException.asDataXException(HdfsWriterErrorCode.KERBEROS_LOGIN_ERROR, e);
            }
        }
    }

    /**
     * 获取指定目录先的文件列表
     *
     * @param dir 需要搜索的目录
     * @return 拿到的是文件全路径，
     * eg：hdfs://10.101.204.12:9000/user/hive/warehouse/writer.db/text/test.textfile
     */
    public String[] hdfsDirList(String dir)
    {
        Path path = new Path(dir);
        String[] files;
        try {
            FileStatus[] status = fileSystem.listStatus(path);
            files = new String[status.length];
            for (int i = 0; i < status.length; i++) {
                files[i] = status[i].getPath().toString();
            }
        }
        catch (IOException e) {
            String message = String.format("获取目录[%s]文件列表时发生网络IO异常,请检查您的网络是否正常！", dir);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return files;
    }

    /**
     * 获取以指定目录下的所有fileName开头的文件
     *
     * @param dir 需要扫描的目录
     * @param fileName String 要匹配的文件或者目录后缀，如果为空，则表示不做模式匹配
     * @return Path[]
     */
    public Path[] hdfsDirList(String dir, String fileName)
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
            String message = String.format("获取目录[%s]下文件列表时发生网络IO异常,请检查您的网络是否正常！", dir);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return files;
    }

    public boolean isPathexists(String filePath)
    {
        Path path = new Path(filePath);
        boolean exist;
        try {
            exist = fileSystem.exists(path);
        }
        catch (IOException e) {
            String message = String.format("判断文件路径[%s]是否存在时发生网络IO异常,请检查您的网络是否正常！",
                    "message:filePath =" + filePath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
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
            String message = String.format("判断路径[%s]是否是目录时发生网络IO异常,请检查您的网络是否正常！", filePath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return isDir;
    }

    /*
     * 根据标志来删除特定文件
     * @link(https://gitlab.ds.cfzq.com/grp_ds/datax/-/issues/8)
     * delDotFile: 是否删除点(.)开头的文件, true: 表示仅删除点开头的文件， false 表示不删除点开头的文件
     *
     */
    public void deleteFiles(Path[] paths, boolean delDotFile)
    {
        List<Path> needDelPaths;
        if (delDotFile) {
            LOG.info("ONLY delete files that starts with a dot (.)");
            needDelPaths = Arrays.stream(paths).filter(x -> x.getName().startsWith(".")).collect(Collectors.toList());
        }
        else {
            LOG.info("Delete all files that DO NOT start with a dot (.) in specified path");
            needDelPaths = Arrays.stream(paths).filter(x -> !x.getName().startsWith(".")).collect(Collectors.toList());
        }

        for (Path path : needDelPaths) {
            try {
                fileSystem.delete(path, true);
            }
            catch (IOException e) {
                LOG.error("IO exception occurred when deleting file [{}], please check your network", path);
                throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
            }
        }
    }

    public void deleteFiles(Path[] paths)
    {
        for (Path path : paths) {
            LOG.info("delete file [{}].", path);
            try {
                fileSystem.delete(path, true);
            }
            catch (IOException e) {
                LOG.error("IO exception occurred when deleting file [{}], please check your network", path);
                throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
            }
        }
    }

    public void deleteDir(Path path)
    {
        LOG.info("start delete tmp dir [{}] .", path);
        try {
            if (isPathexists(path.toString())) {
                fileSystem.delete(path, true);
            }
        }
        catch (Exception e) {
            LOG.error("删除临时目录[{}]时发生IO异常,请检查您的网络是否正常！", path);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        LOG.info("finish delete tmp dir [{}] .", path);
    }

    public void renameFile(Set<String> tmpFiles, Set<String> endFiles)
    {
        Path tmpFilesParent = null;
        if (tmpFiles.size() != endFiles.size()) {
            String message = "临时目录下文件名个数与目标文件名个数不一致!";
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.HDFS_RENAME_FILE_ERROR, message);
        }
        else {
            try {
                for (Iterator<String> it1 = tmpFiles.iterator(), it2 = endFiles.iterator(); it1.hasNext() && it2.hasNext(); ) {
                    String srcFile = it1.next();
                    String dstFile = it2.next();
                    Path srcFilePah = new Path(srcFile);
                    Path dstFilePah = new Path(dstFile);
                    if (tmpFilesParent == null) {
                        tmpFilesParent = srcFilePah.getParent();
                    }
                    LOG.info("start rename file [{}] to file [{}].", srcFile, dstFile);
                    boolean renameTag;
                    long fileLen = fileSystem.getFileStatus(srcFilePah).getLen();
                    if (fileLen > 0) {
                        renameTag = fileSystem.rename(srcFilePah, dstFilePah);
                        if (!renameTag) {
                            String message = String.format("重命名文件[%s]失败,请检查您的网络是否正常！", srcFile);
                            LOG.error(message);
                            throw DataXException.asDataXException(HdfsWriterErrorCode.HDFS_RENAME_FILE_ERROR, message);
                        }
                        LOG.info("finish rename file.");
                    }
                    else {
                        LOG.info("文件［{}］内容为空,请检查写入是否正常！", srcFile);
                    }
                }
            }
            catch (Exception e) {
                LOG.error("重命名文件时发生异常,请检查您的网络是否正常！");
                throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
            }
        }
    }

    //关闭FileSystem
    public void closeFileSystem()
    {
        try {
            fileSystem.close();
        }
        catch (IOException e) {
            LOG.error("关闭FileSystem时发生IO异常,请检查您的网络是否正常！");
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
    }

    // 写textfile类型文件
    public void textFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
            TaskPluginCollector taskPluginCollector)
    {
        char fieldDelimiter = config.getChar(Key.FIELD_DELIMITER);
        List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, "NONE").toUpperCase().trim();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
        String attempt = "attempt_" + dateFormat.format(new Date()) + "_0001_m_000000_0";
        Path outputPath = new Path(fileName);
        conf.set(JobContext.TASK_ATTEMPT_ID, attempt);
        FileOutputFormat.setOutputPath(conf, outputPath);
        FileOutputFormat.setWorkOutputPath(conf, outputPath);
        if (!"NONE".equals(compress)) {
            Class<? extends CompressionCodec> codecClass = getCompressCodec(compress);
            if (null != codecClass) {
                FileOutputFormat.setOutputCompressorClass(conf, codecClass);
            }
        }
        try {
            RecordWriter<NullWritable, Text> writer = new TextOutputFormat<NullWritable, Text>().getRecordWriter(fileSystem, conf, outputPath.toString(), Reporter.NULL);
            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                MutablePair<Text, Boolean> transportResult = transportOneRecord(record, fieldDelimiter, columns, taskPluginCollector);
                if (Boolean.FALSE.equals(transportResult.getRight())) {
                    writer.write(NullWritable.get(), transportResult.getLeft());
                }
            }
            writer.close(Reporter.NULL);
        }
        catch (Exception e) {
            LOG.error("写文件文件[{}]时发生IO异常,请检查您的网络是否正常！", fileName);
            Path path = new Path(fileName);
            deleteDir(path.getParent());
            throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
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
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("目前不支持您配置的 compress 模式 : [%s]", compress));
        }
        return codecClass;
    }

    public static int getDecimalprec(String type)
    {
        if (!type.contains("(")) {
            return DECIMAL_DEFAULT_PRECISION;
        }
        else {
            String regEx = "[^0-9]";
            Pattern p = Pattern.compile(regEx);
            Matcher m = p.matcher(type);
            return Integer.parseInt(m.replaceAll(" ").trim().split(" ")[0]);
        }
    }

    public static int getDecimalscale(String type)
    {
        if (!type.contains(",")) {
            return DECIMAL_DEFAULT_SCALE;
        }
        else {
            return Integer.parseInt(type.split(",")[1].replace(")", "").trim());
        }
    }

    /*
     * 写Parquetfile类型文件
     * 一个parquet文件的schema类似如下：
     * {
     *    "type":	"record",
     *    "name":	"testFile",
     *    "doc":	"test records",
     *    "fields":
     *      [{
     *        "name":	"id",
     *        "type":	"int"
     *
     *      },
     *      {
     *        "name":	"empName",
     *        "type":	"string"
     *      }
     *    ]
     *  }
     */
    public void parquetFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
            TaskPluginCollector taskPluginCollector)
    {

        List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, "UNCOMPRESSED").toUpperCase().trim();
        if ("NONE".equals(compress)) {
            compress = "UNCOMPRESSED";
        }
        // construct parquet schema

        String strschema = "{"
                + "\"type\": \"record\"," //Must be set as record
                + "\"name\": \"dataxFile\"," //Not used in Parquet, can put anything
                + "\"fields\": [";
        String filedName;
        String type;
        for (Configuration column : columns) {
            filedName = column.getString(Key.NAME);
            type = column.getString(Key.TYPE).trim();
            if ( "decimal".equals(type)) {
                strschema += " {\"name\": \"" + filedName
                        + "\", \"type\": [\"null\", {\"type\": \"fixed\", \"name\": \"decimal\", \"size\": 16, \"logicalType\": \"decimal\""
                        + ", \"precision\": " + column.getInt(Key.PRECISION) + ", \"scale\":" + column.getInt(Key.SCALE) + "}]},";
            }
            else {
                strschema += " {\"name\": \"" + filedName + "\", \"type\": [\"null\",\"" + type + "\"]},";
            }
        }
        Path path = new Path(fileName);
        LOG.info("write parquet file {}", fileName);
        strschema = strschema.substring(0, strschema.length() - 1) + " ]}";
        Schema.Parser parser = new Schema.Parser().setValidate(true);
        Schema parSchema = parser.parse(strschema);
        CompressionCodecName codecName = CompressionCodecName.fromConf(compress);

        GenericData decimalSupport = new GenericData();
        decimalSupport.addLogicalTypeConversion(new Conversions.DecimalConversion());
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(path)
                .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withSchema(parSchema)
                .withConf(hadoopConf)
                .withCompressionCodec(codecName)
                .withValidation(false)
                .withDictionaryEncoding(false)
                .withDataModel(decimalSupport)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .build()) {

            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                GenericRecordBuilder builder = new GenericRecordBuilder(parSchema);
                GenericRecord transportResult = transportParRecord(record, columns, taskPluginCollector, builder);
                writer.write(transportResult);
            }
        }
        catch (Exception e) {
            LOG.error("写文件文件[{}]时发生IO异常,请检查您的网络是否正常！", fileName);
            deleteDir(path.getParent());
            throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
    }

    private void setRow(VectorizedRowBatch batch, int row, Record record, List<Configuration> columns)
    {
        for (int i = 0; i < columns.size(); i++) {
            Configuration eachColumnConf = columns.get(i);
            String type = eachColumnConf.getString(Key.TYPE).trim().toUpperCase();
            SupportHiveDataType columnType;
            if (type.startsWith("DECIMAL")) {
                columnType = SupportHiveDataType.DECIMAL;
            }
            else {
                columnType = SupportHiveDataType.valueOf(type);
            }

            switch (columnType) {
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                case DATE:
                case BOOLEAN:
                    ((LongColumnVector) batch.cols[i]).vector[row] = record.getColumn(i).asLong();
                    break;
                case FLOAT:
                case DOUBLE:
                    ((DoubleColumnVector) batch.cols[i]).vector[row] = record.getColumn(i).asDouble();
                    break;
                case DECIMAL:
                    HiveDecimalWritable hdw = new HiveDecimalWritable();
                    hdw.set(HiveDecimal.create(record.getColumn(i).asBigDecimal()).setScale(eachColumnConf.getInt(Key.SCALE), HiveDecimal.ROUND_HALF_UP));
                    ((DecimalColumnVector) batch.cols[i]).set(row, hdw);
                    break;
                case TIMESTAMP:
                    ((TimestampColumnVector) batch.cols[i]).set(row, java.sql.Timestamp.valueOf(record.getColumn(i).asString()));
                    break;
                case STRING:
                case VARCHAR:
                case CHAR:
                case BINARY:
                    byte[] buffer;
                    if ("DATE".equals(record.getColumn(i).getType().toString())) {
                        buffer = record.getColumn(i).asString().getBytes(StandardCharsets.UTF_8);
                    }
                    else {
                        buffer = record.getColumn(i).asBytes();
                    }
                    ((BytesColumnVector) batch.cols[i]).setRef(row, buffer, 0, buffer.length);
                    break;
                default:
                    throw DataXException
                            .asDataXException(
                                    HdfsWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
                                            eachColumnConf.getString(Key.NAME),
                                            eachColumnConf.getString(Key.TYPE)));
            }
        }
    }

    /*
     * 写orcfile类型文件
     */
    public void orcFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
            TaskPluginCollector taskPluginCollector)
    {
        List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, "NONE").toUpperCase();
        List<String> columnNames = getColumnNames(columns);
        List<ObjectInspector> columnTypeInspectors = getColumnTypeInspectors(columns);
        StringJoiner joiner = new StringJoiner(",");
        for (int i = 0; i < columns.size(); i++) {
            if ("decimal".equals(columns.get(i).getString(Key.TYPE))) {
                joiner.add(columnNames.get(i) + ":decimal(" + columns.get(i).getString(Key.PRECISION) + "," + columns.get(i).getString(Key.SCALE) + ")");
            } else {
                joiner.add(columnNames.get(i) + ":" + columnTypeInspectors.get(i).getTypeName());
            }
        }
        TypeDescription schema = TypeDescription.fromString("struct<" + joiner + ">");
        try (Writer writer = OrcFile.createWriter(new Path(fileName),
                OrcFile.writerOptions(conf)
                        .setSchema(schema)
                        .compress(CompressionKind.valueOf(compress)))) {
            Record record;
            VectorizedRowBatch batch = schema.createRowBatch(1024);
            while ((record = lineReceiver.getFromReader()) != null) {
                int row = batch.size++;
                setRow(batch, row, record, columns);
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        catch (IOException e) {
            LOG.error("写文件文件[{}]时发生IO异常,请检查您的网络是否正常！", fileName);
            Path path = new Path(fileName);
            deleteDir(path.getParent());
            throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
    }

    public List<String> getColumnNames(List<Configuration> columns)
    {
        List<String> columnNames = Lists.newArrayList();
        for (Configuration eachColumnConf : columns) {
            columnNames.add(eachColumnConf.getString(Key.NAME));
        }
        return columnNames;
    }

    /*
     * 根据writer配置的字段类型，构建inspector
     */
    public List<ObjectInspector> getColumnTypeInspectors(List<Configuration> columns)
    {
        List<ObjectInspector> columnTypeInspectors = Lists.newArrayList();
        for (Configuration eachColumnConf : columns) {
            String type = eachColumnConf.getString(Key.TYPE).toUpperCase();
            SupportHiveDataType columnType;
            if (type.startsWith("DECIMAL")) {
                columnType = SupportHiveDataType.DECIMAL;
            }
            else {
                columnType = SupportHiveDataType.valueOf(eachColumnConf.getString(Key.TYPE).toUpperCase());
            }
            ObjectInspector objectInspector;
            switch (columnType) {
                case TINYINT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Byte.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case SMALLINT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Short.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case INT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case BIGINT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case FLOAT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Float.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case DOUBLE:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Double.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case DECIMAL:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(HiveDecimal.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

                    break;
                case TIMESTAMP:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Timestamp.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case DATE:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(org.apache.hadoop.hive.common.type.Date.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case STRING:
                case VARCHAR:
                case CHAR:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case BOOLEAN:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case BINARY:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Blob.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                default:
                    throw DataXException
                            .asDataXException(
                                    HdfsWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
                                            eachColumnConf.getString(Key.NAME),
                                            eachColumnConf.getString(Key.TYPE)));
            }

            columnTypeInspectors.add(objectInspector);
        }
        return columnTypeInspectors;
    }
}