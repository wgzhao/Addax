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
import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
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
import org.apache.kerby.config.Conf;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.parquet.avro.*;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.JulianFields;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

public class HdfsHelper {
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

    public static MutablePair<Text, Boolean> transportOneRecord(
            Record record, char fieldDelimiter, List<Configuration> columnsConfiguration, TaskPluginCollector taskPluginCollector) {
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
            TaskPluginCollector taskPluginCollector) {

        MutablePair<List<Object>, Boolean> transportResult = new MutablePair<>();
        transportResult.setRight(false);
        List<Object> recordList = new ArrayList<>();
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
                                throw AddaxException.asAddaxException(
                                        HdfsWriterErrorCode.ILLEGAL_VALUE,
                                        String.format(
                                                "The configuration is incorrect. The database does not support writing this type of field. " +
                                                        "Field name: [%s], field type: [%s].",
                                                columnsConfiguration.get(i).getString(Key.NAME),
                                                columnsConfiguration.get(i).getString(Key.TYPE)));
                        }
                    } catch (Exception e) {
                        // warn: 此处认为脏数据
                        e.printStackTrace();
                        String message = String.format(
                                "Type conversion error：target field type: [%s], field value: [%s].",
                                columnsConfiguration.get(i).getString(Key.TYPE), column.getRawData());
                        taskPluginCollector.collectDirtyRecord(record, message);
                        transportResult.setRight(true);
                        break;
                    }
                } else {
                    // warn: it's all ok if nullFormat is null
                    recordList.add(null);
                }
            }
        }
        transportResult.setLeft(recordList);
        return transportResult;
    }

    private static Binary toInt96(String value) throws ParseException {
//        String value = "2019-02-13 13:35:05";

        final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
        final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
        final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

// Parse date
        SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();
        cal.setTime(parser.parse(value));

// Calculate Julian days and nanoseconds in the day
        LocalDate dt = LocalDate.of(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH));
        int julianDays = (int) JulianFields.JULIAN_DAY.getFrom(dt);
        long nanos = (cal.get(Calendar.HOUR_OF_DAY) * NANOS_PER_HOUR)
                + (cal.get(Calendar.MINUTE) * NANOS_PER_MINUTE)
                + (cal.get(Calendar.SECOND) * NANOS_PER_SECOND);

// Write INT96 timestamp
        byte[] timestampBuffer = new byte[12];
        ByteBuffer buf = ByteBuffer.wrap(timestampBuffer);
        buf.order(ByteOrder.LITTLE_ENDIAN).putLong(nanos).putInt(julianDays);

// This is the properly encoded INT96 timestamp
        Binary tsValue = Binary.fromReusedByteArray(timestampBuffer);
        return tsValue;
    }

    public static GenericData.Record transportParRecord(
            Record record, List<Configuration> columnsConfiguration,
            TaskPluginCollector taskPluginCollector, GenericRecordBuilder builder) {

        int recordLength = record.getColumnNumber();

        if (0 != recordLength) {
            Column column;
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                String colName = columnsConfiguration.get(i).getString(Key.NAME);
                String typename = columnsConfiguration.get(i).getString(Key.TYPE).toUpperCase();
                if (null == column || column.getRawData() == null) {
                    builder.set(colName, null);
                } else {
                    String rowData = column.getRawData().toString();
                    SupportHiveDataType columnType = SupportHiveDataType.valueOf(typename);
                    //根据writer端类型配置做类型转换
                    try {
                        switch (columnType) {
                            case INT:
                            case INTEGER:
                                builder.set(colName, Integer.valueOf(rowData));
                                break;
                            case LONG:
                                builder.set(colName, column.asLong());
                                break;
                            case FLOAT:
                                builder.set(colName, Float.valueOf(rowData));
                                break;
                            case DOUBLE:
                                builder.set(colName, column.asDouble());
                                break;
                            case STRING:
                                builder.set(colName, column.asString());
                                break;
                            case DECIMAL:
                                builder.set(colName, new BigDecimal(column.asString()).setScale(columnsConfiguration.get(i).getInt(Key.SCALE), BigDecimal.ROUND_HALF_UP));
                                break;
                            case BOOLEAN:
                                builder.set(colName, column.asBoolean());
                                break;
                            case BINARY:
                                builder.set(colName, column.asBytes());
                                break;
                            case TIMESTAMP:
//                                builder.set(colName, toInt96(column.asString()));
                                Binary int96 = toInt96("123.45");
                                Schema int96schema = Schema.createFixed("INT96", "INT96 represented as byte[12]", null, 12);
                                GenericFixed genericFixed = new GenericData.Fixed(int96schema, int96.getBytes());
                                builder.set(colName, int96);
//                                builder.set(colName, column.asString());
                                break;
                            default:
                                throw AddaxException
                                        .asAddaxException(
                                                HdfsWriterErrorCode.ILLEGAL_VALUE,
                                                String.format(
                                                        "The columns configuration is incorrect. the field type is unsupported." +
                                                                "Field name:[%s], Field type:[%s].",
                                                        columnsConfiguration.get(i).getString(Key.NAME),
                                                        columnsConfiguration.get(i).getString(Key.TYPE)));
                        }
                    } catch (Exception e) {
                        // warn: 此处认为脏数据
                        String message = String.format(
                                "Type conversion error for field. destination column type: [%s], actual column value: [%s].",
                                columnsConfiguration.get(i).getString(Key.TYPE), column.getRawData());
                        taskPluginCollector.collectDirtyRecord(record, message);
                        break;
                    }
                }
            }
        }
        return builder.build();
    }

    public void getFileSystem(String defaultFS, Configuration taskConfig) {
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
            // fix Failed to specify server's Kerberos principal name
            if (Objects.equals(hadoopConf.get("dfs.namenode.kerberos.principal", ""), "")) {
                // get REALM
                String serverPrincipal = "nn/_HOST@" + Iterables.get(Splitter.on('@').split(this.kerberosPrincipal), 1);
                hadoopConf.set("dfs.namenode.kerberos.principal", serverPrincipal);
            }
        }
        this.kerberosAuthentication(this.kerberosPrincipal, this.kerberosKeytabFilePath);
        conf = new JobConf(hadoopConf);
        try {
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            String message = String.format("Network IO exception occurred while obtaining Filesystem with defaultFS: [%s]",
                    defaultFS);
            LOG.error(message);
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        } catch (Exception e) {
            String message = String.format("Failed to obtain Filesystem with defaultFS: [%s]", defaultFS);
            LOG.error(message);
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }

        if (null == fileSystem) {
            String message = String.format("Failed to obtain Filesystem with defaultFS: [%s].", defaultFS);
            LOG.error(message);
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, message);
        }
    }

    private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath) {
        if (haveKerberos && StringUtils.isNotBlank(this.kerberosPrincipal) && StringUtils.isNotBlank(this.kerberosKeytabFilePath)) {
            UserGroupInformation.setConfiguration(this.hadoopConf);
            try {
                UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
            } catch (Exception e) {
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
    public Path[] hdfsDirList(String dir) {
        Path path = new Path(dir);
        Path[] files;
        try {
            FileStatus[] status = fileSystem.listStatus(path);
            files = new Path[status.length];
            for (int i = 0; i < status.length; i++) {
                files[i] = status[i].getPath();
            }
        } catch (IOException e) {
            String message = String.format("Network IO exception occurred while fetching file list for directory [%s]", dir);
            LOG.error(message);
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return files;
    }

    public boolean isPathExists(String filePath) {
        Path path = new Path(filePath);
        boolean exist;
        try {
            exist = fileSystem.exists(path);
        } catch (IOException e) {
            LOG.error("Network IO exception occurred while checking if file path [{}] exists", filePath);
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return exist;
    }

    public boolean isPathDir(String filePath) {
        Path path = new Path(filePath);
        boolean isDir;
        try {
            isDir = fileSystem.getFileStatus(path).isDirectory();
        } catch (IOException e) {
            LOG.error("Network IO exception occurred while checking if path [{}] is directory or not.", filePath);
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return isDir;
    }

    public void deleteFilesFromDir(Path dir, boolean skipTrash) {
        try {
            final RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(dir, false);
            if (skipTrash) {
                while (files.hasNext()) {
                    final LocatedFileStatus next = files.next();
                    LOG.info("Delete file [{}]", next.getPath());
                    fileSystem.delete(next.getPath(), false);
                }
            } else {
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
        } catch (FileNotFoundException fileNotFoundException) {
            throw new AddaxException(HdfsWriterErrorCode.FILE_NOT_FOUND, fileNotFoundException.getMessage());
        } catch (IOException ioException) {
            throw new AddaxException(HdfsWriterErrorCode.IO_ERROR, ioException.getMessage());
        }
    }

    public void deleteDir(Path path) {
        LOG.info("Begin to delete temporary dir [{}] .", path);
        try {
            if (isPathExists(path.toString())) {
                fileSystem.delete(path, true);
            }
        } catch (Exception e) {
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
    public void moveFilesToDest(Path sourceDir, Path targetDir) {
        try {
            final FileStatus[] fileStatuses = fileSystem.listStatus(sourceDir);
            for (FileStatus file : fileStatuses) {
                if (file.isFile() && file.getLen() > 0) {
                    LOG.info("Begin to move file from [{}] to [{}].", file.getPath(), targetDir.getName());
                    fileSystem.rename(file.getPath(), new Path(targetDir, file.getPath().getName()));
                }
            }
        } catch (IOException e) {
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.IO_ERROR, e);
        }
        LOG.info("Finish move file(s).");
    }

    //关闭FileSystem
    public void closeFileSystem() {
        try {
            fileSystem.close();
        } catch (IOException e) {
            LOG.error("IO exception occurred while closing Filesystem.");
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
    }

    // 写text file类型文件
    public void textFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
                                   TaskPluginCollector taskPluginCollector) {
        char fieldDelimiter = config.getChar(Key.FIELD_DELIMITER);
        List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, "NONE").toUpperCase().trim();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
        String attempt = "attempt_" + dateFormat.format(new Date()) + "_0001_m_000000_0";
        conf.set(JobContext.TASK_ATTEMPT_ID, attempt);
        if (!"NONE".equals(compress)) {
            // fileName must remove suffix, because the FileOutputFormat will add suffix
            fileName = fileName.substring(0, fileName.lastIndexOf("."));
            Class<? extends CompressionCodec> codecClass = getCompressCodec(compress);
            if (null != codecClass) {
                FileOutputFormat.setOutputCompressorClass(conf, codecClass);
            }
        }
        Path outputPath = new Path(fileName);
        FileOutputFormat.setOutputPath(conf, outputPath);
        FileOutputFormat.setWorkOutputPath(conf, outputPath);
        try {
            RecordWriter<NullWritable, Text> writer = new TextOutputFormat<NullWritable, Text>()
                    .getRecordWriter(fileSystem, conf, outputPath.toString(), Reporter.NULL);
            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                MutablePair<Text, Boolean> transportResult = transportOneRecord(record, fieldDelimiter, columns, taskPluginCollector);
                if (Boolean.FALSE.equals(transportResult.getRight())) {
                    writer.write(NullWritable.get(), transportResult.getLeft());
                }
            }
            writer.close(Reporter.NULL);
        } catch (Exception e) {
            LOG.error("IO exception occurred while writing text file [{}]", fileName);
            Path path = new Path(fileName);
            deleteDir(path.getParent());
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
    }

    // compress 已经转为大写
    public Class<? extends CompressionCodec> getCompressCodec(String compress) {
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

    /*
     * 写Parquet file类型文件
     * 一个parquet文件的schema类似如下：
     * {
     *    "type":	"record",
     *    "name":	"testFile",
     *    "doc":	"test records",
     *    "fields":
     *      [{
     *        "name":	"id",
     *        "type":	["null", "int"]
     *
     *      },
     *      {
     *        "name":	"empName",
     *        "type":	"string"
     *      }
     *    ]
     *  }
     * "null" 表示该字段允许为空
     */
    public void parquetFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
                                      TaskPluginCollector taskPluginCollector) {

        List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, "UNCOMPRESSED").toUpperCase().trim();
        if ("NONE".equals(compress)) {
            compress = "UNCOMPRESSED";
        }
        // construct parquet schema
//        Schema schema = generateParquetSchema(columns);
        MessageType s = genParquetSchema(columns);
//        Schema schema = new AvroSchemaConverter().convert(s);
        Path path = new Path(fileName);
        LOG.info("Begin to write parquet file [{}]", fileName);
        CompressionCodecName codecName = CompressionCodecName.fromConf(compress);

        GenericData decimalSupport = new GenericData();
        decimalSupport.addLogicalTypeConversion(new Conversions.DecimalConversion());
        hadoopConf.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);
        hadoopConf.setBoolean(AvroWriteSupport.WRITE_FIXED_AS_INT96, true);
        GroupWriteSupport.setSchema(s, hadoopConf);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(HadoopOutputFile.fromPath(path, hadoopConf))
                .withConf(hadoopConf)
                .enableDictionaryEncoding()
                .withPageSize(1024)
                .withDictionaryPageSize(512)
                .withValidation(false)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .build()) {
            SimpleGroupFactory f = new SimpleGroupFactory(s);
            Group group;
            Record record;
            Column column;
            while ((record = lineReceiver.getFromReader()) != null) {
                group = f.newGroup();
                for (int i = 0; i < record.getColumnNumber(); i++) {
                    column = record.getColumn(i);
                    String colName = columns.get(i).getString(Key.NAME);
                    String typename = columns.get(i).getString(Key.TYPE).toUpperCase();
                    if (null == column || column.getRawData() == null) {
                        group.append(colName, "");
                    }
                    SupportHiveDataType columnType = SupportHiveDataType.valueOf(typename);
                    switch (columnType) {
                        case INT:
                        case INTEGER:
                            group.append(colName, Integer.valueOf(column.getRawData().toString()));
                            break;
                        case LONG:
                            group.append(colName, column.asLong());
                            break;
                        case STRING:
                            group.append(colName, column.asString());
                            break;
                        case DECIMAL:
                            group.append(colName, decimalToBinary(column.asString()));
                            break;
                        case TIMESTAMP:
                            SimpleDateFormat sdf = new SimpleDateFormat(Constant.DEFAULT_DATE_FORMAT);
                            group.append(colName, toInt96(sdf.format(column.asDate())));
                            break;
                        case DATE:
                            group.append(colName, (int) Math.round(column.asLong() * 1.0 / 86400000));
                            break;
                        default:
                            group.append(colName, column.asString());
                            break;
                    }
                }
                writer.write(group);
//                GenericRecord transportResult = transportParRecord(record, columns, taskPluginCollector, group);
//                writer.write(transportResult);
            }
        } catch (ParseException | IOException e) {
            throw new RuntimeException(e);
        }

//        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
//                .<GenericRecord>builder(path)
//                .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
//                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
//                .withSchema(schema)
//                .withConf(hadoopConf)
//                .withCompressionCodec(codecName)
//                .withValidation(false)
//                .withDictionaryEncoding(false)
//                .withDataModel(decimalSupport)
//                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
//                .build()) {
//            SimpleGroupFactory f = new SimpleGroupFactory(new AvroSchemaConverter().convert(schema));
//            Record record;
//            while ((record = lineReceiver.getFromReader()) != null) {
//                GenericRecordBuilder builder = new GenericRecordBuilder(schema);
//
//                GenericRecord transportResult = transportParRecord(record, columns, taskPluginCollector, builder);
//                writer.write(transportResult);
//            }
//        } catch (Exception e) {
//            LOG.error("IO exception occurred while writing file [{}].", fileName);
//            deleteDir(path.getParent());
//            throw AddaxException.asAddaxException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
//        }
    }

    private Binary decimalToBinary(String bigDecimal) {
        BigDecimal myDecimalValue = new BigDecimal(bigDecimal);

        //Next we get the decimal value as one BigInteger (like there was no decimal point)
        BigInteger myUnscaledDecimalValue = myDecimalValue.unscaledValue();

        //Finally we serialize the integer
        byte[] decimalBytes = myUnscaledDecimalValue.toByteArray();

//We need to create an Avro 'Fixed' type and pass the decimal schema once more here:
//        GenericData.Fixed fixed = new GenericData.Fixed(
//                new Schema.Parser().parse("required fixed_len_byte_array(12) dec_field (DECIMAL(12,4));"));

        int PRECISION_TO_BYTE_COUNT[] = new int[38];
        for (int i = 1; i <= 38; i++) {
            // Estimated number of bytes needed.
            PRECISION_TO_BYTE_COUNT[i - 1] = (int)
                    Math.ceil((Math.log(Math.pow(10, i) - 1) / Math.log(2) + 1) / 8);
        }
        byte[] myDecimalBuffer = new byte[16];
        if (myDecimalBuffer.length >= decimalBytes.length) {
            //Because we set our fixed byte array size as 16 bytes, we need to
            //pad-left our original value's bytes with zeros
            int myDecimalBufferIndex = myDecimalBuffer.length - 1;
            for (int i = decimalBytes.length - 1; i >= 0; i--) {
                myDecimalBuffer[myDecimalBufferIndex] = decimalBytes[i];
                myDecimalBufferIndex--;
            }
            return Binary.fromConstantByteArray(myDecimalBuffer);
            //Save result
//            fixed.bytes(myDecimalBuffer);
        } else {
            throw new IllegalArgumentException(String.format("Decimal size: %d was greater than the allowed max: %d", decimalBytes.length, myDecimalBuffer.length));
        }
        //We can finally write our decimal to our record

    }

    private MessageType genParquetSchema(List<Configuration> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("message addax {");
        String type;
        String fieldName;
        Type t;
        Types.MessageTypeBuilder builder = Types.buildMessage();
        Type.Repetition repetition = Type.Repetition.OPTIONAL;
        List<Type> types = new ArrayList<>();
        for (Configuration column : columns) {
            type = column.getString(Key.TYPE).trim().toUpperCase();
            fieldName = column.getString(Key.NAME);
            switch (type) {
                case "INT":
                    t = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition).named(fieldName);
                    break;
                case "DECIMAL":
//                    int PRECISION_TO_BYTE_COUNT[] = new int[38];
//                    for (int i = 1; i <= 38; i++) {
//                        // Estimated number of bytes needed.
//                        PRECISION_TO_BYTE_COUNT[i - 1] = (int)
//                                Math.ceil((Math.log(Math.pow(10, i) - 1) / Math.log(2) + 1) / 8);
//                    }
                    int prec = column.getInt(Key.PRECISION, Constant.DEFAULT_DECIMAL_MAX_PRECISION);
                    int scale = column.getInt(Key.SCALE, Constant.DEFAULT_DECIMAL_MAX_SCALE);
                    t = Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
                            .length(16)
                            .as(decimalType(scale, prec))
                            .named(fieldName);
                    break;
                case "STRING":
                    t = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition).as(LogicalTypeAnnotation.stringType()).named(fieldName);
                    break;
                case "BYTES":
                    t = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition).named(fieldName);
                    break;
                case "DATE":
                    t = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition).as(LogicalTypeAnnotation.dateType()).named(fieldName);
//                    sb.append("optional int32 ").append(fieldName).append("(DATE)").append(";");
                    break;
                case "TIMESTAMP":
                    t = Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, repetition).named(fieldName);
//                    sb.append("optional int96 ").append(fieldName).append(";");
                    break;
                default:
                    t = Types.primitive(PrimitiveType.PrimitiveTypeName.valueOf(type), Type.Repetition.OPTIONAL).named(fieldName);
//                    sb.append(t.toString()).append(fieldName).append(";");
                    break;
            }
            builder.addField(t);
        }
        return builder.named("addax");
//        return parseMessageType(sb.toString());
    }

    private Schema generateParquetSchema(List<Configuration> columns) {
        List<Schema.Field> fields = new ArrayList<>();
        String fieldName;
        String type;
        List<Schema> unionList = new ArrayList<>(2);
        for (Configuration column : columns) {
            unionList.clear();
            fieldName = column.getString(Key.NAME);
            type = column.getString(Key.TYPE).trim().toUpperCase();
            unionList.add(Schema.create(Schema.Type.NULL));
            switch (type) {
                case "DECIMAL":
                    Schema dec = LogicalTypes
                            .decimal(column.getInt(Key.PRECISION, Constant.DEFAULT_DECIMAL_MAX_PRECISION),
                                    column.getInt(Key.SCALE, Constant.DEFAULT_DECIMAL_MAX_SCALE))
                            .addToSchema(Schema.createFixed(fieldName, null, null, 16));
                    unionList.add(dec);
                    break;
                case "DATE":
                    Schema date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
                    unionList.add(date);
                    break;
                case "TIMESTAMP":
//                    Schema ts = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
//                    Schema ts = LogicalTypes.timestampMillis().addToSchema(Schema.createFixed("INT96", "INT96 represented as byte[12]",
//                            null, 12));
                    MessageType n = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT96).named(fieldName).named(fieldName);
                    Schema int96schema = Schema.createFixed("INT96", "INT96 represented as byte[12]", null, 12);
                    unionList.add(int96schema);
//                    unionList.add(n);
//                    unionList.add(Schema.createFixed("INT96", "INT96 represented as byte[12]", null, 12));
                    break;
                case "UUID":
                    Schema uuid = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
                    unionList.add(uuid);
                    break;
                case "BINARY":
                    unionList.add(Schema.create(Schema.Type.BYTES));
                    break;
                default:
                    // other types
                    unionList.add(Schema.create(Schema.Type.valueOf(type)));
                    break;
            }
            fields.add(new Schema.Field(fieldName, Schema.createUnion(unionList), null, null));
        }
        Schema schema = Schema.createRecord("addax", null, "parquet", false);
        schema.setFields(fields);
        return schema;
    }

    /**
     * write an orc record
     *
     * @param batch               {@link VectorizedRowBatch}
     * @param row                 row number
     * @param record              {@link Record}
     * @param columns             table columns, {@link List}
     * @param taskPluginCollector {@link TaskPluginCollector}
     */
    private void setRow(VectorizedRowBatch batch, int row, Record record, List<Configuration> columns,
                        TaskPluginCollector taskPluginCollector) {
        for (int i = 0; i < columns.size(); i++) {
            Configuration eachColumnConf = columns.get(i);
            String type = eachColumnConf.getString(Key.TYPE).trim().toUpperCase();
            SupportHiveDataType columnType;
            ColumnVector col = batch.cols[i];
            if (type.startsWith("DECIMAL")) {
                columnType = SupportHiveDataType.DECIMAL;
            } else {
                columnType = SupportHiveDataType.valueOf(type);
            }
            if (record.getColumn(i) == null || record.getColumn(i).getRawData() == null) {
                col.isNull[row] = true;
                col.noNulls = false;
                continue;
            }

            try {
                switch (columnType) {
                    case TINYINT:
                    case SMALLINT:
                    case INT:
                    case BIGINT:
                    case BOOLEAN:
                        ((LongColumnVector) col).vector[row] = record.getColumn(i).asLong();
                        break;
                    case DATE:
                        ((LongColumnVector) col).vector[row] = LocalDate.parse(record.getColumn(i).asString()).toEpochDay();
                        break;
                    case FLOAT:
                    case DOUBLE:
                        ((DoubleColumnVector) col).vector[row] = record.getColumn(i).asDouble();
                        break;
                    case DECIMAL:
                        HiveDecimalWritable hdw = new HiveDecimalWritable();
                        hdw.set(HiveDecimal.create(record.getColumn(i).asBigDecimal())
                                .setScale(eachColumnConf.getInt(Key.SCALE), HiveDecimal.ROUND_HALF_UP));
                        ((DecimalColumnVector) col).set(row, hdw);
                        break;
                    case TIMESTAMP:
                        ((TimestampColumnVector) col).set(row, record.getColumn(i).asTimestamp());
                        break;
                    case STRING:
                    case VARCHAR:
                    case CHAR:
                        byte[] buffer;
                        if (record.getColumn(i).getType() == Column.Type.BYTES) {
                            //convert bytes to base64 string
                            buffer = Base64.getEncoder().encode((byte[]) record.getColumn(i).getRawData());
                        } else if (record.getColumn(i).getType() == Column.Type.DATE) {
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                            buffer = sdf.format(record.getColumn(i).asDate()).getBytes(StandardCharsets.UTF_8);
                        } else {
                            buffer = record.getColumn(i).getRawData().toString().getBytes(StandardCharsets.UTF_8);
                        }
                        ((BytesColumnVector) col).setRef(row, buffer, 0, buffer.length);
                        break;
                    case BINARY:
                        byte[] content = (byte[]) record.getColumn(i).getRawData();
                        ((BytesColumnVector) col).setRef(row, content, 0, content.length);
                        break;
                    default:
                        throw AddaxException
                                .asAddaxException(
                                        HdfsWriterErrorCode.ILLEGAL_VALUE,
                                        String.format("The columns configuration is incorrect. the field type is unsupported yet. Field name: [%s], Field type name:[%s].",
                                                eachColumnConf.getString(Key.NAME),
                                                eachColumnConf.getString(Key.TYPE)));
                }
            } catch (Exception e) {
                taskPluginCollector.collectDirtyRecord(record, e.getMessage());
                throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("Failed to set ORC row, source field type: %s, destination field original type: %s, " +
                                        "destination field hive type: %s, field name: %s, source field value: %s, root cause:%n%s",
                                record.getColumn(i).getType(), columnType, eachColumnConf.getString(Key.TYPE),
                                eachColumnConf.getString(Key.NAME),
                                record.getColumn(i).getRawData(), e));
            }
        }
    }

    /*
     * 写orcfile类型文件
     */
    public void orcFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
                                  TaskPluginCollector taskPluginCollector) {
        List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, "NONE").toUpperCase();
        StringJoiner joiner = new StringJoiner(",");
        for (Configuration column : columns) {
            if ("decimal".equals(column.getString(Key.TYPE))) {
                joiner.add(String.format("%s:%s(%s,%s)", column.getString(Key.NAME), "decimal",
                        column.getInt(Key.PRECISION, Constant.DEFAULT_DECIMAL_MAX_PRECISION),
                        column.getInt(Key.SCALE, Constant.DEFAULT_DECIMAL_MAX_SCALE)));
            } else {
                joiner.add(String.format("%s:%s", column.getString(Key.NAME), column.getString(Key.TYPE)));
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
                setRow(batch, row, record, columns, taskPluginCollector);
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        } catch (IOException e) {
            LOG.error("IO exception occurred while writing file [{}}.", fileName);
            Path path = new Path(fileName);
            deleteDir(path.getParent());
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
    }
}
