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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

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
                                throw AddaxException
                                        .asAddaxException(
                                                HdfsWriterErrorCode.ILLEGAL_VALUE,
                                                String.format(
                                                        "您的配置文件中的列配置信息有误. 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
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
                String colName = columnsConfiguration.get(i).getString(Key.NAME);
                String typename = columnsConfiguration.get(i).getString(Key.TYPE).toUpperCase();
                if (null == column || column.getRawData() == null) {
                    builder.set(colName, null);
                }
                else {
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
                                builder.set(colName, column.asLong() / 1000);
                                break;
                            default:
                                throw AddaxException
                                        .asAddaxException(
                                                HdfsWriterErrorCode.ILLEGAL_VALUE,
                                                String.format(
                                                        "您的配置文件中的列配置信息有误. 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
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
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        catch (Exception e) {
            String message = String.format("获取FileSystem失败,请检查HDFS地址是否正确: [%s]",
                    "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }

        if (null == fileSystem) {
            String message = String.format("获取FileSystem失败,请检查HDFS地址是否正确: [message:defaultFS = %s]",
                    defaultFS);
            LOG.error(message);
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, message);
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
                throw AddaxException.asAddaxException(HdfsWriterErrorCode.KERBEROS_LOGIN_ERROR, e);
            }
        }
    }

    /**
     * 获取指定目录下的文件列表
     *
     * @param dir 需要搜索的目录
     * @return 文件数组，文件是全路径，
     * eg：hdfs://10.101.204.12:9000/user/hive/warehouse/writer.db/text/test.textfile
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
            String message = String.format("获取目录[%s]文件列表时发生网络IO异常,请检查您的网络是否正常！", dir);
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
            String message = String.format("判断文件路径[%s]是否存在时发生网络IO异常,请检查您的网络是否正常！",
                    "message:filePath =" + filePath);
            LOG.error(message);
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
            String message = String.format("判断路径[%s]是否是目录时发生网络IO异常,请检查您的网络是否正常！", filePath);
            LOG.error(message);
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return isDir;
    }

    public void deleteFilesFromDir(Path dir)
    {
        try {
            final RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(dir, false);
            while (files.hasNext()) {
                final LocatedFileStatus next = files.next();
                fileSystem.deleteOnExit(next.getPath());
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
        LOG.info("start delete tmp dir [{}] .", path);
        try {
            if (isPathExists(path.toString())) {
                fileSystem.delete(path, true);
            }
        }
        catch (Exception e) {
            LOG.error("删除临时目录[{}]时发生IO异常,请检查您的网络是否正常！", path);
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        LOG.info("finish delete tmp dir [{}] .", path);
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
                    LOG.info("start move file [{}] to dir [{}].", file.getPath(), targetDir.getName());
                    fileSystem.rename(file.getPath(), new Path(targetDir, file.getPath().getName()));
                }
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.IO_ERROR, e);
        }
        LOG.info("finish move file(s).");
    }

    //关闭FileSystem
    public void closeFileSystem()
    {
        try {
            fileSystem.close();
        }
        catch (IOException e) {
            LOG.error("关闭FileSystem时发生IO异常,请检查您的网络是否正常！");
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
    }

    // 写text file类型文件
    public void textFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
            TaskPluginCollector taskPluginCollector)
    {
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
        }
        catch (Exception e) {
            LOG.error("写文件文件[{}]时发生IO异常,请检查您的网络是否正常！", fileName);
            Path path = new Path(fileName);
            deleteDir(path.getParent());
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
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
                        String.format("目前不支持您配置的 compress 模式 : [%s]", compress));
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
            TaskPluginCollector taskPluginCollector)
    {

        List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, "UNCOMPRESSED").toUpperCase().trim();
        if ("NONE".equals(compress)) {
            compress = "UNCOMPRESSED";
        }
        // construct parquet schema
        Schema schema = generateParquetSchema(columns);

        Path path = new Path(fileName);
        LOG.info("write parquet file {}", fileName);
        CompressionCodecName codecName = CompressionCodecName.fromConf(compress);

        GenericData decimalSupport = new GenericData();
        decimalSupport.addLogicalTypeConversion(new Conversions.DecimalConversion());

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(path)
                .withRowGroupSize((long) ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withSchema(schema)
                .withConf(hadoopConf)
                .withCompressionCodec(codecName)
                .withValidation(false)
                .withDictionaryEncoding(false)
                .withDataModel(decimalSupport)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .build()) {

            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                GenericRecordBuilder builder = new GenericRecordBuilder(schema);
                GenericRecord transportResult = transportParRecord(record, columns, taskPluginCollector, builder);
                writer.write(transportResult);
            }
        }
        catch (Exception e) {
            LOG.error("写文件文件[{}]时发生IO异常,请检查您的网络是否正常！", fileName);
            deleteDir(path.getParent());
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
    }

    private Schema generateParquetSchema(List<Configuration> columns)
    {
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
                            .decimal(column.getInt(Key.PRECISION, Constant.DEFAULT_DECIMAL_PRECISION),
                                    column.getInt(Key.SCALE, Constant.DEFAULT_DECIMAL_SCALE))
                            .addToSchema(Schema.createFixed(fieldName, null, null, 16));
                    unionList.add(dec);
                    break;
                case "DATE":
                    Schema date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
                    unionList.add(date);
                    break;
                case "TIMESTAMP":
                    Schema ts = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
                    unionList.add(ts);
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

    private void setRow(VectorizedRowBatch batch, int row, Record record, List<Configuration> columns,
            TaskPluginCollector taskPluginCollector)
    {
        for (int i = 0; i < columns.size(); i++) {
            Configuration eachColumnConf = columns.get(i);
            String type = eachColumnConf.getString(Key.TYPE).trim().toUpperCase();
            SupportHiveDataType columnType;
            ColumnVector col = batch.cols[i];
            if (type.startsWith("DECIMAL")) {
                columnType = SupportHiveDataType.DECIMAL;
            }
            else {
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
                    case DATE:
                        ((LongColumnVector) col).vector[row] = record.getColumn(i).asLong();
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
                        ((TimestampColumnVector) col).set(row, java.sql.Timestamp.valueOf(record.getColumn(i).asString()));
                        break;
                    case STRING:
                    case VARCHAR:
                    case CHAR:
                        byte[] buffer = record.getColumn(i).getRawData().toString().getBytes(StandardCharsets.UTF_8);
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
                                        String.format("您的配置文件中的列配置信息有误. 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%s]. " +
                                                        "请修改表中该字段的类型或者不同步该字段.",
                                                eachColumnConf.getString(Key.NAME),
                                                eachColumnConf.getString(Key.TYPE)));
                }
            }
            catch (Exception e) {
                taskPluginCollector.collectDirtyRecord(record, e.getMessage());
                throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("设置Orc数据行失败，源列类型: %s, 目的原始类型:%s, 目的列Hive类型: %s, 字段名称: %s, 源值: %s, 错误根源:%n%s",
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
            TaskPluginCollector taskPluginCollector)
    {
        List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, "NONE").toUpperCase();
        StringJoiner joiner = new StringJoiner(",");
        for (Configuration column : columns) {
            if ("decimal".equals(column.getString(Key.TYPE))) {
                joiner.add(String.format("%s:%s(%s,%s)", column.getString(Key.NAME), "decimal",
                        column.getInt(Key.PRECISION, Constant.DEFAULT_DECIMAL_PRECISION),
                        column.getInt(Key.SCALE, Constant.DEFAULT_DECIMAL_SCALE)));
            }
            else if ("date".equalsIgnoreCase(column.getString(Key.TYPE))) {
                joiner.add(String.format("%s:bigint", column.getString(Key.NAME)));
            }
            else {
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
        }
        catch (IOException e) {
            LOG.error("写文件文件[{}]时发生IO异常,请检查您的网络是否正常！", fileName);
            Path path = new Path(fileName);
            deleteDir(path.getParent());
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
    }
}