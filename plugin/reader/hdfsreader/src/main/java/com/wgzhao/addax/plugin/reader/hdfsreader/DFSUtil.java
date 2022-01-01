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

package com.wgzhao.addax.plugin.reader.hdfsreader;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.BoolColumn;
import com.wgzhao.addax.common.element.BytesColumn;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.ColumnEntry;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.storage.reader.StorageReaderErrorCode;
import com.wgzhao.addax.storage.reader.StorageReaderUtil;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileRecordReader;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.orc.TypeDescription;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.wgzhao.addax.common.base.Key.COLUMN;
import static com.wgzhao.addax.common.base.Key.NULL_FORMAT;

/**
 * Created by mingya.wmy on 2015/8/12.
 */
public class DFSUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(DFSUtil.class);

    // the offset of julian, 2440588 is 1970/1/1
    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
    private static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);

    private static final int DIRECTORY_SIZE_GUESS = 16 * 1024;
    private final org.apache.hadoop.conf.Configuration hadoopConf;
    private final boolean haveKerberos;
    private final HashSet<String> sourceHDFSAllFilesList = new HashSet<>();
    private String specifiedFileType = null;
    private String kerberosKeytabFilePath;
    private String kerberosPrincipal;

    public DFSUtil(Configuration taskConfig)
    {
        hadoopConf = new org.apache.hadoop.conf.Configuration();
        //io.file.buffer.size 性能参数
        //http://blog.csdn.net/yangjl38/article/details/7583374
        Configuration hadoopSiteParams = taskConfig.getConfiguration(Key.HADOOP_CONFIG);
        JSONObject hadoopSiteParamsAsJsonObject = JSON.parseObject(taskConfig.getString(Key.HADOOP_CONFIG));
        if (null != hadoopSiteParams) {
            Set<String> paramKeys = hadoopSiteParams.getKeys();
            for (String each : paramKeys) {
                hadoopConf.set(each, hadoopSiteParamsAsJsonObject.getString(each));
            }
        }
        hadoopConf.set(HdfsConstant.HDFS_DEFAULT_KEY, taskConfig.getString(Key.DEFAULT_FS));

        //是否有Kerberos认证
        this.haveKerberos = taskConfig.getBool(Key.HAVE_KERBEROS, false);
        if (haveKerberos) {
            this.kerberosKeytabFilePath = taskConfig.getString(Key.KERBEROS_KEYTAB_FILE_PATH);
            this.kerberosPrincipal = taskConfig.getString(Key.KERBEROS_PRINCIPAL);
            this.hadoopConf.set(HdfsConstant.HADOOP_SECURITY_AUTHENTICATION_KEY, "kerberos");
        }
        this.kerberosAuthentication(this.kerberosPrincipal, this.kerberosKeytabFilePath);

        LOG.info("hadoopConfig details:{}", JSON.toJSONString(this.hadoopConf));
    }

    private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath)
    {
        if (haveKerberos && StringUtils.isNotBlank(kerberosPrincipal) && StringUtils.isNotBlank(kerberosKeytabFilePath)) {
            UserGroupInformation.setConfiguration(hadoopConf);
            try {
                UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
            }
            catch (Exception e) {
                String message = String.format("kerberos认证失败,请确定kerberosKeytabFilePath[%s]和kerberosPrincipal[%s]填写正确",
                        kerberosKeytabFilePath, kerberosPrincipal);
                throw AddaxException.asAddaxException(HdfsReaderErrorCode.KERBEROS_LOGIN_ERROR, message, e);
            }
        }
    }

    /**
     * 获取指定路径列表下符合条件的所有文件的绝对路径
     *
     * @param srcPaths 路径列表
     * @param specifiedFileType 指定文件类型
     * @return set of string
     */
    public Set<String> getAllFiles(List<String> srcPaths, String specifiedFileType)
    {

        this.specifiedFileType = specifiedFileType;

        if (!srcPaths.isEmpty()) {
            for (String eachPath : srcPaths) {
                LOG.info("get HDFS all files in path = [{}]", eachPath);
                getHDFSAllFiles(eachPath);
            }
        }
        return sourceHDFSAllFilesList;
    }

    private void addSourceFileIfNotEmpty(FileStatus f)
    {
        if (f.isFile()) {
            String filePath = f.getPath().toString();
            if (f.getLen() > 0) {
                addSourceFileByType(filePath);
            }
            else {
                LOG.warn("文件[{}]长度为0，将会跳过不作处理！", filePath);
            }
        }
    }

    public void getHDFSAllFiles(String hdfsPath)
    {

        try {
            FileSystem hdfs = FileSystem.get(hadoopConf);
            //判断hdfsPath是否包含正则符号
            if (hdfsPath.contains("*") || hdfsPath.contains("?")) {
                Path path = new Path(hdfsPath);
                FileStatus[] stats = hdfs.globStatus(path);
                for (FileStatus f : stats) {
                    if (f.isFile()) {
                        addSourceFileIfNotEmpty(f);
                    }
                    else if (f.isDirectory()) {
                        getHDFSAllFilesNORegex(f.getPath().toString(), hdfs);
                    }
                }
            }
            else {
                getHDFSAllFilesNORegex(hdfsPath, hdfs);
            }
        }
        catch (IOException e) {
            String message = String.format("无法读取路径[%s]下的所有文件,请确认您的配置项fs.defaultFS, path的值是否正确，" +
                    "是否有读写权限，网络是否已断开！", hdfsPath);
            LOG.error(message);
            throw AddaxException.asAddaxException(HdfsReaderErrorCode.PATH_CONFIG_ERROR, e);
        }
    }

    private void getHDFSAllFilesNORegex(String path, FileSystem hdfs)
            throws IOException
    {

        // 获取要读取的文件的根目录
        Path listFiles = new Path(path);

        // If the network disconnected, this method will retry 45 times
        // each time the retry interval for 20 seconds
        // 获取要读取的文件的根目录的所有二级子文件目录
        FileStatus[] stats = hdfs.listStatus(listFiles);

        for (FileStatus f : stats) {
            // 判断是不是目录，如果是目录，递归调用
            if (f.isDirectory()) {
                LOG.info("[{}] 是目录, 递归获取该目录下的文件", f.getPath());
                getHDFSAllFilesNORegex(f.getPath().toString(), hdfs);
            }
            else if (f.isFile()) {
                addSourceFileIfNotEmpty(f);
            }
            else {
                String message = String.format("该路径[%s]文件类型既不是目录也不是文件，插件自动忽略。", f.getPath());
                LOG.info(message);
            }
        }
    }

    // 根据用户指定的文件类型，将指定的文件类型的路径加入sourceHDFSAllFilesList
    private void addSourceFileByType(String filePath)
    {
        // 检查file的类型和用户配置的fileType类型是否一致
        boolean isMatchedFileType = checkHdfsFileType(filePath, this.specifiedFileType);

        if (isMatchedFileType) {
            String msg = String.format("[%s]是[%s]类型的文件, 将该文件加入source files列表", filePath, this.specifiedFileType);
            LOG.info(msg);
            sourceHDFSAllFilesList.add(filePath);
        }
        else {
            String message = String.format("文件[%s]的类型与用户配置的fileType类型不一致，" +
                            "请确认您配置的目录下面所有文件的类型均为[%s]"
                    , filePath, this.specifiedFileType);
            LOG.error(message);
            throw AddaxException.asAddaxException(
                    HdfsReaderErrorCode.FILE_TYPE_UNSUPPORTED, message);
        }
    }

    public InputStream getInputStream(String filepath)
    {
        InputStream inputStream;
        Path path = new Path(filepath);
        try {
            FileSystem fs = FileSystem.get(hadoopConf);
            //If the network disconnected, this method will retry 45 times
            //each time the retry interval for 20 seconds
            inputStream = fs.open(path);
            return inputStream;
        }
        catch (IOException e) {
            String message = String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", filepath, filepath);
            throw AddaxException.asAddaxException(HdfsReaderErrorCode.READ_FILE_ERROR, message, e);
        }
    }

    public void sequenceFileStartRead(String sourceSequenceFilePath, Configuration readerSliceConfig,
            RecordSender recordSender, TaskPluginCollector taskPluginCollector)
    {
        LOG.info("Start Read sequence file [{}].", sourceSequenceFilePath);

        Path seqFilePath = new Path(sourceSequenceFilePath);
        try (SequenceFile.Reader reader = new SequenceFile.Reader(this.hadoopConf,
                SequenceFile.Reader.file(seqFilePath))) {
            //获取SequenceFile.Reader实例
            //获取key 与 value
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), this.hadoopConf);
            Text value = new Text();
            while (reader.next(key, value)) {
                if (StringUtils.isNotBlank(value.toString())) {
                    StorageReaderUtil.transportOneRecord(recordSender, readerSliceConfig, taskPluginCollector, value.toString());
                }
            }
        }
        catch (Exception e) {
            String message = String.format("SequenceFile.Reader读取文件[%s]时出错", sourceSequenceFilePath);
            LOG.error(message);
            throw AddaxException.asAddaxException(HdfsReaderErrorCode.READ_SEQUENCE_FILE_ERROR, message, e);
        }
    }

    public void rcFileStartRead(String sourceRcFilePath, Configuration readerSliceConfig,
            RecordSender recordSender, TaskPluginCollector taskPluginCollector)
    {
        LOG.info("Start Read rc-file [{}].", sourceRcFilePath);
        List<ColumnEntry> column = StorageReaderUtil
                .getListColumnEntry(readerSliceConfig, COLUMN);
        // warn: no default value '\N'
        String nullFormat = readerSliceConfig.getString(NULL_FORMAT);

        Path rcFilePath = new Path(sourceRcFilePath);
        RCFileRecordReader recordReader = null;
        try (FileSystem fs = FileSystem.get(rcFilePath.toUri(), hadoopConf)) {
            long fileLen = fs.getFileStatus(rcFilePath).getLen();
            FileSplit split = new FileSplit(rcFilePath, 0, fileLen, (String[]) null);
            recordReader = new RCFileRecordReader(hadoopConf, split);
            LongWritable key = new LongWritable();
            BytesRefArrayWritable value = new BytesRefArrayWritable();
            Text txt = new Text();
            while (recordReader.next(key, value)) {
                String[] sourceLine = new String[value.size()];
                txt.clear();
                for (int i = 0; i < value.size(); i++) {
                    BytesRefWritable v = value.get(i);
                    txt.set(v.getData(), v.getStart(), v.getLength());
                    sourceLine[i] = txt.toString();
                }
                StorageReaderUtil.transportOneRecord(recordSender,
                        column, sourceLine, nullFormat, taskPluginCollector);
            }
        }
        catch (IOException e) {
            String message = String.format("读取文件[%s]时出错", sourceRcFilePath);
            LOG.error(message);
            throw AddaxException.asAddaxException(HdfsReaderErrorCode.READ_RCFILE_ERROR, message, e);
        }
        finally {
            try {
                if (recordReader != null) {
                    recordReader.close();
                    LOG.info("Finally, Close RCFileRecordReader.");
                }
            }
            catch (IOException e) {
                LOG.warn(String.format("finally: 关闭RCFileRecordReader失败, %s", e.getMessage()));
            }
        }
    }

    public void orcFileStartRead(String sourceOrcFilePath, Configuration readerSliceConfig,
            RecordSender recordSender, TaskPluginCollector taskPluginCollector)
    {
        LOG.info("Start Read orc-file [{}].", sourceOrcFilePath);
        List<ColumnEntry> column = StorageReaderUtil.getListColumnEntry(readerSliceConfig, COLUMN);
        String nullFormat = readerSliceConfig.getString(NULL_FORMAT);

        try {
            Path orcFilePath = new Path(sourceOrcFilePath);
            Reader reader = OrcFile.createReader(orcFilePath, OrcFile.readerOptions(hadoopConf));
            TypeDescription schema = reader.getSchema();
            assert column != null;
            if (column.isEmpty()) {
                for (int i = 0; i < schema.getChildren().size(); i++) {
                    ColumnEntry columnEntry = new ColumnEntry();
                    columnEntry.setIndex(i);
                    columnEntry.setType(schema.getChildren().get(i).getCategory().getName());
                    column.add(columnEntry);
                }
            }

            VectorizedRowBatch rowBatch = schema.createRowBatch(1024);
            org.apache.orc.RecordReader rowIterator = reader.rows(reader.options().schema(schema));
            while (rowIterator.nextBatch(rowBatch)) {
                transportOrcRecord(rowBatch, column, recordSender, taskPluginCollector, nullFormat);
            }
        }
        catch (Exception e) {
            String message = String.format("从orc-file文件路径[%s]中读取数据发生异常，请联系系统管理员。"
                    , sourceOrcFilePath);
            LOG.error(message);
            throw AddaxException.asAddaxException(HdfsReaderErrorCode.READ_FILE_ERROR, message);
        }
    }

    private void transportOrcRecord(VectorizedRowBatch rowBatch, List<ColumnEntry> columns, RecordSender recordSender,
            TaskPluginCollector taskPluginCollector, String nullFormat)
    {
        Record record;
        for (int row = 0; row < rowBatch.size; row++) {
            record = recordSender.createRecord();
            try {
                for (ColumnEntry column : columns) {

                    Column columnGenerated;
                    if (column.getValue() != null) {
                        if (!"null".equals(column.getValue())) {
                            columnGenerated = new StringColumn(column.getValue());
                        }
                        else {
                            columnGenerated = new StringColumn();
                        }
                        record.addColumn(columnGenerated);
                        continue;
                    }
                    int i = column.getIndex();
                    String columnType = column.getType().toUpperCase();
                    ColumnVector col = rowBatch.cols[i];
                    Type type = Type.valueOf(columnType);
                    if (col.isNull[row]) {
                        record.addColumn(new StringColumn(null));
                        continue;
                    }
                    switch (type) {
                        case INT:
                        case LONG:
                        case BOOLEAN:
                            columnGenerated = new LongColumn(((LongColumnVector) col).vector[row]);
                            break;
                        case DATE:
                            columnGenerated = new DateColumn(new Date(((LongColumnVector) col).vector[row]));
                            break;
                        case DOUBLE:
                            columnGenerated = new DoubleColumn(((DoubleColumnVector) col).vector[row]);
                            break;
                        case DECIMAL:
                            columnGenerated = new DoubleColumn(((DecimalColumnVector) col).vector[row].doubleValue());
                            break;
                        case BINARY:
                            BytesColumnVector b = (BytesColumnVector) col;
                            byte[] val = Arrays.copyOfRange(b.vector[row], b.start[row], b.start[row] + b.length[row]);
                            columnGenerated = new BytesColumn(val);
                            break;
                        case TIMESTAMP:
                            columnGenerated = new DateColumn(((TimestampColumnVector) col).getTime(row));
                            break;
                        default:
                            // type is string or other
                            String v = ((BytesColumnVector) col).toString(row);
                            columnGenerated = v.equals(nullFormat) ? new StringColumn() : new StringColumn(v);
                            break;
                    }
                    record.addColumn(columnGenerated);
                }
                recordSender.sendToWriter(record);
            }
            catch (Exception e) {
                if (e instanceof AddaxException) {
                    throw (AddaxException) e;
                }
                taskPluginCollector.collectDirtyRecord(record, e.getMessage());
            }
        }
    }

    public void parquetFileStartRead(String sourceParquetFilePath, Configuration readerSliceConfig,
            RecordSender recordSender, TaskPluginCollector taskPluginCollector)
    {
        LOG.info("Start Read orc-file [{}].", sourceParquetFilePath);
        List<ColumnEntry> column = StorageReaderUtil.getListColumnEntry(readerSliceConfig, COLUMN);
        String nullFormat = readerSliceConfig.getString(NULL_FORMAT);
        Path parquetFilePath = new Path(sourceParquetFilePath);

        hadoopConf.set("parquet.avro.readInt96AsFixed", "true");
        JobConf conf = new JobConf(hadoopConf);

        GenericData decimalSupport = new GenericData();
        decimalSupport.addLogicalTypeConversion(new Conversions.DecimalConversion());
        try (ParquetReader<GenericData.Record> reader = AvroParquetReader
                .<GenericData.Record>builder(HadoopInputFile.fromPath(parquetFilePath, hadoopConf))
                .withDataModel(decimalSupport)
                .withConf(conf)
                .build()) {
            GenericData.Record gRecord = reader.read();
            Schema schema = gRecord.getSchema();

            if (null == column || column.isEmpty()) {
                column = new ArrayList<>(schema.getFields().size());
                String sType;
                // 用户没有填写具体的字段信息，需要从parquet文件构建
                for (int i = 0; i < schema.getFields().size(); i++) {
                    ColumnEntry columnEntry = new ColumnEntry();
                    columnEntry.setIndex(i);
                    Schema type;
                    if (schema.getFields().get(i).schema().getType() == Schema.Type.UNION) {
                        type = schema.getFields().get(i).schema().getTypes().get(1);
                    }
                    else {
                        type = schema.getFields().get(i).schema();
                    }
                    sType = type.getProp("logicalType") != null ? type.getProp("logicalType") : type.getType().getName();
                    if (sType.startsWith("timestamp")) {
                        columnEntry.setType("timestamp");
                    }
                    else {
                        columnEntry.setType(sType);
                    }
                    column.add(columnEntry);
                }
            }
            while (gRecord != null) {
                transportOneRecord(column, gRecord, recordSender, taskPluginCollector, nullFormat);
                gRecord = reader.read();
            }
        }
        catch (IOException e) {
            String message = String.format("从parquet file文件路径[%s]中读取数据发生异常，请联系系统管理员。"
                    , sourceParquetFilePath);
            LOG.error(message);
            throw AddaxException.asAddaxException(HdfsReaderErrorCode.READ_FILE_ERROR, message);
        }
    }

    /*
     * create a transport record for Parquet file
     *
     *
     */
    private void transportOneRecord(List<ColumnEntry> columnConfigs, GenericData.Record gRecord, RecordSender recordSender,
            TaskPluginCollector taskPluginCollector, String nullFormat)
    {
        Record record = recordSender.createRecord();
        Column columnGenerated;
        int scale = 10;
        try {
            for (ColumnEntry columnEntry : columnConfigs) {
                String columnType = columnEntry.getType();
                Integer columnIndex = columnEntry.getIndex();
                String columnConst = columnEntry.getValue();
                String columnValue = null;
                if (null != columnIndex) {
                    if (null != gRecord.get(columnIndex)) {
                        columnValue = gRecord.get(columnIndex).toString();
                    }
                    else {
                        record.addColumn(new StringColumn(null));
                        continue;
                    }
                }
                else {
                    columnValue = columnConst;
                }
                if (columnType.startsWith("decimal(")) {
                    String ps = columnType.replace("decimal(", "").replace(")", "");
                    columnType = "decimal";
                    if (ps.contains(",")) {
                        scale = Integer.parseInt(ps.split(",")[1].trim());
                    }
                    else {
                        scale = 0;
                    }
                }
                Type type = Type.valueOf(columnType.toUpperCase());
                if (StringUtils.equals(columnValue, nullFormat)) {
                    columnValue = null;
                }
                try {
                    switch (type) {
                        case STRING:
                            columnGenerated = new StringColumn(columnValue);
                            break;
                        case INT:
                        case LONG:
                            columnGenerated = new LongColumn(columnValue);
                            break;
                        case DOUBLE:
                            columnGenerated = new DoubleColumn(columnValue);
                            break;
                        case DECIMAL:
                            if (null == columnValue) {
                                columnGenerated = new DoubleColumn((Double) null);
                            }
                            else {
                                columnGenerated = new DoubleColumn(new BigDecimal(columnValue).setScale(scale, RoundingMode.HALF_UP));
                            }
                            break;
                        case BOOLEAN:
                            columnGenerated = new BoolColumn(columnValue);
                            break;
                        case DATE:
                            if (columnValue == null) {
                                columnGenerated = new DateColumn((Date) null);
                            }
                            else {
                                String formatString = columnEntry.getFormat();
                                if (StringUtils.isNotBlank(formatString)) {
                                    // 用户自己配置的格式转换
                                    SimpleDateFormat format = new SimpleDateFormat(
                                            formatString);
                                    columnGenerated = new DateColumn(
                                            format.parse(columnValue));
                                }
                                else {
                                    // 框架尝试转换
                                    columnGenerated = new DateColumn(new StringColumn(columnValue).asDate());
                                }
                            }
                            break;
                        case TIMESTAMP:
                            if (null == columnValue) {
                                columnGenerated = new DateColumn();
                            }
                            else if (columnValue.startsWith("[")) {
                                // INT96 https://github.com/apache/parquet-mr/pull/901
                                GenericData.Fixed fixed = (GenericData.Fixed) gRecord.get(columnIndex);
                                Date date = new Date(getTimestampMills(fixed.bytes()));
                                columnGenerated = new DateColumn(date);
                            }
                            else {
                                columnGenerated = new DateColumn(Long.parseLong(columnValue) * 1000);
                            }
                            break;
                        case BINARY:
                            columnGenerated = new BytesColumn(((ByteBuffer) gRecord.get(columnIndex)).array());
                            break;
                        default:
                            String errorMessage = String.format("您配置的列类型暂不支持 : [%s]", columnType);
                            LOG.error(errorMessage);
                            throw AddaxException.asAddaxException(StorageReaderErrorCode.NOT_SUPPORT_TYPE, errorMessage);
                    }
                }
                catch (Exception e) {
                    throw new IllegalArgumentException(String.format(
                            "类型转换错误, 无法将[%s] 转换为[%s], %s", columnValue, type, e));
                }
                record.addColumn(columnGenerated);
            } // end for

            recordSender.sendToWriter(record);
        }
        catch (IllegalArgumentException | IndexOutOfBoundsException iae) {
            taskPluginCollector.collectDirtyRecord(record, iae.getMessage());
        }
        catch (Exception e) {
            if (e instanceof AddaxException) {
                throw (AddaxException) e;
            }
            // 每一种转换失败都是脏数据处理,包括数字格式 & 日期格式
            taskPluginCollector.collectDirtyRecord(record, e.getMessage());
        }
    }

    private TypeDescription getOrcSchema(String filePath)
    {
        Path path = new Path(filePath);
        try {
            Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(hadoopConf));
//            return reader.getTypes().get(0).getSubtypesCount()
            return reader.getSchema();
        }
        catch (IOException e) {
            String message = "读取orc-file column列数失败，请联系系统管理员";
            throw AddaxException.asAddaxException(HdfsReaderErrorCode.READ_FILE_ERROR, message);
        }
    }

    public boolean checkHdfsFileType(String filepath, String specifiedFileType)
    {

        Path file = new Path(filepath);

        try (FileSystem fs = FileSystem.get(hadoopConf); FSDataInputStream in = fs.open(file)) {
            if (StringUtils.equalsIgnoreCase(specifiedFileType, HdfsConstant.ORC)) {
                return isORCFile(file, fs, in);
            }
            else if (StringUtils.equalsIgnoreCase(specifiedFileType, HdfsConstant.RC)) {
                return isRCFile(filepath, in);
            }
            else if (StringUtils.equalsIgnoreCase(specifiedFileType, HdfsConstant.SEQ)) {

                return isSequenceFile(file, in);
            }
            else if (StringUtils.equalsIgnoreCase(specifiedFileType, HdfsConstant.PARQUET)) {
                return isParquetFile(file);
            }
            else if (StringUtils.equalsIgnoreCase(specifiedFileType, HdfsConstant.CSV)
                    || StringUtils.equalsIgnoreCase(specifiedFileType, HdfsConstant.TEXT)) {
                return true;
            }
        }
        catch (Exception e) {
            String message = String.format("检查文件[%s]类型失败，目前支持 %s 格式的文件," +
                    "请检查您文件类型和文件是否正确。", filepath, HdfsConstant.SUPPORT_FILE_TYPE);
            LOG.error(message);
            throw AddaxException.asAddaxException(HdfsReaderErrorCode.READ_FILE_ERROR, message, e);
        }
        return false;
    }

    // 判断file是否是ORC File
    private boolean isORCFile(Path file, FileSystem fs, FSDataInputStream in)
    {
        try {
            // figure out the size of the file using the option or filesystem
            long size = fs.getFileStatus(file).getLen();

            //read last bytes into buffer to get PostScript
            int readSize = (int) Math.min(size, DIRECTORY_SIZE_GUESS);
            in.seek(size - readSize);
            ByteBuffer buffer = ByteBuffer.allocate(readSize);
            in.readFully(buffer.array(), buffer.arrayOffset() + buffer.position(),
                    buffer.remaining());

            //read the PostScript
            //get length of PostScript
            int psLen = buffer.get(readSize - 1) & 0xff;
            String orcMagic = org.apache.orc.OrcFile.MAGIC;
            int len = orcMagic.length();
            if (psLen < len + 1) {
                return false;
            }
            int offset = buffer.arrayOffset() + buffer.position() + buffer.limit() - 1
                    - len;
            byte[] array = buffer.array();
            // now look for the magic string at the end of the postscript.
            if (Text.decode(array, offset, len).equals(orcMagic)) {
                return true;
            }
            else {
                // If it isn't there, this may be the 0.11.0 version of ORC.
                // Read the first 3 bytes of the file to check for the header
                in.seek(0);
                byte[] header = new byte[len];
                in.readFully(header, 0, len);
                // if it isn't there, this isn't an ORC file
                if (Text.decode(header, 0, len).equals(orcMagic)) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            LOG.info("检查文件类型: [{}] 不是ORC File.", file);
        }
        return false;
    }

    // 判断file是否是RC file
    private boolean isRCFile(String filepath, FSDataInputStream in)
    {

        // The first version of RCFile used the sequence file header.
        final byte[] originalMagic = {(byte) 'S', (byte) 'E', (byte) 'Q'};
        // The 'magic' bytes at the beginning of the RCFile
        final byte[] rcMagic = {(byte) 'R', (byte) 'C', (byte) 'F'};
        // the version that was included with the original magic, which is mapped
        // into ORIGINAL_VERSION
        final byte ORIGINAL_MAGIC_VERSION_WITH_METADATA = 6;
        // All the versions should be place in this list.
        final int ORIGINAL_VERSION = 0;  // version with SEQ
        // version with RCF
        // final int NEW_MAGIC_VERSION = 1
        // final int CURRENT_VERSION = NEW_MAGIC_VERSION
        final int CURRENT_VERSION = 1;
        byte version;

        byte[] magic = new byte[rcMagic.length];
        try {
            in.seek(0);
            in.readFully(magic);

            if (Arrays.equals(magic, originalMagic)) {
                if (in.readByte() != ORIGINAL_MAGIC_VERSION_WITH_METADATA) {
                    return false;
                }
                version = ORIGINAL_VERSION;
            }
            else {
                if (!Arrays.equals(magic, rcMagic)) {
                    return false;
                }

                // Set 'version'
                version = in.readByte();
                if (version > CURRENT_VERSION) {
                    return false;
                }
            }

            if (version == ORIGINAL_VERSION) {
                try {
                    Class<?> keyCls = hadoopConf.getClassByName(Text.readString(in));
                    Class<?> valCls = hadoopConf.getClassByName(Text.readString(in));
                    if (!keyCls.equals(RCFile.KeyBuffer.class) || !valCls.equals(RCFile.ValueBuffer.class)) {
                        return false;
                    }
                }
                catch (ClassNotFoundException e) {
                    return false;
                }
            }
//            boolean decompress = in.readBoolean(); // is compressed?
            if (version == ORIGINAL_VERSION) {
                // is block-compressed? it should be always false.
                boolean blkCompressed = in.readBoolean();
                return !blkCompressed;
            }
            return true;
        }
        catch (IOException e) {
            LOG.info("检查文件类型: [{}] 不是RC File.", filepath);
        }
        return false;
    }

    // 判断file是否是Sequence file
    private boolean isSequenceFile(Path filepath, FSDataInputStream in)
    {
        final byte[] seqMagic = {(byte) 'S', (byte) 'E', (byte) 'Q'};
        byte[] magic = new byte[seqMagic.length];
        try {
            in.seek(0);
            in.readFully(magic);
            return Arrays.equals(magic, seqMagic);
        }
        catch (IOException e) {
            LOG.info("检查文件类型: [{}] 不是Sequence File.", filepath);
        }
        return false;
    }

    //判断是否为parquet
    private boolean isParquetFile(Path file)
    {
        try {
            GroupReadSupport readSupport = new GroupReadSupport();
            ParquetReader.Builder<Group> reader = ParquetReader.builder(readSupport, file);
            ParquetReader<Group> build = reader.build();
            if (build.read() != null) {
                return true;
            }
        }
        catch (IOException e) {
            LOG.info("检查文件类型: [{}] 不是Parquet File.", file);
        }
        return false;
    }

    /**
     * Returns GMT's timestamp from binary encoded parquet timestamp (12 bytes - julian date + time of day nanos).
     *
     * @param timestampBinary INT96 parquet timestamp
     * @return timestamp in millis, GMT timezone
     */
    public static long getTimestampMillis(Binary timestampBinary)
    {
        if (timestampBinary.length() != 12) {
            return 0;
        }
        byte[] bytes = timestampBinary.getBytes();

        return getTimestampMills(bytes);
    }

    public static long getTimestampMills(byte[] bytes)
    {
        assert bytes.length == 12;
        // little endian encoding - need to invert byte order
        long timeOfDayNanos = Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
        int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);

        return julianDayToMillis(julianDay) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
    }

    private static long julianDayToMillis(int julianDay)
    {
        return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;
    }

    private enum Type
    {
        INT, STRING, LONG, BOOLEAN, DOUBLE, DATE, BINARY, TIMESTAMP, DECIMAL,
    }
}
