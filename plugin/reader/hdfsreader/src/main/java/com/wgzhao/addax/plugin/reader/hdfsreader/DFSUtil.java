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

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.BytesColumn;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.ColumnEntry;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.element.TimestampColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.storage.reader.StorageReaderUtil;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.wgzhao.addax.common.base.Key.COLUMN;
import static com.wgzhao.addax.common.base.Key.NULL_FORMAT;
import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.common.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.LOGIN_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.NOT_SUPPORT_TYPE;

/**
 * Created by mingya.wmy on 2015/8/12.
 */
public class DFSUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(DFSUtil.class);

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
            // fix Failed to specify server's Kerberos principal name
            if (Objects.equals(hadoopConf.get("dfs.namenode.kerberos.principal", ""), "")) {
                // get REALM
                String serverPrincipal = "nn/_HOST@" + Iterables.get(Splitter.on('@').split(this.kerberosPrincipal), 1);
                hadoopConf.set("dfs.namenode.kerberos.principal", serverPrincipal);
            }
        }
        this.kerberosAuthentication(this.kerberosPrincipal, this.kerberosKeytabFilePath);

        LOG.debug("hadoopConfig details:{}", JSON.toJSONString(this.hadoopConf));
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
                throw AddaxException.asAddaxException(LOGIN_ERROR, message, e);
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
                LOG.warn("It will ignore file [{}] because it is empty.", filePath);
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
            LOG.error("IO exception occurred while reading file(s) under [{}].", hdfsPath);
            throw AddaxException.asAddaxException(CONFIG_ERROR, e);
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
                LOG.info("The [{}] is directory, reading all files in the directory.", f.getPath());
                getHDFSAllFilesNORegex(f.getPath().toString(), hdfs);
            }
            else if (f.isFile()) {
                addSourceFileIfNotEmpty(f);
            }
            else {
                String message = String.format("The [%s] neither directory nor file,ignore it.", f.getPath());
                LOG.info(message);
            }
        }
    }

    // 根据用户指定的文件类型，将指定的文件类型的路径加入sourceHDFSAllFilesList
    private void addSourceFileByType(String filePath)
    {
        // 检查file的类型和用户配置的fileType类型是否一致
        boolean isMatchedFileType = FileTypeUtils.checkHdfsFileType(hadoopConf, filePath, this.specifiedFileType);

        if (isMatchedFileType) {
            LOG.info("The file [{}] format is [{}], add it to source files list.", filePath, this.specifiedFileType);
            sourceHDFSAllFilesList.add(filePath);
        }
        else {
            String message = String.format("The file [%s] format is not the same of [%s] you configured."
                    , filePath, this.specifiedFileType);
            LOG.error(message);
            throw AddaxException.asAddaxException(
                    NOT_SUPPORT_TYPE, message);
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
            String message = String.format("IO exception occurred while reading the file [%s].", filepath);
            throw AddaxException.asAddaxException(IO_ERROR, message, e);
        }
    }

    public void sequenceFileStartRead(String sourceSequenceFilePath, Configuration readerSliceConfig,
            RecordSender recordSender, TaskPluginCollector taskPluginCollector)
    {
        LOG.info("Begin to read the sequence file [{}].", sourceSequenceFilePath);

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
            String message = String.format("Exception occurred while reading the file [%s].", sourceSequenceFilePath);
            LOG.error(message);
            throw AddaxException.asAddaxException(EXECUTE_FAIL, message, e);
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
            String message = String.format("IO exception occurred while reading the file [%s].", sourceRcFilePath);
            LOG.error(message);
            throw AddaxException.asAddaxException(IO_ERROR, message, e);
        }
        finally {
            try {
                if (recordReader != null) {
                    recordReader.close();
                    LOG.info("Finally, Close RCFileRecordReader.");
                }
            }
            catch (IOException e) {
                LOG.warn(String.format("Failed to close RCFileRecordReader: %s", e.getMessage()));
            }
        }
    }

    public void orcFileStartRead(String sourceOrcFilePath, Configuration readerSliceConfig,
            RecordSender recordSender, TaskPluginCollector taskPluginCollector)
    {
        LOG.info("Being to read the orc-file [{}].", sourceOrcFilePath);
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
            String message = String.format("Exception occurred while reading the file [%s].", sourceOrcFilePath);
            LOG.error(message);
            throw AddaxException.asAddaxException(IO_ERROR, message);
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
                    JavaType type = JavaType.valueOf(columnType);
                    if (col.isNull[row]) {
                        record.addColumn(new StringColumn(null));
                        continue;
                    }
                    switch (type) {
                        case INT:
                        case LONG:
                        case BOOLEAN:
                        case BIGINT:
                            columnGenerated = new LongColumn(((LongColumnVector) col).vector[row]);
                            break;
                        case DATE:
                            columnGenerated = new DateColumn(new Date(((LongColumnVector) col).vector[row]));
                            break;
                        case FLOAT:
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
                            // FIXME: incorrect timezone value
                            columnGenerated = new TimestampColumn(((TimestampColumnVector) col).getTime(row));
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
        LOG.info("Begin to read the parquet-file [{}].", sourceParquetFilePath);
        String nullFormat = readerSliceConfig.getString(NULL_FORMAT);
        List<ColumnEntry> column = StorageReaderUtil.getListColumnEntry(readerSliceConfig, COLUMN);
        Path parquetFilePath = new Path(sourceParquetFilePath);
        MyParquetReader myParquetReader = new MyParquetReader(hadoopConf, parquetFilePath, nullFormat, column);

        myParquetReader.reader(recordSender, taskPluginCollector);
    }

    private TypeDescription getOrcSchema(String filePath)
    {
        Path path = new Path(filePath);
        try {
            Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(hadoopConf));
            return reader.getSchema();
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "IO exception occurred when reading orc file");
        }
    }
}
