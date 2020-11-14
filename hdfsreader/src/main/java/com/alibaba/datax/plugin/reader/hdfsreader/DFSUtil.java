package com.alibaba.datax.plugin.reader.hdfsreader;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.ColumnEntry;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderErrorCode;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileRecordReader;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Created by mingya.wmy on 2015/8/12.
 */
public class DFSUtil
{
    public static final String HDFS_DEFAULTFS_KEY = "fs.defaultFS";
    public static final String HADOOP_SECURITY_AUTHENTICATION_KEY = "hadoop.security.authentication";
    private static final Logger LOG = LoggerFactory.getLogger(DFSUtil.class);
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
        hadoopConf.set(HDFS_DEFAULTFS_KEY, taskConfig.getString(Key.DEFAULT_FS));

        //是否有Kerberos认证
        this.haveKerberos = taskConfig.getBool(Key.HAVE_KERBEROS, false);
        if (haveKerberos) {
            this.kerberosKeytabFilePath = taskConfig.getString(Key.KERBEROS_KEYTAB_FILE_PATH);
            this.kerberosPrincipal = taskConfig.getString(Key.KERBEROS_PRINCIPAL);
            this.hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION_KEY, "kerberos");
        }
        this.kerberosAuthentication(this.kerberosPrincipal, this.kerberosKeytabFilePath);

        LOG.info("hadoopConfig details:{}", JSON.toJSONString(this.hadoopConf));
    }

    public static void main(String[] args)
    {
        ParquetReader<GenericData.Record> reader = null;
        Path path = new Path("/tmp/000000_0");
        try {
            GenericData decimalSupport = new GenericData();
            decimalSupport.addLogicalTypeConversion(new Conversions.DecimalConversion());
            reader = AvroParquetReader
                    .<GenericData.Record>builder(path)
                    .withDataModel(decimalSupport)
                    .withConf(new org.apache.hadoop.conf.Configuration())
                    .build();
            GenericData.Record record = reader.read();
            while (record != null) {
                System.out.println(record);
                record = reader.read();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
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
                throw DataXException.asDataXException(HdfsReaderErrorCode.KERBEROS_LOGIN_ERROR, message, e);
            }
        }
    }

    /**
     * 获取指定路径列表下符合条件的所有文件的绝对路径
     *
     * @param srcPaths 路径列表
     * @param specifiedFileType 指定文件类型
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
            throw DataXException.asDataXException(HdfsReaderErrorCode.PATH_CONFIG_ERROR, e);
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
            throw DataXException.asDataXException(
                    HdfsReaderErrorCode.FILE_TYPE_UNSUPPORT, message);
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
            throw DataXException.asDataXException(HdfsReaderErrorCode.READ_FILE_ERROR, message, e);
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
                    UnstructuredStorageReaderUtil.transportOneRecord(recordSender,
                            readerSliceConfig, taskPluginCollector, value.toString());
                }
            }
        }
        catch (Exception e) {
            String message = String.format("SequenceFile.Reader读取文件[%s]时出错", sourceSequenceFilePath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsReaderErrorCode.READ_SEQUENCEFILE_ERROR, message, e);
        }
    }

    public void rcFileStartRead(String sourceRcFilePath, Configuration readerSliceConfig,
            RecordSender recordSender, TaskPluginCollector taskPluginCollector)
    {
        LOG.info("Start Read rcfile [{}].", sourceRcFilePath);
        List<ColumnEntry> column = UnstructuredStorageReaderUtil
                .getListColumnEntry(readerSliceConfig, com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
        // warn: no default value '\N'
        String nullFormat = readerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.NULL_FORMAT);

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
                UnstructuredStorageReaderUtil.transportOneRecord(recordSender,
                        column, sourceLine, nullFormat, taskPluginCollector);
            }
        }
        catch (IOException e) {
            String message = String.format("读取文件[%s]时出错", sourceRcFilePath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsReaderErrorCode.READ_RCFILE_ERROR, message, e);
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
        LOG.info("Start Read orcfile [{}].", sourceOrcFilePath);
        List<ColumnEntry> column = UnstructuredStorageReaderUtil
                .getListColumnEntry(readerSliceConfig, com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
        String nullFormat = readerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.NULL_FORMAT);
        StringBuilder allColumns = new StringBuilder();
        StringBuilder allColumnTypes = new StringBuilder();
        boolean isReadAllColumns = false;
        int columnIndexMax;
        // 判断是否读取所有列
        if (null == column || column.isEmpty()) {
            int allColumnsCount = getAllColumnsCount(sourceOrcFilePath);
            columnIndexMax = allColumnsCount - 1;
            isReadAllColumns = true;
        }
        else {
            columnIndexMax = getMaxIndex(column);
        }
        for (int i = 0; i <= columnIndexMax; i++) {
            allColumns.append("col");
            allColumnTypes.append("string");
            if (i != columnIndexMax) {
                allColumns.append(",");
                allColumnTypes.append(":");
            }
        }
        if (columnIndexMax >= 0) {
            JobConf conf = new JobConf(hadoopConf);
            Path orcFilePath = new Path(sourceOrcFilePath);
            Properties p = new Properties();
            p.setProperty("columns", allColumns.toString());
            p.setProperty("columns.types", allColumnTypes.toString());
            try {
                OrcSerde serde = new OrcSerde();
                serde.initialize(conf, p);
                StructObjectInspector inspector = (StructObjectInspector) serde.getObjectInspector();
                InputFormat<?, ?> in = new OrcInputFormat();
                FileInputFormat.setInputPaths(conf, orcFilePath.toString());

                //If the network disconnected, will retry 45 times, each time the retry interval for 20 seconds
                //Each file as a split

                // OrcInputFormat getSplits params numSplits not used, splits size = block numbers
                InputSplit[] splits = in.getSplits(conf, -1);
                for (InputSplit split : splits) {
                    {
                        RecordReader reader = in.getRecordReader(split, conf, Reporter.NULL);
                        Object key = reader.createKey();
                        Object value = reader.createValue();
                        // 获取列信息
                        List<? extends StructField> fields = inspector.getAllStructFieldRefs();

                        List<Object> recordFields;
                        while (reader.next(key, value)) {
                            recordFields = new ArrayList<>();

                            for (int i = 0; i <= columnIndexMax; i++) {
                                Object field = inspector.getStructFieldData(value, fields.get(i));
                                recordFields.add(field);
                            }
                            transportOneRecord(column, recordFields, recordSender,
                                    taskPluginCollector, isReadAllColumns, nullFormat);
                        }
                        reader.close();
                    }
                }
            }
            catch (Exception e) {
                String message = String.format("从orcfile文件路径[%s]中读取数据发生异常，请联系系统管理员。"
                        , sourceOrcFilePath);
                LOG.error(message);
                throw DataXException.asDataXException(HdfsReaderErrorCode.READ_FILE_ERROR, message);
            }
        }
        else {
            String message = String.format("请确认您所读取的列配置正确！columnIndexMax 小于0,column:%s", JSON.toJSONString(column));
            throw DataXException.asDataXException(HdfsReaderErrorCode.BAD_CONFIG_VALUE, message);
        }
    }

    public void parquetFileStartRead(String sourceParquestFilePath, Configuration readerSliceConfig,
            RecordSender recordSender, TaskPluginCollector taskPluginCollector)
    {
        LOG.info("Start Read orcfile [{}].", sourceParquestFilePath);
        List<ColumnEntry> column = UnstructuredStorageReaderUtil
                .getListColumnEntry(readerSliceConfig, com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
        String nullFormat = readerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.NULL_FORMAT);
        boolean isReadAllColumns = false;
        int columnIndexMax;
        // 判断是否读取所有列
        if (null == column || column.isEmpty()) {
            int allColumnsCount = getAllColumnsCount(sourceParquestFilePath);
            columnIndexMax = allColumnsCount - 1;
            isReadAllColumns = true;
        }
        else {
            columnIndexMax = getMaxIndex(column);
        }
        if (columnIndexMax >= 0) {
            JobConf conf = new JobConf(hadoopConf);
            Path parquetFilePath = new Path(sourceParquestFilePath);
            GenericData decimalSupport = new GenericData();
            decimalSupport.addLogicalTypeConversion(new Conversions.DecimalConversion());
            try (ParquetReader<GenericData.Record> reader = AvroParquetReader
                    .<GenericData.Record>builder(parquetFilePath)
                    .withDataModel(decimalSupport)
                    .withConf(conf)
                    .build()) {
                GenericData.Record gRecord = reader.read();
                Schema schema = gRecord.getSchema();
                while (gRecord != null) {
                    transportOneRecord(column, gRecord, schema, recordSender, taskPluginCollector, isReadAllColumns, nullFormat
                    );
                    gRecord = reader.read();
                }
            }
            catch (IOException e) {
                String message = String.format("从parquetfile文件路径[%s]中读取数据发生异常，请联系系统管理员。"
                        , sourceParquestFilePath);
                LOG.error(message);
                throw DataXException.asDataXException(HdfsReaderErrorCode.READ_FILE_ERROR, message);
            }
        }
        else {
            String message = String.format("请确认您所读取的列配置正确！columnIndexMax 小于0,column:%s", JSON.toJSONString(column));
            throw DataXException.asDataXException(HdfsReaderErrorCode.BAD_CONFIG_VALUE, message);
        }
    }

    /*
     * create a transport record for Parquet file
     *
     *
     */
    private void transportOneRecord(List<ColumnEntry> columnConfigs, GenericData.Record gRecord, Schema schema, RecordSender recordSender,
            TaskPluginCollector taskPluginCollector, boolean isReadAllColumns, String nullFormat)
    {
        Record record = recordSender.createRecord();
        Column columnGenerated;
        try {
            if (isReadAllColumns) {
                for (int i = 0; i < schema.getFields().size(); i++) {
                    record.addColumn(new StringColumn((String) gRecord.get(i)));
                }
            }
            else {
                for (ColumnEntry columnEntry : columnConfigs) {
                    String columnType = columnEntry.getType();
                    Integer columnIndex = columnEntry.getIndex();
                    String columnConst = columnEntry.getValue();
                    String columnValue = null;
                    if (null != columnIndex) {
                        if (null != gRecord.get(columnIndex)) {
                            columnValue = gRecord.get(columnIndex).toString();
                        }
                    }
                    else {
                        columnValue = columnConst;
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
                            case LONG:
                                columnGenerated = new LongColumn(columnValue);
                                break;
                            case DOUBLE:
                                columnGenerated = new DoubleColumn(columnValue);
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
                                        columnGenerated = new DateColumn(
                                                new StringColumn(columnValue)
                                                        .asDate());
                                    }
                                }
                                break;
                            default:
                                String errorMessage = String.format(
                                        "您配置的列类型暂不支持 : [%s]", columnType);
                                LOG.error(errorMessage);
                                throw DataXException
                                        .asDataXException(
                                                UnstructuredStorageReaderErrorCode.NOT_SUPPORT_TYPE,
                                                errorMessage);
                        }
                    }
                    catch (Exception e) {
                        throw new IllegalArgumentException(String.format(
                                "类型转换错误, 无法将[%s] 转换为[%s]", columnValue, type));
                    }
                    record.addColumn(columnGenerated);
                } // end for
            } // end else
            recordSender.sendToWriter(record);
        }
        catch (IllegalArgumentException | IndexOutOfBoundsException iae) {
            taskPluginCollector
                    .collectDirtyRecord(record, iae.getMessage());
        }
        catch (Exception e) {
            if (e instanceof DataXException) {
                throw (DataXException) e;
            }
            // 每一种转换失败都是脏数据处理,包括数字格式 & 日期格式
            taskPluginCollector.collectDirtyRecord(record, e.getMessage());
        }
    }

    private void transportOneRecord(List<ColumnEntry> columnConfigs, List<Object> recordFields
            , RecordSender recordSender, TaskPluginCollector taskPluginCollector, boolean isReadAllColumns, String nullFormat)
    {
        Record record = recordSender.createRecord();
        Column columnGenerated;
        try {
            if (isReadAllColumns) {
                // 读取所有列，创建都为String类型的column
                for (Object recordField : recordFields) {
                    String columnValue = null;
                    if (recordField != null) {
                        columnValue = recordField.toString();
                    }
                    columnGenerated = new StringColumn(columnValue);
                    record.addColumn(columnGenerated);
                }
            }
            else {
                for (ColumnEntry columnConfig : columnConfigs) {
                    String columnType = columnConfig.getType();
                    Integer columnIndex = columnConfig.getIndex();
                    String columnConst = columnConfig.getValue();

                    String columnValue = null;
                    if (null != columnIndex) {
                        if (null != recordFields.get(columnIndex)) {
                            columnValue = recordFields.get(columnIndex).toString();
                        }
                    }
                    else {
                        columnValue = columnConst;
                    }
                    Type type = Type.valueOf(columnType.toUpperCase());
                    // it's all ok if nullFormat is null
                    if (StringUtils.equals(columnValue, nullFormat)) {
                        columnValue = null;
                    }
                    try {
                        switch (type) {
                            case STRING:
                                columnGenerated = new StringColumn(columnValue);
                                break;
                            case LONG:
                                columnGenerated = new LongColumn(columnValue);
                                break;
                            case DOUBLE:
                                columnGenerated = new DoubleColumn(columnValue);
                                break;
                            case BOOLEAN:
                                columnGenerated = new BoolColumn(columnValue);
                                break;
                            case DATE:
                                if (columnValue == null) {
                                    columnGenerated = new DateColumn((Date) null);
                                }
                                else {
                                    String formatString = columnConfig.getFormat();
                                    if (StringUtils.isNotBlank(formatString)) {
                                        // 用户自己配置的格式转换
                                        SimpleDateFormat format = new SimpleDateFormat(
                                                formatString);
                                        columnGenerated = new DateColumn(
                                                format.parse(columnValue));
                                    }
                                    else {
                                        // 框架尝试转换
                                        columnGenerated = new DateColumn(
                                                new StringColumn(columnValue)
                                                        .asDate());
                                    }
                                }
                                break;
                            default:
                                String errorMessage = String.format(
                                        "您配置的列类型暂不支持 : [%s]", columnType);
                                LOG.error(errorMessage);
                                throw DataXException
                                        .asDataXException(
                                                UnstructuredStorageReaderErrorCode.NOT_SUPPORT_TYPE,
                                                errorMessage);
                        }
                    }
                    catch (Exception e) {
                        throw new IllegalArgumentException(String.format(
                                "类型转换错误, 无法将[%s] 转换为[%s]", columnValue, type));
                    }

                    record.addColumn(columnGenerated);
                }
            }
            recordSender.sendToWriter(record);
        }
        catch (IllegalArgumentException | IndexOutOfBoundsException iae) {
            taskPluginCollector
                    .collectDirtyRecord(record, iae.getMessage());
        }
        catch (Exception e) {
            if (e instanceof DataXException) {
                throw (DataXException) e;
            }
            // 每一种转换失败都是脏数据处理,包括数字格式 & 日期格式
            taskPluginCollector.collectDirtyRecord(record, e.getMessage());
        }

//        return record;
    }

    private int getAllColumnsCount(String filePath)
    {
        Path path = new Path(filePath);
        try {
            Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(hadoopConf));
//            return reader.getTypes().get(0).getSubtypesCount();
            return reader.getSchema().getChildren().size();
        }
        catch (IOException e) {
            String message = "读取orcfile column列数失败，请联系系统管理员";
            throw DataXException.asDataXException(HdfsReaderErrorCode.READ_FILE_ERROR, message);
        }
    }

    private int getMaxIndex(List<ColumnEntry> columnConfigs)
    {
        int maxIndex = -1;
        for (ColumnEntry columnConfig : columnConfigs) {
            Integer columnIndex = columnConfig.getIndex();
            if (columnIndex != null && columnIndex < 0) {
                String message = String.format("您column中配置的index不能小于0，请修改为正确的index,column配置:%s",
                        JSON.toJSONString(columnConfigs));
                LOG.error(message);
                throw DataXException.asDataXException(HdfsReaderErrorCode.CONFIG_INVALID_EXCEPTION, message);
            }
            else if (columnIndex != null && columnIndex > maxIndex) {
                maxIndex = columnIndex;
            }
        }
        return maxIndex;
    }

    public boolean checkHdfsFileType(String filepath, String specifiedFileType)
    {

        Path file = new Path(filepath);

        try (FileSystem fs = FileSystem.get(hadoopConf); FSDataInputStream in = fs.open(file)) {

            if (StringUtils.equalsIgnoreCase(specifiedFileType, Constant.CSV)
                    || StringUtils.equalsIgnoreCase(specifiedFileType, Constant.TEXT)) {

                boolean isORC = isORCFile(file, fs, in);// 判断是否是 ORC File
                if (isORC) {
                    return false;
                }
                boolean isRC = isRCFile(filepath, in);// 判断是否是 RC File
                if (isRC) {
                    return false;
                }
                boolean isSEQ = isSequenceFile(file, in);// 判断是否是 Sequence File
                if (isSEQ) {
                    return false;
                }
                boolean isParquet = isParquetFile(file, in); //判断是否为Parquet File
                // 如果不是ORC,RC,PARQUET,SEQ,则默认为是TEXT或CSV类型
                return !isParquet;
            }
            else if (StringUtils.equalsIgnoreCase(specifiedFileType, Constant.ORC)) {

                return isORCFile(file, fs, in);
            }
            else if (StringUtils.equalsIgnoreCase(specifiedFileType, Constant.RC)) {

                return isRCFile(filepath, in);
            }
            else if (StringUtils.equalsIgnoreCase(specifiedFileType, Constant.SEQ)) {

                return isSequenceFile(file, in);
            }
            else if (StringUtils.equalsIgnoreCase(specifiedFileType, Constant.PARQUET)) {
                return isParquetFile(file, in);
            }
        }
        catch (Exception e) {
            String message = String.format("检查文件[%s]类型失败，目前支持ORC,SEQUENCE,RCFile,TEXT,CSV五种格式的文件," +
                    "请检查您文件类型和文件是否正确。", filepath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsReaderErrorCode.READ_FILE_ERROR, message, e);
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
        byte[] ORIGINAL_MAGIC = {(byte) 'S', (byte) 'E', (byte) 'Q'};
        // The 'magic' bytes at the beginning of the RCFile
        byte[] RC_MAGIC = {(byte) 'R', (byte) 'C', (byte) 'F'};
        // the version that was included with the original magic, which is mapped
        // into ORIGINAL_VERSION
        final byte ORIGINAL_MAGIC_VERSION_WITH_METADATA = 6;
        // All of the versions should be place in this list.
        final int ORIGINAL_VERSION = 0;  // version with SEQ
        // version with RCF
        // final int NEW_MAGIC_VERSION = 1;
        // final int CURRENT_VERSION = NEW_MAGIC_VERSION;
        final int CURRENT_VERSION = 1;
        byte version;

        byte[] magic = new byte[RC_MAGIC.length];
        try {
            in.seek(0);
            in.readFully(magic);

            if (Arrays.equals(magic, ORIGINAL_MAGIC)) {
                byte vers = in.readByte();
                if (vers != ORIGINAL_MAGIC_VERSION_WITH_METADATA) {
                    return false;
                }
                version = ORIGINAL_VERSION;
            }
            else {
                if (!Arrays.equals(magic, RC_MAGIC)) {
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
                    if (!keyCls.equals(RCFile.KeyBuffer.class)
                            || !valCls.equals(RCFile.ValueBuffer.class)) {
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
        byte[] SEQ_MAGIC = {(byte) 'S', (byte) 'E', (byte) 'Q'};
        byte[] magic = new byte[SEQ_MAGIC.length];
        try {
            in.seek(0);
            in.readFully(magic);
            return Arrays.equals(magic, SEQ_MAGIC);
        }
        catch (IOException e) {
            LOG.info("检查文件类型: [{}] 不是Sequence File.", filepath);
        }
        return false;
    }

    //判断是否为parquet
    private boolean isParquetFile(Path file, FSDataInputStream in)
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

    private enum Type
    {
        STRING, LONG, BOOLEAN, DOUBLE, DATE,
    }
}
