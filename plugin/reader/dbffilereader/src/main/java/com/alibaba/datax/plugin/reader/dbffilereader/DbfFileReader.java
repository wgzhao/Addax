package com.alibaba.datax.plugin.reader.dbffilereader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.ColumnEntry;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jamel.dbf.DbfReader;
import org.jamel.dbf.structure.DbfDataType;
import org.jamel.dbf.structure.DbfField;
import org.jamel.dbf.structure.DbfHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Created by zhongtian.hu on 19-8-8.
 */
public class DbfFileReader
        extends Reader
{
    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;

        private List<String> path = null;

        private List<String> sourceFiles;

        private Map<String, Pattern> pattern;

        private Map<String, Boolean> isRegexPath;

        private boolean isBlank(Object object)
        {
            if (null == object) {
                return true;
            }
            if ((object instanceof String)) {
                return "".equals(((String) object).trim());
            }
            return false;
        }

        @Override
        public void init()
        {
            this.originConfig = this.getPluginJobConf();
            this.pattern = new HashMap<>();
            this.isRegexPath = new HashMap<>();
            this.validateParameter();
        }

        private void validateParameter()
        {
            // Compatible with the old version, path is a string before
            String pathInString = this.originConfig.getNecessaryValue(Key.PATH,
                    DbfFileReaderErrorCode.REQUIRED_VALUE);
            if (isBlank(pathInString)) {
                throw DataXException.asDataXException(
                        DbfFileReaderErrorCode.REQUIRED_VALUE,
                        "您需要指定待读取的源目录或文件");
            }
            if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
                path = new ArrayList<>();
                path.add(pathInString);
            }
            else {
                path = this.originConfig.getList(Key.PATH, String.class);
                if (null == path || path.isEmpty()) {
                    throw DataXException.asDataXException(
                            DbfFileReaderErrorCode.REQUIRED_VALUE,
                            "您需要指定待读取的源目录或文件");
                }
            }

            String encoding = this.originConfig
                    .getString(
                            com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING,
                            com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_ENCODING);
            if (isBlank(encoding)) {
                this.originConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING,
                                com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_ENCODING);
            }
            else {
                try {
                    encoding = encoding.trim();
                    this.originConfig
                            .set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING,
                                    encoding);
                    Charsets.toCharset(encoding);
                }
                catch (UnsupportedCharsetException uce) {
                    throw DataXException.asDataXException(
                            DbfFileReaderErrorCode.ILLEGAL_VALUE,
                            String.format("不支持您配置的编码格式 : [%s]", encoding), uce);
                }
                catch (Exception e) {
                    throw DataXException.asDataXException(
                            DbfFileReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            String.format("编码配置异常, 请联系我们: %s", e.getMessage()),
                            e);
                }
            }
            // 检测是column 是否为 ["*"] 若是则填为空
            List<Configuration> column = this.originConfig
                    .getListConfiguration(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
            if (null != column
                    && 1 == column.size()
                    && ("\"*\"".equals(column.get(0).toString()) || "'*'"
                    .equals(column.get(0).toString()))) {
                originConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN, new ArrayList<String>());
            }
            else {
                // column: 1. index type 2.value type 3.when type is Data, may have format
                List<Configuration> columns = this.originConfig
                        .getListConfiguration(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);

                if (null == columns || columns.isEmpty()) {
                    throw DataXException.asDataXException(
                            DbfFileReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            "您需要指定 columns");
                }

                for (Configuration eachColumnConf : columns) {
                    eachColumnConf.getNecessaryValue(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.TYPE, DbfFileReaderErrorCode.REQUIRED_VALUE);
                    Integer columnIndex = eachColumnConf.getInt(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.INDEX);
                    String columnValue = eachColumnConf.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.VALUE);

                    if (null == columnIndex && null == columnValue) {
                        throw DataXException.asDataXException(
                                DbfFileReaderErrorCode.NO_INDEX_VALUE,
                                "由于您配置了type, 则至少需要配置 index 或 value");
                    }

                    if (null != columnIndex && null != columnValue) {
                        throw DataXException.asDataXException(
                                DbfFileReaderErrorCode.MIXED_INDEX_VALUE,
                                "您混合配置了index, value, 每一列同时仅能选择其中一种");
                    }
                }
            }
        }

        @Override
        public void prepare()
        {
            LOG.debug("prepare() begin...");
            // warn:make sure this regex string
            // warn:no need trim
            for (String eachPath : this.path) {
                String regexString = eachPath.replace("*", ".*").replace("?",
                        ".?");
                Pattern patt = Pattern.compile(regexString);
                this.pattern.put(eachPath, patt);
            }
            this.sourceFiles = this.buildSourceTargets();

            LOG.info("您即将读取的文件数为: [{}]", this.sourceFiles.size());
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            //
        }

        // warn: 如果源目录为空会报错，拖空目录意图=>空文件显示指定此意图
        @Override
        public List<Configuration> split(int adviceNumber)
        {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<>();

            // warn:每个slice拖且仅拖一个文件,
            int splitNumber = this.sourceFiles.size();
            if (0 == splitNumber) {
                throw DataXException.asDataXException(
                        DbfFileReaderErrorCode.EMPTY_DIR_EXCEPTION, String
                                .format("未能找到待读取的文件,请确认您的配置项path: %s",
                                        this.originConfig.getString(Key.PATH)));
            }

            List<List<String>> splitedSourceFiles = this.splitSourceFiles(
                    this.sourceFiles, splitNumber);
            for (List<String> files : splitedSourceFiles) {
                Configuration splitedConfig = this.originConfig.clone();
                splitedConfig.set(Constant.SOURCE_FILES, files);
                readerSplitConfigs.add(splitedConfig);
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }

        // validate the path, path must be a absolute path
        private List<String> buildSourceTargets()
        {
            // for eath path
            Set<String> toBeReadFiles = new HashSet<>();
            for (String eachPath : this.path) {
                LOG.info("parse path {}", eachPath);
                int endMark;
                for (endMark = 0; endMark < eachPath.length(); endMark++) {
                    if ('*' == eachPath.charAt(endMark)
                            || '?' == eachPath.charAt(endMark)) {
                        this.isRegexPath.put(eachPath, true);
                        break;
                    }
                }

                String parentDirectory;
                if (!this.isRegexPath.isEmpty() && this.isRegexPath.containsKey(eachPath)) {
                    int lastDirSeparator = eachPath.substring(0, endMark)
                            .lastIndexOf(IOUtils.DIR_SEPARATOR);
                    parentDirectory = eachPath.substring(0,
                            lastDirSeparator + 1);
                }
                else {
                    this.isRegexPath.put(eachPath, false);
                    parentDirectory = eachPath;
                }
                this.buildSourceTargetsEathPath(eachPath, parentDirectory,
                        toBeReadFiles);
            }
            return Arrays.asList(toBeReadFiles.toArray(new String[0]));
        }

        private void buildSourceTargetsEathPath(String regexPath,
                String parentDirectory, Set<String> toBeReadFiles)
        {
            // 检测目录是否存在，错误情况更明确
            try {
                File dir = new File(parentDirectory);
                boolean isExists = dir.exists();
                if (!isExists) {
                    String message = String.format("您设定的目录不存在 : [%s]",
                            parentDirectory);
                    LOG.error(message);
                    throw DataXException.asDataXException(
                            DbfFileReaderErrorCode.FILE_NOT_EXISTS, message);
                }
            }
            catch (SecurityException se) {
                String message = String.format("您没有权限查看目录 : [%s]",
                        parentDirectory);
                LOG.error(message);
                throw DataXException.asDataXException(
                        DbfFileReaderErrorCode.SECURITY_NOT_ENOUGH, message);
            }

            directoryRover(regexPath, parentDirectory, toBeReadFiles);
        }

        private void directoryRover(String regexPath, String parentDirectory,
                Set<String> toBeReadFiles)
        {
            File directory = new File(parentDirectory);
            // is a normal file
            if (!directory.isDirectory()) {
                if (this.isTargetFile(regexPath, directory.getAbsolutePath())) {
                    toBeReadFiles.add(parentDirectory);
                    LOG.info("add file [{}] as a candidate to be read.", parentDirectory);
                }
            }
            else {
                // 是目录
                try {
                    // warn:对于没有权限的目录,listFiles 返回null，而不是抛出SecurityException
                    File[] files = directory.listFiles();
                    if (null != files) {
                        for (File subFileNames : files) {
                            directoryRover(regexPath,
                                    subFileNames.getAbsolutePath(),
                                    toBeReadFiles);
                        }
                    }
                    else {
                        // warn: 对于没有权限的文件，是直接throw DataXException
                        String message = String.format("您没有权限查看目录 : [%s]",
                                directory);
                        LOG.error(message);
                        throw DataXException.asDataXException(
                                DbfFileReaderErrorCode.SECURITY_NOT_ENOUGH,
                                message);
                    }
                }
                catch (SecurityException e) {
                    String message = String.format("您没有权限查看目录 : [%s]",
                            directory);
                    LOG.error(message);
                    throw DataXException.asDataXException(
                            DbfFileReaderErrorCode.SECURITY_NOT_ENOUGH,
                            message, e);
                }
            }
        }

        // 正则过滤
        private boolean isTargetFile(String regexPath, String absoluteFilePath)
        {
            if (Boolean.TRUE.equals(this.isRegexPath.get(regexPath))) {
                return this.pattern.get(regexPath).matcher(absoluteFilePath)
                        .matches();
            }
            else {
                return true;
            }
        }

        private <T> List<List<T>> splitSourceFiles(final List<T> sourceList,
                int adviceNumber)
        {
            List<List<T>> splitedList = new ArrayList<>();
            int averageLength = sourceList.size() / adviceNumber;
            averageLength = averageLength == 0 ? 1 : averageLength;

            for (int begin = 0, end; begin < sourceList.size(); begin = end) {
                end = begin + averageLength;
                if (end > sourceList.size()) {
                    end = sourceList.size();
                }
                splitedList.add(sourceList.subList(begin, end));
            }
            return splitedList;
        }
    }

    public static class Task
            extends Reader.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;
        private List<String> sourceFiles;

        @Override
        public void init()
        {
            this.readerSliceConfig = this.getPluginJobConf();
            this.sourceFiles = this.readerSliceConfig.getList(
                    Constant.SOURCE_FILES, String.class);
        }

        @Override
        public void prepare()
        {
            //
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            //
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            LOG.debug("start read dbf files...");
            for (String fileName : this.sourceFiles) {
                LOG.info("reading file : [{}]", fileName);

                String encod = readerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING);
                String nullFormat = readerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.NULL_FORMAT);

                try (DbfReader reader = new DbfReader(FileUtils.getFile(fileName))) {

                    List<ColumnEntry> column = UnstructuredStorageReaderUtil.getListColumnEntry(readerSliceConfig,
                            com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
                    DbfHeader header = reader.getHeader();
                    assert column != null;
                    int colnum = column.isEmpty() ? header.getFieldsCount() : column.size();
                    Object[] row;
                    while ((row = reader.nextRecord()) != null) {
                        String[] sourceLine = new String[colnum];
                        for (int i = 0; i < colnum; i++) {
                            if (column.get(i).getValue() != null) {
                                sourceLine[i] = column.get(i).getValue();
                            }
                            else if (i < header.getFieldsCount()) {
                                DbfField field = header.getField(i);
                                String value;
                                if (field.getDataType() == DbfDataType.DATE) {
                                    value =
                                            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format((Date) row[i]);
                                }
                                else {
                                    value = field.getDataType() == DbfDataType.CHAR
                                            ? new String((byte[]) row[i], encod)
                                            : String.valueOf(row[i]);
                                }
                                if (value != null) {
                                    value = value.trim();
                                }
                                sourceLine[i] = value;
                            }
                        }
                        UnstructuredStorageReaderUtil.transportOneRecord(recordSender, column, sourceLine, nullFormat, this.getTaskPluginCollector());
                    }
                }
                catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
            LOG.debug("end read dbf files...");
        }
    }
}
