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

package com.wgzhao.datax.plugin.reader.txtfilereader;

import com.alibaba.fastjson.JSON;
import com.csvreader.CsvReader;
import com.wgzhao.datax.common.element.BoolColumn;
import com.wgzhao.datax.common.element.BytesColumn;
import com.wgzhao.datax.common.element.Column;
import com.wgzhao.datax.common.element.DateColumn;
import com.wgzhao.datax.common.element.DoubleColumn;
import com.wgzhao.datax.common.element.LongColumn;
import com.wgzhao.datax.common.element.Record;
import com.wgzhao.datax.common.element.StringColumn;
import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.plugin.RecordSender;
import com.wgzhao.datax.common.plugin.TaskPluginCollector;
import com.wgzhao.datax.common.spi.Reader;
import com.wgzhao.datax.common.util.Configuration;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Created by haiwei.luo on 14-9-20.
 */
public class TxtFileReader
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

        private String encoding;

        @Override
        public void init()
        {
            this.originConfig = this.getPluginJobConf();
            this.pattern = new HashMap<>();
            this.isRegexPath = new HashMap<>();
            this.validateParameter();
            validateParameter();
        }

        private void validateParameter()
        {
            // Compatible with the old version, path is a string before
            String pathInString = this.originConfig.getNecessaryValue(Key.PATH, TxtFileReaderErrorCode.REQUIRED_VALUE);
            if (StringUtils.isBlank(pathInString)) {
                throw DataXException.asDataXException(
                        TxtFileReaderErrorCode.REQUIRED_VALUE, "您需要指定待读取的源目录或文件");
            }
            if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
                path = new ArrayList<>();
                path.add(pathInString);
            }
            else {
                path = this.originConfig.getList(Key.PATH, String.class);
                if (null == path || path.isEmpty()) {
                    throw DataXException.asDataXException(
                            TxtFileReaderErrorCode.REQUIRED_VALUE, "您需要指定待读取的源目录或文件");
                }
            }

            this.encoding = this.originConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
            if (StringUtils.isBlank(encoding)) {
                this.originConfig.set(Key.ENCODING, Constant.DEFAULT_ENCODING);
            }
            else {
                try {
                    encoding = encoding.trim();
                    Charsets.toCharset(encoding);
                    this.originConfig.set(Key.ENCODING, encoding);
                }
                catch (UnsupportedCharsetException uce) {
                    throw DataXException.asDataXException(
                            TxtFileReaderErrorCode.ILLEGAL_VALUE,
                            String.format("不支持您配置的编码格式 : [%s]", encoding), uce);
                }
                catch (Exception e) {
                    throw DataXException.asDataXException(
                            TxtFileReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            String.format("编码配置异常, 请联系我们: %s", e.getMessage()),
                            e);
                }
            }

            // column: 1. index type 2.value type 3.when type is Date, may have
            // format
            List<Configuration> columns = this.originConfig.getListConfiguration(Key.COLUMN);
            // handle ["*"]
            if (null != columns && 1 == columns.size()) {
                String columnsInStr = columns.get(0).toString();
                if ("\"*\"".equals(columnsInStr) || "'*'".equals(columnsInStr)) {
                    this.originConfig.set(Key.COLUMN, null);
                    columns = null;
                }
            }

            if (null != columns && !columns.isEmpty()) {
                for (Configuration eachColumnConf : columns) {
                    eachColumnConf
                            .getNecessaryValue(
                                    Key.TYPE,
                                    TxtFileReaderErrorCode.REQUIRED_VALUE);
                    Integer columnIndex = eachColumnConf
                            .getInt(Key.INDEX);
                    String columnValue = eachColumnConf
                            .getString(Key.VALUE);

                    if (null == columnIndex && null == columnValue) {
                        throw DataXException.asDataXException(
                                TxtFileReaderErrorCode.NO_INDEX_VALUE,
                                "由于您配置了type, 则至少需要配置 index 或 value");
                    }

                    if (null != columnIndex && null != columnValue) {
                        throw DataXException.asDataXException(
                                TxtFileReaderErrorCode.MIXED_INDEX_VALUE,
                                "您混合配置了index, value, 每一列同时仅能选择其中一种");
                    }
                    if (null != columnIndex && columnIndex < 0) {
                        throw DataXException.asDataXException(
                                TxtFileReaderErrorCode.ILLEGAL_VALUE, String
                                        .format("index需要大于等于0, 您配置的index为[%s]",
                                                columnIndex));
                    }
                }
            }

            String delimiterInStr = this.originConfig.getString(Key.FIELD_DELIMITER);
            // warn: if have, length must be one
            if (null != delimiterInStr && 1 != delimiterInStr.length()) {
                throw DataXException.asDataXException(
                        TxtFileReaderErrorCode.ILLEGAL_VALUE,
                        String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", delimiterInStr));
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
            // int splitNumber = adviceNumber
            int splitNumber = this.sourceFiles.size();
            if (0 == splitNumber) {
                throw DataXException.asDataXException(
                        TxtFileReaderErrorCode.EMPTY_DIR_EXCEPTION, String
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
                int endMark;
                for (endMark = 0; endMark < eachPath.length(); endMark++) {
                    if ('*' == eachPath.charAt(endMark)
                            || '?' == eachPath.charAt(endMark)) {
                        this.isRegexPath.put(eachPath, true);
                        break;
                    }
                }

                String parentDirectory;
                if (BooleanUtils.isTrue(this.isRegexPath.get(eachPath))) {
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
                            TxtFileReaderErrorCode.FILE_NOT_EXISTS, message);
                }
            }
            catch (SecurityException se) {
                String message = String.format("您没有权限查看目录 : [%s]",
                        parentDirectory);
                LOG.error(message);
                throw DataXException.asDataXException(
                        TxtFileReaderErrorCode.SECURITY_NOT_ENOUGH, message);
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
                    LOG.info("add file [{}] as a candidate to be read.",
                            parentDirectory);
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
                                TxtFileReaderErrorCode.SECURITY_NOT_ENOUGH,
                                message);
                    }
                }
                catch (SecurityException e) {
                    String message = String.format("您没有权限查看目录 : [%s]",
                            directory);
                    LOG.error(message);
                    throw DataXException.asDataXException(
                            TxtFileReaderErrorCode.SECURITY_NOT_ENOUGH,
                            message, e);
                }
            }
        }

        // 正则过滤
        private boolean isTargetFile(String regexPath, String absoluteFilePath)
        {
            if (this.isRegexPath.get(regexPath)) {
                return this.pattern.get(regexPath).matcher(absoluteFilePath).matches();
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

        private enum Type
        {
            STRING, LONG, BOOL, DOUBLE, DATE, BYTES,
            ;
        }

        private Configuration readerSliceConfig;
        private List<String> sourceFiles;
        private List<Configuration> column;
        private String encoding;
        private int bufferSize;
        private Character fieldDelimiter;
        private boolean skipHeader;
        private String nullFormat;

        @Override
        public void init()
        {
            this.readerSliceConfig = this.getPluginJobConf();
            this.sourceFiles = this.readerSliceConfig.getList(Constant.SOURCE_FILES, String.class);
            this.column = readerSliceConfig.getListConfiguration(Key.COLUMN);
            // handle ["*"] -> [], null
            if (null != column && 1 == column.size() && "\"*\"".equals(column.get(0).toString())) {
                this.column = null;
            }
            this.encoding = this.readerSliceConfig.getString(Key.ENCODING);
            this.bufferSize = readerSliceConfig.getInt(Key.BUFFER_SIZE, Constant.DEFAULT_BUFFER_SIZE);
            String delimiterInStr = readerSliceConfig.getString(Key.FIELD_DELIMITER);
            if (null != delimiterInStr && 1 != delimiterInStr.length()) {
                throw DataXException.asDataXException(
                        TxtFileReaderErrorCode.ILLEGAL_VALUE,
                        String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", delimiterInStr));
            }
            if (null == delimiterInStr) {
                LOG.warn("您没有配置列分隔符, 使用默认值[{}]", Constant.DEFAULT_FIELD_DELIMITER);
            }

            this.fieldDelimiter = readerSliceConfig.getChar(Key.FIELD_DELIMITER, Constant.DEFAULT_FIELD_DELIMITER);
            this.skipHeader = readerSliceConfig.getBool(Key.SKIP_HEADER, Constant.DEFAULT_SKIP_HEADER);
            // warn: no default value '\N'
            this.nullFormat = readerSliceConfig.getString(Key.NULL_FORMAT, Constant.DEFAULT_NULL_FORMAT);
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
            LOG.debug("start read source files...");
            BufferedReader reader = null;
            InputStream inputStream;
            for (String fileName : this.sourceFiles) {
                LOG.info("reading file : [{}]", fileName);
                try {
                    inputStream = new FileInputStream(fileName);
                }
                catch (FileNotFoundException e) {
                    // warn: sock 文件无法read,能影响所有文件的传输,需要用户自己保证
                    throw DataXException.asDataXException(
                            TxtFileReaderErrorCode.OPEN_FILE_ERROR, String.format("找不到待读取的文件 : [%s]", fileName));
                }
                try {
                    String compressType = FileHelper.getCompressType(fileName);
                    if (compressType != null) {
                        if ("zip".equals(compressType)) {
                            ZipCycleInputStream zis = new ZipCycleInputStream(inputStream);
                            reader = new BufferedReader(new InputStreamReader(zis, encoding), bufferSize);
                        }
                        else {
                            BufferedInputStream bis = new BufferedInputStream(inputStream);
                            CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
                            reader = new BufferedReader(new InputStreamReader(input, encoding), bufferSize);
                        }
                    }
                    else {
                        reader = new BufferedReader(new InputStreamReader(inputStream, encoding), bufferSize);
                    }
                    doReadFromStream(reader, fileName, readerSliceConfig, recordSender, getTaskPluginCollector());
                    recordSender.flush();
                }
                catch (CompressorException | IOException e) {
                    e.printStackTrace();
                }
                finally {
                    IOUtils.closeQuietly(reader, null);
                }
            }
            LOG.debug("end read source files...");
        }

        public void doReadFromStream(BufferedReader reader, String context, Configuration readerSliceConfig, RecordSender recordSender,
                TaskPluginCollector taskPluginCollector)
        {

            CsvReader csvReader = null;

            // every line logic
            try {
                // TODO lineDelimiter
                if (skipHeader) {
                    String fetchLine = reader.readLine();
                    LOG.info("Header line {} has been skiped.",
                            fetchLine);
                }
                csvReader = new CsvReader(reader);
                csvReader.setDelimiter(fieldDelimiter);

                setCsvReaderConfig(csvReader);

                String[] parseRows;
                while ((parseRows = splitBufferedReader(csvReader)) != null) {
                    transportOneRecord(recordSender, parseRows, taskPluginCollector);
                }
            }
            catch (UnsupportedEncodingException uee) {
                throw DataXException
                        .asDataXException(
                                TxtFileReaderErrorCode.OPEN_FILE_WITH_CHARSET_ERROR,
                                String.format("不支持的编码格式 : [%s]", encoding), uee);
            }
            catch (FileNotFoundException fnfe) {
                throw DataXException.asDataXException(
                        TxtFileReaderErrorCode.FILE_NOT_EXISTS,
                        String.format("无法找到文件 : [%s]", context), fnfe);
            }
            catch (IOException ioe) {
                throw DataXException.asDataXException(
                        TxtFileReaderErrorCode.READ_FILE_IO_ERROR,
                        String.format("读取文件错误 : [%s]", context), ioe);
            }
            catch (Exception e) {
                throw DataXException.asDataXException(
                        TxtFileReaderErrorCode.RUNTIME_EXCEPTION,
                        String.format("运行时异常 : %s", e.getMessage()), e);
            }
            finally {
                if (csvReader != null) {
                    csvReader.close();
                }
                IOUtils.closeQuietly(reader, null);
            }
        }

        private void transportOneRecord(RecordSender recordSender, String[] sourceLine, TaskPluginCollector taskPluginCollector)
        {
            Record record = recordSender.createRecord();
            Column columnGenerated;

            // 创建都为String类型column的record
            if (null == column || column.isEmpty()) {
                for (String columnValue : sourceLine) {
                    // not equalsIgnoreCase, it's all ok if nullFormat is null
                    if (columnValue.equals(nullFormat)) {
                        columnGenerated = new StringColumn(null);
                    }
                    else {
                        columnGenerated = new StringColumn(columnValue);
                    }
                    record.addColumn(columnGenerated);
                }
                recordSender.sendToWriter(record);
            }
            else {
                try {
                    for (Configuration columnConfig : column) {
                        String columnType = columnConfig.getString(Key.TYPE);
                        Integer columnIndex = columnConfig.getInt(Key.INDEX);
                        String columnConst = columnConfig.getString(Key.VALUE);

                        String columnValue;

                        if (null == columnIndex && null == columnConst) {
                            throw DataXException
                                    .asDataXException(
                                            TxtFileReaderErrorCode.NO_INDEX_VALUE,
                                            "由于您配置了type, 则至少需要配置 index 或 value");
                        }

                        if (null != columnIndex && null != columnConst) {
                            throw DataXException
                                    .asDataXException(
                                            TxtFileReaderErrorCode.MIXED_INDEX_VALUE,
                                            "您混合配置了index, value, 每一列同时仅能选择其中一种");
                        }

                        if (null != columnIndex) {
                            if (columnIndex >= sourceLine.length) {
                                String message = String
                                        .format("您尝试读取的列越界,源文件该行有 [%s] 列,您尝试读取第 [%s] 列, 数据详情[%s]",
                                                sourceLine.length, columnIndex + 1,
                                                StringUtils.join(sourceLine, ","));
                                LOG.warn(message);
                                throw new IndexOutOfBoundsException(message);
                            }

                            columnValue = sourceLine[columnIndex];
                        }
                        else {
                            columnValue = columnConst;
                        }
                        Type type = Type.valueOf(columnType.toUpperCase());
                        // it's all ok if nullFormat is null
                        if (columnValue.equals(nullFormat)) {
                            columnValue = null;
                        }
                        String errorTemplate =  "类型转换错误, 无法将[%s] 转换为[%s]";
                        switch (type) {
                            case STRING:
                                columnGenerated = new StringColumn(columnValue);
                                break;
                            case LONG:
                                try {
                                    columnGenerated = new LongColumn(columnValue);
                                }
                                catch (Exception e) {
                                    throw new IllegalArgumentException(String.format(
                                            errorTemplate, columnValue,
                                            "LONG"));
                                }
                                break;
                            case DOUBLE:
                                try {
                                    columnGenerated = new DoubleColumn(columnValue);
                                }
                                catch (Exception e) {
                                    throw new IllegalArgumentException(String.format(
                                            errorTemplate, columnValue,
                                            "DOUBLE"));
                                }
                                break;
                            case BOOL:
                                try {
                                    columnGenerated = new BoolColumn(columnValue);
                                }
                                catch (Exception e) {
                                    throw new IllegalArgumentException(String.format(
                                            errorTemplate, columnValue,
                                            "BOOLEAN"));
                                }

                                break;
                            case DATE:
                                try {
                                    if (columnValue == null || StringUtils.isBlank(columnValue)) {
                                        columnGenerated = null;
                                    }
                                    else {
                                        String formatString = columnConfig.getString(Key.FORMAT);
                                        if (StringUtils.isNotBlank(formatString)) {
                                            // 用户自己配置的格式转换, 脏数据行为出现变化
                                            DateFormat format = new SimpleDateFormat(formatString);
                                            columnGenerated = new DateColumn(format.parse(columnValue));
                                        }
                                        else {
                                            // 框架尝试转换
                                            columnGenerated = new DateColumn(
                                                    new StringColumn(columnValue)
                                                            .asDate());
                                        }
                                    }
                                }
                                catch (Exception e) {
                                    throw new IllegalArgumentException(String.format(
                                            errorTemplate, columnValue,
                                            "DATE"));
                                }
                                break;
                            case BYTES:
                                if (columnValue == null) {
                                    columnGenerated = new BytesColumn(new byte[0]);
                                }
                                else {
                                    columnGenerated = new BytesColumn(columnValue.getBytes(StandardCharsets.UTF_8));
                                }
                                break;
                            default:
                                String errorMessage = String.format(
                                        "您配置的列类型暂不支持 : [%s]", columnType);
                                LOG.error(errorMessage);
                                throw DataXException
                                        .asDataXException(
                                                TxtFileReaderErrorCode.NOT_SUPPORT_TYPE,
                                                errorMessage);
                        }

                        record.addColumn(columnGenerated);
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
            }
        }

        public String[] splitBufferedReader(CsvReader csvReader)
                throws IOException
        {
            String[] splitedResult = null;
            if (csvReader.readRecord()) {
                splitedResult = csvReader.getValues();
            }
            return splitedResult;
        }

        public void setCsvReaderConfig(CsvReader csvReader)
        {
            Map<String, Object> csvReaderConfigMap = readerSliceConfig.getMap(Key.CSV_READER_CONFIG);
            if (null != csvReaderConfigMap && !csvReaderConfigMap.isEmpty()) {
                try {
                    BeanUtils.populate(csvReader, csvReaderConfigMap);
                    LOG.debug("csvReaderConfig设置成功,设置后CsvReader: {}", JSON.toJSONString(csvReader));
                }
                catch (Exception e) {
                    LOG.warn("WARN!!!!忽略csvReaderConfig配置!通过BeanUtils.populate配置您的csvReaderConfig发生异常,您配置的值为: {};请检查您的配置!CsvReader使用默认值[{}]",
                            JSON.toJSONString(csvReaderConfigMap), JSON.toJSONString(csvReader));
                }
            }
            else {
                //默认关闭安全模式, 放开10W字节的限制
                csvReader.setSafetySwitch(false);
                LOG.debug("CsvReader使用默认值[{}],csvReaderConfig值为[{}]", JSON.toJSONString(csvReader), JSON.toJSONString(csvReaderConfigMap));
            }
        }
    }
}
