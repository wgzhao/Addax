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

package com.wgzhao.addax.plugin.reader.jsonfilereader;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.BoolColumn;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
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
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
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
 * Created by jin.zhang on 18-05-30.
 */
public class JsonReader
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
                    JsonReaderErrorCode.REQUIRED_VALUE);
            if (StringUtils.isBlank(pathInString)) {
                throw AddaxException.asAddaxException(
                        JsonReaderErrorCode.REQUIRED_VALUE,
                        "您需要指定待读取的源目录或文件");
            }
            if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
                path = new ArrayList<>();
                path.add(pathInString);
            }
            else {
                path = this.originConfig.getList(Key.PATH, String.class);
                if (null == path || path.isEmpty()) {
                    throw AddaxException.asAddaxException(
                            JsonReaderErrorCode.REQUIRED_VALUE,
                            "您需要指定待读取的源目录或文件");
                }
            }

            String encoding = this.originConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
            if (StringUtils.isBlank(encoding)) {
                this.originConfig.set(Key.ENCODING, Constant.DEFAULT_ENCODING);
            }
            else {
                try {
                    encoding = encoding.trim();
                    this.originConfig.set(Key.ENCODING, encoding);
                    Charsets.toCharset(encoding);
                }
                catch (UnsupportedCharsetException uce) {
                    throw AddaxException.asAddaxException(
                            JsonReaderErrorCode.ILLEGAL_VALUE,
                            String.format("不支持您配置的编码格式 : [%s]", encoding), uce);
                }
                catch (Exception e) {
                    throw AddaxException.asAddaxException(
                            JsonReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            String.format("编码配置异常, 请联系我们: %s", e.getMessage()),
                            e);
                }
            }

            // column: 1. index type 2.value type 3.when type is Date, may have
            List<Configuration> columns = this.originConfig.getListConfiguration(Key.COLUMN);
            // 不再支持 ["*"]，必须指定json数据的路径

            if (null != columns && !columns.isEmpty()) {
                for (Configuration eachColumnConf : columns) {
                    eachColumnConf.getNecessaryValue(Key.TYPE, JsonReaderErrorCode.REQUIRED_VALUE);
                    String columnIndex = eachColumnConf.getString(Key.INDEX);
                    String columnValue = eachColumnConf.getString(Key.VALUE);

                    if (null == columnIndex && null == columnValue) {
                        throw AddaxException.asAddaxException(JsonReaderErrorCode.NO_INDEX_VALUE, "由于您配置了type, 则至少需要配置 index 或 value");
                    }

                    if (null != columnIndex && null != columnValue) {
                        throw AddaxException.asAddaxException(JsonReaderErrorCode.MIXED_INDEX_VALUE, "您混合配置了index, value, 每一列同时仅能选择其中一种");
                    }
                }
            }
            // 后续支持解压缩，现在暂不支持
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
                Pattern pattern = Pattern.compile(regexString);
                this.pattern.put(eachPath, pattern);
                this.sourceFiles = this.buildSourceTargets();
            }

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
                throw AddaxException.asAddaxException(
                        JsonReaderErrorCode.EMPTY_DIR_EXCEPTION, String
                                .format("未能找到待读取的文件,请确认您的配置项path: %s",
                                        this.originConfig.getString(Key.PATH)));
            }

            List<List<String>> splitedSourceFiles = this.splitSourceFiles(
                    this.sourceFiles, splitNumber);
            for (List<String> files : splitedSourceFiles) {
                Configuration splitedConfig = this.originConfig.clone();
                splitedConfig.set(Key.SOURCE_FILES, files);
                readerSplitConfigs.add(splitedConfig);
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }

        // validate the path, path must be a absolute path
        private List<String> buildSourceTargets()
        {
            // for each path
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
                this.buildSourceTargetsEachPath(eachPath, parentDirectory,
                        toBeReadFiles);
            }
            return Arrays.asList(toBeReadFiles.toArray(new String[0]));
        }

        private void buildSourceTargetsEachPath(String regexPath,
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
                    throw AddaxException.asAddaxException(
                            JsonReaderErrorCode.FILE_NOT_EXISTS, message);
                }
            }
            catch (SecurityException se) {
                String message = String.format("您没有权限查看目录 : [%s]", parentDirectory);
                LOG.error(message);
                throw AddaxException.asAddaxException(
                        JsonReaderErrorCode.SECURITY_NOT_ENOUGH, message);
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
                        throw AddaxException.asAddaxException(
                                JsonReaderErrorCode.SECURITY_NOT_ENOUGH,
                                message);
                    }
                }
                catch (SecurityException e) {
                    String message = String.format("您没有权限查看目录 : [%s]",
                            directory);
                    LOG.error(message);
                    throw AddaxException.asAddaxException(
                            JsonReaderErrorCode.SECURITY_NOT_ENOUGH,
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

        private <T> List<List<T>> splitSourceFiles(List<T> sourceList, int adviceNumber)
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
        public static final String STRING = "string";
        public static final String LONG = "long";
        public static final String BOOLEAN = "boolean";
        public static final String DATE = "date";
        public static final String DOUBLE = "double";

        private List<String> sourceFiles;
        private List<Configuration> columns;
        private String compressType;
        private String encoding;


        @Override
        public void init()
        {
            Configuration readerSliceConfig = this.getPluginJobConf();
            this.sourceFiles = readerSliceConfig.getList(Key.SOURCE_FILES, String.class);
            this.columns = readerSliceConfig
                    .getListConfiguration(Key.COLUMN);
            this.compressType = readerSliceConfig.getString(Key.COMPRESS, null);
            this.encoding = readerSliceConfig.getString(Key.ENCODING, "utf-8");
        }

        //解析json，返回已经经过处理的行
        private List<Column> parseFromJson(String json)
        {
            List<Column> splitLine = new ArrayList<>();
            DocumentContext document = JsonPath.parse(json);
//            JSONArray objects = JSON.parseArray(json);
            String tempValue;
            for (Configuration eachColumnConf : columns) {
                String columnIndex = eachColumnConf
                        .getString(Key.INDEX);
                String columnType = eachColumnConf
                        .getString(Key.TYPE);
                String columnFormat = eachColumnConf
                        .getString(Key.FORMAT);
                String columnValue = eachColumnConf
                        .getString(Key.VALUE);
                // 这里是为了支持常量Value 现在需要考虑做容错，如果json里面没有的解析路径置为null
                if (null != columnValue) {
                    tempValue = columnValue;
                }
                else {
                    try {
                        tempValue = document.read(columnIndex, columnType.getClass());
                    }
                    catch (Exception ignore) {
                        tempValue = null;
                    }
                }
                Column insertColumn = getColumn(columnType, tempValue, columnFormat);
                splitLine.add(insertColumn);
            }
            return splitLine;
        }

        //匹配类型
        private Column getColumn(String type, String columnValue, String columnFormat)
        {
            Column columnGenerated;
            String errorTemplate = " 类型转换错误, 无法将[%s] 转换为[%s]";
            switch (type) {
                case STRING:
                    columnGenerated = new StringColumn(columnValue);
                    break;
                case DOUBLE:
                    try {
                        columnGenerated = new DoubleColumn(columnValue);
                    }
                    catch (Exception e) {
                        throw new IllegalArgumentException(String.format(
                                errorTemplate, columnValue, "DOUBLE"));
                    }
                    break;
                case BOOLEAN:
                    try {
                        columnGenerated = new BoolColumn(columnValue);
                    }
                    catch (Exception e) {
                        throw new IllegalArgumentException(String.format(
                                errorTemplate, columnValue, "BOOLEAN"));
                    }
                    break;
                case LONG:
                    try {
                        columnGenerated = new LongColumn(columnValue);
                    }
                    catch (Exception e) {
                        LOG.error(e.getMessage());
                        throw new IllegalArgumentException(String.format(
                                errorTemplate, columnValue, "LONG"));
                    }
                    break;
                case DATE:
                    try { //直接利用支持的处理日期数据
                        if (StringUtils.isNotBlank(columnFormat)) {
                            // 用户自己配置的格式转换, 脏数据行为出现变化
                            DateFormat format = new SimpleDateFormat(columnFormat);
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
                    catch (Exception e) {
                        throw new IllegalArgumentException(String.format(
                                errorTemplate, columnValue, "DATE"));
                    }
                    break;
                default:
                    String errorMessage = String.format(
                            "您配置的列类型暂不支持 : [%s]", type);
                    LOG.error(errorMessage);
                    throw AddaxException
                            .asAddaxException(
                                    JsonReaderErrorCode.NOT_SUPPORT_TYPE,
                                    errorMessage);
            }
            return columnGenerated;
        }

        //传输一行数据
        private void transportOneRecord(RecordSender recordSender, List<Column> sourceLine)
        {
            Record record = recordSender.createRecord();
            for (Column eachValue : sourceLine) {
                record.addColumn(eachValue);
            }
            recordSender.sendToWriter(record);
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
            FileInputStream fileInputStream;
            BufferedReader reader = null;
            for (String fileName : this.sourceFiles) {
                LOG.info("reading file : [{}]", fileName);
                try {
                    fileInputStream = new FileInputStream(fileName);
                }
                catch (FileNotFoundException e) {
                    // warn: sock 文件无法read,能影响所有文件的传输,需要用户自己保证
                    String message = String
                            .format("找不到待读取的文件 : [%s]", fileName);
                    LOG.error(message);
                    throw AddaxException.asAddaxException(
                            JsonReaderErrorCode.OPEN_FILE_ERROR, message);
                }
                try {
                    if (compressType != null) {
                        if ("zip".equalsIgnoreCase(compressType)) {
                            ZipCycleInputStream zis = new ZipCycleInputStream(fileInputStream);
                            reader = new BufferedReader(new InputStreamReader(zis, encoding), Constant.DEFAULT_BUFFER_SIZE);
                        }
                        else {
                            BufferedInputStream bis = new BufferedInputStream(fileInputStream);
                            CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
                            reader = new BufferedReader(new InputStreamReader(input, encoding), Constant.DEFAULT_BUFFER_SIZE);
                        }
                    }
                    else {
                        reader = new BufferedReader(new InputStreamReader(fileInputStream, encoding), Constant.DEFAULT_BUFFER_SIZE);
                    }

                    // read the content
                    String jsonLine;
                    jsonLine = reader.readLine();
                    while (jsonLine != null) {
                        List<Column> sourceLine = parseFromJson(jsonLine);
                        transportOneRecord(recordSender, sourceLine);
                        recordSender.flush();
                        jsonLine = reader.readLine();
                    }
                }
                catch (CompressorException | UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                catch (IOException e) {
                    // warn: 有可能本地无法读取文件
                    String message = String
                            .format("无法读取文件 : [%s]", fileName);
                    LOG.error(message);
                    throw AddaxException.asAddaxException(
                            JsonReaderErrorCode.READ_FILE_IO_ERROR, message);
                }
                finally {
                    IOUtils.closeQuietly(reader, null);
                }
            }
            LOG.debug("end read source files...");
        }
    }
}
