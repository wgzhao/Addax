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
import com.wgzhao.addax.common.compress.ZipCycleInputStream;
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
import com.wgzhao.addax.storage.util.FileHelper;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.exception.CommonErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.exception.CommonErrorCode.ENCODING_ERROR;
import static com.wgzhao.addax.common.exception.CommonErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.exception.CommonErrorCode.IO_ERROR;
import static com.wgzhao.addax.common.exception.CommonErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.common.exception.CommonErrorCode.REQUIRED_VALUE;

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

        @Override
        public void init()
        {
            this.originConfig = this.getPluginJobConf();

            this.validateParameter();
        }

        private void validateParameter()
        {
            // Compatible with the old version, path is a string before
            String pathInString = this.originConfig.getNecessaryValue(Key.PATH,
                    REQUIRED_VALUE);
            if (StringUtils.isBlank(pathInString)) {
                throw AddaxException.asAddaxException(
                        REQUIRED_VALUE,
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
                            REQUIRED_VALUE,
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
                            NOT_SUPPORT_TYPE,
                            String.format("不支持您配置的编码格式 : [%s]", encoding), uce);
                }
                catch (Exception e) {
                    throw AddaxException.asAddaxException(
                            ENCODING_ERROR,
                            String.format("编码配置异常, 请联系我们: %s", e.getMessage()),
                            e);
                }
            }

            // column: 1. index type 2.value type 3.when type is Date, may have
            List<Configuration> columns = this.originConfig.getListConfiguration(Key.COLUMN);
            // 不再支持 ["*"]，必须指定json数据的路径

            if (null != columns && !columns.isEmpty()) {
                for (Configuration eachColumnConf : columns) {
                    eachColumnConf.getNecessaryValue(Key.TYPE, REQUIRED_VALUE);
                    String columnIndex = eachColumnConf.getString(Key.INDEX);
                    String columnValue = eachColumnConf.getString(Key.VALUE);

                    if (null == columnIndex && null == columnValue) {
                        throw AddaxException.asAddaxException(CONFIG_ERROR, "由于您配置了type, 则至少需要配置 index 或 value");
                    }

                    if (null != columnIndex && null != columnValue) {
                        throw AddaxException.asAddaxException(CONFIG_ERROR, "您混合配置了index, value, 每一列同时仅能选择其中一种");
                    }
                }
            }
            // 后续支持解压缩，现在暂不支持
        }

        @Override
        public void prepare()
        {
            LOG.debug("begin to prepare...");
            this.sourceFiles = FileHelper.buildSourceTargets(this.path);
            LOG.info("The number of files you will read: [{}]", this.sourceFiles.size());
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
            LOG.debug("begin to split...");
            List<Configuration> readerSplitConfigs = new ArrayList<>();

            // warn:每个slice拖且仅拖一个文件,
            // int splitNumber = adviceNumber
            int splitNumber = this.sourceFiles.size();
            if (0 == splitNumber) {
                throw AddaxException.asAddaxException(
                        CONFIG_ERROR,
                        String.format("NOT find any file in your path: %s", originConfig.getString(Key.PATH)));
            }

            List<List<String>> splitSourceFiles = FileHelper.splitSourceFiles(sourceFiles, splitNumber);
            for (List<String> files : splitSourceFiles) {
                Configuration splitConfig = this.originConfig.clone();
                splitConfig.set(Key.SOURCE_FILES, files);
                readerSplitConfigs.add(splitConfig);
            }
            LOG.debug("end split ...");
            return readerSplitConfigs;
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
            this.columns = readerSliceConfig.getListConfiguration(Key.COLUMN);
            this.compressType = readerSliceConfig.getString(Key.COMPRESS, null);
            this.encoding = readerSliceConfig.getString(Key.ENCODING, "utf-8");
        }

        //解析json，返回已经经过处理的行
        private List<Column> parseFromJson(String json)
        {
            List<Column> splitLine = new ArrayList<>();
            DocumentContext document = JsonPath.parse(json);
            String tempValue;
            for (Configuration eachColumnConf : columns) {
                String columnIndex = eachColumnConf.getString(Key.INDEX);
                String columnType = eachColumnConf.getString(Key.TYPE).toLowerCase();
                String columnFormat = eachColumnConf.getString(Key.FORMAT);
                String columnValue = eachColumnConf.getString(Key.VALUE);
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
            String errorTemplate = "Type cast error, can not cast %s to %s";
            switch (type) {
                case STRING:
                    columnGenerated = new StringColumn(columnValue);
                    break;
                case DOUBLE:
                    try {
                        columnGenerated = new DoubleColumn(columnValue);
                    }
                    catch (Exception e) {
                        throw new IllegalArgumentException(String.format(errorTemplate, columnValue, "DOUBLE"));
                    }
                    break;
                case BOOLEAN:
                    try {
                        columnGenerated = new BoolColumn(columnValue);
                    }
                    catch (Exception e) {
                        throw new IllegalArgumentException(String.format(errorTemplate, columnValue, "BOOLEAN"));
                    }
                    break;
                case LONG:
                    try {
                        columnGenerated = new LongColumn(columnValue);
                    }
                    catch (Exception e) {
                        LOG.error(e.getMessage());
                        throw new IllegalArgumentException(String.format(errorTemplate, columnValue, "LONG"));
                    }
                    break;
                case DATE:
                    try { //直接利用支持的处理日期数据
                        if (StringUtils.isNotBlank(columnFormat)) {
                            // 用户自己配置的格式转换, 脏数据行为出现变化
                            DateFormat format = new SimpleDateFormat(columnFormat);
                            columnGenerated = new DateColumn(format.parse(columnValue));
                        }
                        else {
                            // 框架尝试转换
                            columnGenerated = new DateColumn(new StringColumn(columnValue).asDate());
                        }
                    }
                    catch (Exception e) {
                        throw new IllegalArgumentException(String.format(errorTemplate, columnValue, "DATE"));
                    }
                    break;
                default:
                    String errorMessage = String.format("The type %s is unsupported", type);
                    LOG.error(errorMessage);
                    throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE, errorMessage);
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
            LOG.debug("begin to read source files...");
            FileInputStream fileInputStream;
            BufferedReader reader = null;
            for (String fileName : this.sourceFiles) {
                LOG.info("reading file : [{}]", fileName);
                try {
                    fileInputStream = new FileInputStream(fileName);
                }
                catch (FileNotFoundException e) {
                    // warn: sock 文件无法read,能影响所有文件的传输,需要用户自己保证
                    String message = String.format("The file %s not found", fileName);
                    LOG.error(message);
                    throw AddaxException.asAddaxException(CONFIG_ERROR, message);
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
                    String message = String.format("Failed to open file %s", fileName);
                    LOG.error(message);
                    throw AddaxException.asAddaxException(IO_ERROR, message);
                }
                finally {
                    IOUtils.closeQuietly(reader, null);
                }
            }
            LOG.debug("end reading source files...");
        }
    }
}
