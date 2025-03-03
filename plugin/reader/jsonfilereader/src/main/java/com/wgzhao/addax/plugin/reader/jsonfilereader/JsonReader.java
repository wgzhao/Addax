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
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.TypeRef;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.ENCODING_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;

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
            if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
                path = new ArrayList<>();
                path.add(pathInString);
            }
            else {
                path = this.originConfig.getList(Key.PATH, String.class);
                if (null == path || path.isEmpty()) {
                    throw AddaxException.asAddaxException(
                            REQUIRED_VALUE,
                            "The item `path` must be not empty");
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
                            "Not supported encoding type " + encoding, uce);
                }
                catch (Exception e) {
                    throw AddaxException.asAddaxException(
                            ENCODING_ERROR,
                            "Encoding Error:", e);
                }
            }

            // column: 1. index type 2.value type 3.when type is Date, may have
            List<Configuration> columns = this.originConfig.getListConfiguration(Key.COLUMN);

            if (null != columns && !columns.isEmpty()) {
                for (Configuration eachColumnConf : columns) {
                    eachColumnConf.getNecessaryValue(Key.TYPE, REQUIRED_VALUE);
                    String columnIndex = eachColumnConf.getString(Key.INDEX);
                    String columnValue = eachColumnConf.getString(Key.VALUE);

                    if (null == columnIndex && null == columnValue) {
                        throw AddaxException.asAddaxException(CONFIG_ERROR,
                                "Either index or value is required for type configuration");
                    }

                    if (null != columnIndex && null != columnValue) {
                        throw AddaxException.asAddaxException(CONFIG_ERROR,
                                "Both index and value are set, only one is allowed");
                    }
                }
            }
        }

        @Override
        public void prepare()
        {
            LOG.debug("begin to prepare...");
            this.sourceFiles = FileHelper.buildSourceTargets(this.path);
            LOG.info("The number of files you will read: [{}]", this.sourceFiles.size());
        }

        @Override
        public void destroy()
        {
            //
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            LOG.debug("begin to split...");
            List<Configuration> readerSplitConfigs = new ArrayList<>();

            int splitNumber = Math.min(sourceFiles.size(), adviceNumber);
            if (0 == splitNumber) {
                throw AddaxException.asAddaxException(
                        CONFIG_ERROR,
                        "none find path " + originConfig.getString(Key.PATH));
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

        private ParseContext parse;
        private boolean multiline;

        @Override
        public void init()
        {
            Configuration readerSliceConfig = this.getPluginJobConf();
            this.sourceFiles = readerSliceConfig.getList(Key.SOURCE_FILES, String.class);
            this.columns = readerSliceConfig.getListConfiguration(Key.COLUMN);
            this.compressType = readerSliceConfig.getString(Key.COMPRESS, null);
            this.encoding = readerSliceConfig.getString(Key.ENCODING, "utf-8");
            this.multiline = readerSliceConfig.getBool("singleLine", true);
            // return null for missing leafs.
            com.jayway.jsonpath.Configuration jsonConf = com.jayway.jsonpath.Configuration.defaultConfiguration();
            jsonConf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
            this.parse = JsonPath.using(jsonConf);
        }

        private List<Column> parseFromJson(String json)
        {
            List<Column> splitLine = new ArrayList<>();
            DocumentContext document = parse.parse(json);
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
                    tempValue = document.read(columnIndex, columnType.getClass());
                }
                Column insertColumn = getColumn(columnType, tempValue, columnFormat);
                splitLine.add(insertColumn);
            }
            return splitLine;
        }

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
                    try {
                        if (StringUtils.isNotBlank(columnFormat)) {
                            DateFormat format = new SimpleDateFormat(columnFormat);
                            columnGenerated = new DateColumn(format.parse(columnValue));
                        }
                        else {
                            columnGenerated = new DateColumn(new StringColumn(columnValue).asDate());
                        }
                    }
                    catch (Exception e) {
                        throw new IllegalArgumentException(String.format(errorTemplate, columnValue, "DATE"));
                    }
                    break;
                default:
                    throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE, "The type" + type + " is unsupported");
            }
            return columnGenerated;
        }

        private void transportOneRecord(RecordSender recordSender, List<Column> sourceLine)
        {
            Record record = recordSender.createRecord();
            for (Column eachValue : sourceLine) {
                record.addColumn(eachValue);
            }
            recordSender.sendToWriter(record);
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
                    // warn: the socket file can not be read, it may affect the transmission of all files, the user needs to ensure it by himself
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
                }
                catch (CompressorException | UnsupportedEncodingException e) {
                    throw AddaxException.asAddaxException(IO_ERROR, e);
                }
                if (multiline) {
                    multilineJsonParse(reader, recordSender);
                } else {
                    singleJsonParse(reader, recordSender);
                }
                IOUtils.closeQuietly(reader, null);
            }
            LOG.debug("end reading source files...");
        }

        /**
         * parse JSON Lines file
         * each line is a json object
         *
         * @param reader {@link BufferedReader}
         * @param recordSender {@link RecordSender}
         */
        private void multilineJsonParse(BufferedReader reader, RecordSender recordSender)
        {
            // read the content
            String jsonLine;
            try {
                jsonLine = reader.readLine();
                while (jsonLine != null) {
                    List<Column> sourceLine = parseFromJson(jsonLine);
                    transportOneRecord(recordSender, sourceLine);
                    recordSender.flush();
                    jsonLine = reader.readLine();
                }
            }
            catch (IOException e) {
                throw AddaxException.asAddaxException(IO_ERROR, e);
            }
        }

        private void singleJsonParse(BufferedReader reader, RecordSender recordSender)
        {
            StringBuilder jsonBuffer = new StringBuilder();
            String line;
            try {
                while ((line = reader.readLine()) != null) {
                    jsonBuffer.append(line);
                }
            }
            catch (IOException e) {
                throw AddaxException.asAddaxException(IO_ERROR, e);
            }

            DocumentContext ctx = parse.parse(jsonBuffer.toString());
            List<List<String>> jsonColumns = new ArrayList<>();
            List<Column> sourceLine = new ArrayList<>();
            int recordNum = -1;
            List<String> placeHolder =  Collections.emptyList();
            for (Configuration col: columns) {
                if (col.getString(Key.VALUE) == null) {
                    if (recordNum < 0) {
                        List<String> jsonColumn = ctx.read(col.getString(Key.INDEX));
                        recordNum = jsonColumn.size();
                        jsonColumns.add(jsonColumn);
                    } else {
                        jsonColumns.add(ctx.read(col.getString(Key.INDEX)));
                    }
                } else {
                    // the column use constant, mark it
                    jsonColumns.add(placeHolder);
                }
            }
            for (int i =0 ;i < recordNum; i++) {
                for (int j=0; j < columns.size(); j++) {
                    Configuration column = columns.get(j);
                    if (jsonColumns.get(j).isEmpty()) {
                        // use constant value
                        sourceLine.add(getColumn(column.getString(Key.TYPE), column.getString(Key.VALUE), column.getString(Key.FORMAT)));
                    } else {
                        sourceLine.add(getColumn(column.getString(Key.TYPE), String.valueOf(jsonColumns.get(j).get(i)), column.getString(Key.FORMAT)));
                    }
                }
                transportOneRecord(recordSender, sourceLine);
                recordSender.flush();
                sourceLine.clear();
            }
        }
    }
}
