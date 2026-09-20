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
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.storage.reader.StorageReaderUtil;
import com.wgzhao.addax.storage.util.FileHelper;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.CONVERT_NOT_SUPPORT;
import static com.wgzhao.addax.core.spi.ErrorCode.ENCODING_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/**
 * Created by jin.zhang on 18-05-30.
 */
public class JsonReader
        extends Reader
{
    /**
     * The declared type of a column. It is a fixed part of the job configuration, so it is resolved
     * once when the column plan is built instead of once for every record.
     */
    private enum ColumnType
    {
        STRING, LONG, DOUBLE, BOOLEAN, DATE;

        static ColumnType of(String name)
        {
            return switch (name.toLowerCase(Locale.ROOT)) {
                case "string" -> STRING;
                case "long" -> LONG;
                case "double" -> DOUBLE;
                case "boolean" -> BOOLEAN;
                case "date" -> DATE;
                default -> throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                        "The type %s is unsupported".formatted(name));
            };
        }

        /**
         * Converts a raw value into a column of this type. A null value yields a null column, the
         * element constructors accept null for that reason.
         *
         * @param value the value read from the json document
         * @param dateFormat the format used to parse a date column, ignored for the other types
         * @return the parsed column
         */
        Column parse(String value, DateFormat dateFormat)
        {
            try {
                return switch (this) {
                    case STRING -> new StringColumn(value);
                    case LONG -> new LongColumn(value);
                    case DOUBLE -> new DoubleColumn(value);
                    case BOOLEAN -> new BoolColumn(value);
                    // without an explicit format, the formats configured by common.column.* are tried
                    case DATE -> dateFormat != null && value != null
                            ? new DateColumn(dateFormat.parse(value))
                            : new DateColumn(new StringColumn(value).asDate());
                };
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(CONVERT_NOT_SUPPORT,
                        "Cannot convert the value [%s] to %s".formatted(value, this), e);
            }
        }
    }

    /** Job. */
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
            this.originConfig.getNecessaryValue(Key.PATH, REQUIRED_VALUE);
            Object pathValue = this.originConfig.get(Key.PATH);
            if (pathValue instanceof List) {
                path = this.originConfig.getList(Key.PATH, String.class);
                if (path.isEmpty()) {
                    throw AddaxException.asAddaxException(REQUIRED_VALUE,
                            "The item `path` must be not empty");
                }
            }
            else {
                path = List.of(String.valueOf(pathValue));
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
                    throw AddaxException.asAddaxException(ENCODING_ERROR,
                            "Encoding Error:", e);
                }
            }

            var columns = this.originConfig.getListConfiguration(Key.COLUMN);
            if (columns == null || columns.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "The item `column` must be not empty");
            }
            columns.forEach(this::validateColumn);
        }

        private void validateColumn(Configuration columnConf)
        {
            columnConf.getNecessaryValue(Key.TYPE, REQUIRED_VALUE);
            // fail early on an unsupported type instead of on the first record
            ColumnType.of(columnConf.getString(Key.TYPE));
            var columnIndex = columnConf.getString(Key.INDEX);
            var columnValue = columnConf.getString(Key.VALUE);

            if (columnIndex == null && columnValue == null) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "Either index or value is required for type configuration");
            }

            if (columnIndex != null && columnValue != null) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "Both index and value are set, only one is allowed");
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

    /** Task. */
    public static class Task
            extends Reader.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        /**
         * Everything needed to read one column. Resolving the configuration, compiling the json path
         * and building the date formatter once per column keeps the record loop free of that work.
         *
         * @param type the declared type of the column
         * @param path the compiled json path, null for a constant column
         * @param constant the column emitted for every record, null when the column comes from the document
         * @param dateFormat the formatter of a date column, null when no format is configured
         */
        private record ColumnPlan(ColumnType type, JsonPath path, Column constant, DateFormat dateFormat) {}

        private List<String> sourceFiles;
        private List<ColumnPlan> columnPlans;
        private String encoding;
        private String compress;
        private boolean singleLine;

        private ParseContext parse;
        private JsonProvider jsonProvider;

        @Override
        public void init()
        {
            Configuration readerSliceConfig = this.getPluginJobConf();
            this.sourceFiles = readerSliceConfig.getList(Key.SOURCE_FILES, String.class);
            this.encoding = readerSliceConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
            this.compress = readerSliceConfig.getString(Key.COMPRESS, null);
            this.singleLine = readerSliceConfig.getBool(JsonKey.SINGLE_LINE, true);
            // a path that matches nothing yields a null column instead of failing the whole job
            com.jayway.jsonpath.Configuration jsonConf = com.jayway.jsonpath.Configuration
                    .defaultConfiguration()
                    .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS);
            this.parse = JsonPath.using(jsonConf);
            this.jsonProvider = jsonConf.jsonProvider();
            this.columnPlans = buildColumnPlans(readerSliceConfig.getListConfiguration(Key.COLUMN));
        }

        private List<ColumnPlan> buildColumnPlans(List<Configuration> columns)
        {
            List<ColumnPlan> plans = new ArrayList<>(columns.size());
            for (Configuration column : columns) {
                ColumnType type = ColumnType.of(column.getNecessaryValue(Key.TYPE, REQUIRED_VALUE));
                DateFormat dateFormat = type == ColumnType.DATE ? dateFormatOf(column.getString(Key.FORMAT)) : null;
                String value = column.getString(Key.VALUE);
                // a constant column is parsed once and then shared by every record
                Column constant = value == null ? null : type.parse(value, dateFormat);
                JsonPath path = value == null ? JsonPath.compile(column.getString(Key.INDEX)) : null;
                plans.add(new ColumnPlan(type, path, constant, dateFormat));
            }
            return plans;
        }

        private static DateFormat dateFormatOf(String format)
        {
            // SimpleDateFormat is not thread safe, but a task is only read by a single thread
            return StringUtils.isBlank(format) ? null : new SimpleDateFormat(format);
        }

        /**
         * Converts a value of the document into the column it is declared as.
         *
         * @param plan the plan of a column that is read from the document
         * @param value the value matched by the json path of the column
         * @return the parsed column
         */
        private Column toColumn(ColumnPlan plan, Object value)
        {
            if (value == null) {
                return plan.type().parse(null, plan.dateFormat());
            }
            if (value instanceof Map || value instanceof List) {
                // a container cannot be converted to a scalar type, keep its json text instead of dropping it
                return plan.type().parse(jsonProvider.toJson(value), plan.dateFormat());
            }
            return plan.type().parse(String.valueOf(value), plan.dateFormat());
        }

        /**
         * Read a file in the JSON Lines layout, every line holds one json object.
         *
         * @param reader the content of the file
         * @param recordSender the sender the records are sent to
         * @return the number of records that were read
         * @throws IOException if the file cannot be read
         */
        private long parseJsonLines(BufferedReader reader, RecordSender recordSender)
                throws IOException
        {
            long recordNum = 0;
            // a column may legitimately be absent from some records, warn about it only once
            boolean[] nullWarned = new boolean[columnPlans.size()];
            String jsonLine;
            while ((jsonLine = reader.readLine()) != null) {
                if (jsonLine.isBlank()) {
                    // a blank line holds no record, skip it instead of failing the job
                    continue;
                }
                DocumentContext document = parse.parse(jsonLine);
                Record record = recordSender.createRecord();
                for (int i = 0; i < columnPlans.size(); i++) {
                    ColumnPlan plan = columnPlans.get(i);
                    if (plan.constant() != null) {
                        record.addColumn(plan.constant());
                        continue;
                    }
                    Object value = document.read(plan.path());
                    if (value == null && !nullWarned[i]) {
                        nullWarned[i] = true;
                        LOG.warn("The index [{}] matches no value in some records, the column is null there",
                                plan.path().getPath());
                    }
                    record.addColumn(toColumn(plan, value));
                }
                recordSender.sendToWriter(record);
                recordNum++;
            }
            return recordNum;
        }

        /**
         * Read a file that holds a single json document. Every index has to match a json array and one
         * record is sent per element, so that arrays of the same length line up positionally. Each of
         * them has to point at a leaf, see {@link JsonKey#SINGLE_LINE} for what that requires.
         *
         * @param reader the content of the file
         * @param recordSender the sender the records are sent to
         * @return the number of records that were read
         * @throws IOException if the file cannot be read
         */
        private long parseJsonDocument(BufferedReader reader, RecordSender recordSender)
                throws IOException
        {
            StringWriter buffer = new StringWriter(Constant.DEFAULT_BUFFER_SIZE);
            reader.transferTo(buffer);
            DocumentContext document = parse.parse(buffer.toString());

            List<List<?>> columns = new ArrayList<>(columnPlans.size());
            JsonPath counterPath = null;
            boolean hasDocumentColumn = false;
            int recordNum = -1;
            for (ColumnPlan plan : columnPlans) {
                if (plan.constant() != null) {
                    columns.add(null);
                    continue;
                }
                hasDocumentColumn = true;
                JsonPath path = plan.path();
                if (!(document.read(path) instanceof List<?> values)) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR,
                            "The index [%s] does not match a json array, when `%s` is false every index must be a multi-value path such as $.data[*].field"
                                    .formatted(path.getPath(), JsonKey.SINGLE_LINE));
                }
                if (values.isEmpty()) {
                    // an array that holds no element for this document, the column is null for every record
                    LOG.warn("The index [{}] matches no element in the document, the column is null for every record",
                            path.getPath());
                    columns.add(null);
                    continue;
                }
                if (values.stream().allMatch(Objects::isNull)) {
                    LOG.warn("The index [{}] matches only null values in the document", path.getPath());
                }
                if (recordNum < 0) {
                    counterPath = path;
                    recordNum = values.size();
                }
                else if (recordNum != values.size()) {
                    // an element of one array would end up in the record of another element
                    throw AddaxException.asAddaxException(CONFIG_ERROR,
                            "The index [%s] matches %d elements, but the index [%s] matches %d, when `%s` is false every index must match the same number of elements"
                                    .formatted(counterPath.getPath(), recordNum, path.getPath(), values.size(),
                                            JsonKey.SINGLE_LINE));
                }
                columns.add(values);
            }
            if (!hasDocumentColumn) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "No column reads a json array, when `%s` is false at least one index must do so, otherwise the number of records is unknown"
                                .formatted(JsonKey.SINGLE_LINE));
            }
            if (recordNum < 0) {
                // every index matched no element, the caller warns about the file holding no record
                recordNum = 0;
            }

            for (int i = 0; i < recordNum; i++) {
                Record record = recordSender.createRecord();
                for (int j = 0; j < columnPlans.size(); j++) {
                    ColumnPlan plan = columnPlans.get(j);
                    if (plan.constant() != null) {
                        record.addColumn(plan.constant());
                    }
                    else {
                        List<?> values = columns.get(j);
                        record.addColumn(toColumn(plan, values == null ? null : values.get(i)));
                    }
                }
                recordSender.sendToWriter(record);
            }
            return recordNum;
        }

        @Override
        public void destroy()
        {
            //
        }

        /**
         * Open a source file, honoring the configured compression type and falling back to the
         * compression detected from the content when none is configured.
         *
         * @param fileName the file to open
         * @return a reader over the decompressed content
         * @throws IOException if the file cannot be opened or read
         */
        private BufferedReader openReader(String fileName)
                throws IOException
        {
            if (StringUtils.isBlank(compress)) {
                return FileHelper.readCompressFile(fileName, encoding, Constant.DEFAULT_BUFFER_SIZE);
            }
            InputStream input = new BufferedInputStream(Files.newInputStream(Paths.get(fileName)));
            try {
                return StorageReaderUtil.createBufferedReader(input, compress, encoding, Constant.DEFAULT_BUFFER_SIZE);
            }
            catch (CompressorException e) {
                input.close();
                throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                        "The compress algorithm [" + compress + "] is unsupported yet", e);
            }
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            LOG.debug("begin to read source files...");
            for (String fileName : this.sourceFiles) {
                LOG.info("reading file : [{}]", fileName);
                try (BufferedReader reader = openReader(fileName)) {
                    long recordNum = singleLine
                            ? parseJsonLines(reader, recordSender)
                            : parseJsonDocument(reader, recordSender);
                    if (recordNum == 0) {
                        // an empty file and an index that matches nothing look the same here
                        LOG.warn("No record was read from the file [{}], check that it holds data and that the indexes match its content",
                                fileName);
                    }
                }
                catch (IOException e) {
                    throw AddaxException.asAddaxException(IO_ERROR,
                            "Failed to read the file " + fileName, e);
                }
            }
            LOG.debug("end reading source files...");
        }
    }
}
