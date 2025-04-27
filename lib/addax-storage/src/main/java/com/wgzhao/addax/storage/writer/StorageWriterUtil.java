/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.wgzhao.addax.storage.writer;

import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.compress.ZipCycleOutputStream;
import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.TimestampColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.storage.util.FileHelper;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.ENCODING_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

public class StorageWriterUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(StorageWriterUtil.class);
    private static final Set<String> supportedWriteModes = Set.of("truncate", "append", "nonConflict", "overwrite");

    private StorageWriterUtil()
    {

    }

    /*
     * check parameter: writeMode, encoding, compress, filedDelimiter
     */
    public static void validateParameter(Configuration writerConfiguration)
    {
        // writeMode check
        String writeMode = writerConfiguration.getNecessaryValue(Key.WRITE_MODE, REQUIRED_VALUE);
        writeMode = writeMode.trim();
        if (!supportedWriteModes.contains(writeMode)) {
            throw AddaxException.illegalConfigValue(Key.WRITE_MODE, writeMode, "valid write modes " + StringUtils.join(supportedWriteModes, ","));
        }
        writerConfiguration.set(Key.WRITE_MODE, writeMode);

        // encoding check
        String encoding = writerConfiguration.getString(Key.ENCODING);
        if (StringUtils.isBlank(encoding)) {
            // like "  ", null
            LOG.warn("The item encoding is empty, uses [{}] as default.", Constant.DEFAULT_ENCODING);
            writerConfiguration.set(Key.ENCODING, Constant.DEFAULT_ENCODING);
        }
        else {
            try {
                encoding = encoding.trim();
                writerConfiguration.set(Key.ENCODING, encoding);
                Charsets.toCharset(encoding);
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(
                        NOT_SUPPORT_TYPE,
                        String.format("The encoding [%s] is unsupported.", encoding), e);
            }
        }

        // only support compress types
        String compress = writerConfiguration.getString(Key.COMPRESS);
        if (StringUtils.isBlank(compress)) {
            writerConfiguration.set(Key.COMPRESS, null);
        }

        // fieldDelimiter check
        String delimiterInStr = writerConfiguration.getString(Key.FIELD_DELIMITER);
        // warn: if it has, length must be one
        if (null != delimiterInStr && 1 != delimiterInStr.length()) {
            throw AddaxException.illegalConfigValue(Key.FIELD_DELIMITER, delimiterInStr);
        }
        if (null == delimiterInStr) {
            LOG.warn("The item delimiter is empty, uses {} as default.", Constant.DEFAULT_FIELD_DELIMITER);
            writerConfiguration.set(Key.FIELD_DELIMITER, Constant.DEFAULT_FIELD_DELIMITER);
        }

        // fileFormat check
        String fileFormat = writerConfiguration.getString(Key.FILE_FORMAT, Constant.DEFAULT_FILE_FORMAT);
        if (!Constant.SUPPORTED_FILE_FORMAT.contains(fileFormat)) {
            throw AddaxException.illegalConfigValue(Key.FILE_NAME, fileFormat, "valid file format are " + Constant.SUPPORTED_FILE_FORMAT.toString());
        }
    }

    public static List<Configuration> split(Configuration writerSliceConfig, Set<String> originAllFileExists, int mandatoryNumber)
    {
        List<Configuration> writerSplitConfigs = new ArrayList<>();
        LOG.info("Begin to split...");
        if (mandatoryNumber == 1) {
            writerSplitConfigs.add(writerSliceConfig);
            return writerSplitConfigs;
        }

        Set<String> allFileExists = new HashSet<>(originAllFileExists);

        String filePrefix = writerSliceConfig.getString(Key.FILE_NAME);
        String suffix = "";
        if (filePrefix.contains(".")) {
            String[] split = filePrefix.split("\\.");
            filePrefix = split[0];
            suffix = "." + split[1];
        }
        for (int i = 0; i < mandatoryNumber; i++) {
            // handle same file name
            Configuration splitTaskConfig = writerSliceConfig.clone();
            String fullFileName;
            do {
                fullFileName = String.format("%s_%s%s", filePrefix, FileHelper.generateFileMiddleName(), suffix);
            }
            while (allFileExists.contains(fullFileName));
            allFileExists.add(fullFileName);
            splitTaskConfig.set(Key.FILE_NAME, fullFileName);
            LOG.info("split write file name:[{}]", fullFileName);
            writerSplitConfigs.add(splitTaskConfig);
        }
        LOG.info("Finished split.");
        return writerSplitConfigs;
    }

    public static String buildFilePath(String path, String fileName, String suffix)
    {
        boolean isEndWithSeparator = switch (IOUtils.DIR_SEPARATOR) {
            case IOUtils.DIR_SEPARATOR_UNIX -> path.endsWith(String.valueOf(IOUtils.DIR_SEPARATOR));
            case IOUtils.DIR_SEPARATOR_WINDOWS -> path.endsWith(String.valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
            default -> false;
        };

        if (!isEndWithSeparator) {
            path = path + IOUtils.DIR_SEPARATOR;
        }

        suffix = (suffix == null) ? "" : suffix.trim();
        return "%s%s%s".formatted(path, fileName, suffix);
    }

    public static void writeToStream(RecordReceiver lineReceiver,
            OutputStream outputStream, Configuration config, String fileName,
            TaskPluginCollector taskPluginCollector)
    {
        String encoding = config.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);

        String compress = config.getString(Key.COMPRESS);

        try (var writer = createWriter(outputStream, encoding, compress, fileName)) {
            StorageWriterUtil.doWriteToStream(lineReceiver, writer, fileName, config, taskPluginCollector);
        }
        catch (UnsupportedEncodingException uee) {
            throw AddaxException.asAddaxException(ENCODING_ERROR,
                    "Unsupported encoding: " + encoding, uee);
        }
        catch (CompressorException e) {
            throw AddaxException.asAddaxException(
                    NOT_SUPPORT_TYPE,
                    "Unsupported compression algorithm: " + compress
            );
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(
                    IO_ERROR,
                    "IO exception occurred when writing: " + fileName, e);
        }
    }

    private static BufferedWriter createWriter(OutputStream outputStream, String encoding,
            String compress, String fileName)
            throws IOException, CompressorException
    {
        if (compress == null) {
            return new BufferedWriter(new OutputStreamWriter(outputStream, encoding));
        }

        // Normalize compress name
        compress = switch (compress.toLowerCase()) {
            case "gzip" -> "gz";
            case "bz2" -> "bzip2";
            default -> compress;
        };

        if ("zip".equals(compress)) {
            var zis = new ZipCycleOutputStream(outputStream, fileName);
            return new BufferedWriter(new OutputStreamWriter(zis, encoding));
        }

        var compressorOutputStream = new CompressorStreamFactory()
                .createCompressorOutputStream(compress, outputStream);
        return new BufferedWriter(new OutputStreamWriter(compressorOutputStream, encoding));
    }

    private static void doWriteToStream(RecordReceiver lineReceiver,
            BufferedWriter writer, String context, Configuration config,
            TaskPluginCollector taskPluginCollector)
            throws IOException
    {
        CSVFormat.Builder csvBuilder = CSVFormat.DEFAULT.builder();
        csvBuilder.setRecordSeparator(IOUtils.LINE_SEPARATOR_UNIX);
        String nullFormat = config.getString(Key.NULL_FORMAT);
        csvBuilder.setNullString(nullFormat);
        String dateFormat = config.getString(Key.DATE_FORMAT);
        DateFormat dateParse = null; //
        if (StringUtils.isNotBlank(dateFormat)) {
            dateParse = new SimpleDateFormat(dateFormat);
        }

        // warn: default false
        String fileFormat = config.getString(Key.FILE_FORMAT, Constant.DEFAULT_FILE_FORMAT);
        if (Objects.equals(fileFormat, Constant.SQL_FORMAT)) {
            writeToSql(lineReceiver, writer, config);
            return;
        }

        String delimiterInStr = config.getString(Key.FIELD_DELIMITER);
        if (null != delimiterInStr && 1 != delimiterInStr.length()) {
            throw AddaxException.illegalConfigValue(Key.FIELD_DELIMITER, delimiterInStr);
        }
        if (null == delimiterInStr) {
            LOG.warn("Take the  {}  as the value of {}", Constant.DEFAULT_FIELD_DELIMITER, Key.FIELD_DELIMITER);
        }

        // warn: fieldDelimiter could not be '' for no fieldDelimiter
        char fieldDelimiter = config.getChar(Key.FIELD_DELIMITER, Constant.DEFAULT_FIELD_DELIMITER);
        csvBuilder.setDelimiter(fieldDelimiter);

        List<String> headers = config.getList(Key.HEADER, String.class);
        if (null != headers && !headers.isEmpty()) {
            csvBuilder.setHeader(headers.toArray(new String[0]));
        }

        Record record;
        CSVPrinter csvPrinter = new CSVPrinter(writer, csvBuilder.build());
        while ((record = lineReceiver.getFromReader()) != null) {
            final List<String> result = recordToList(record, nullFormat, dateParse, taskPluginCollector);
            if (result != null) {
                csvPrinter.printRecord(result);
            }
        }
        csvPrinter.close();
    }

    public static List<String> recordToList(Record record, String nullFormat, DateFormat dateParse, TaskPluginCollector taskPluginCollector)
    {
        try {
            List<String> splitRows = new ArrayList<>();
            int recordLength = record.getColumnNumber();
            if (0 != recordLength) {
                Column column;
                for (int i = 0; i < recordLength; i++) {
                    column = record.getColumn(i);
                    if (null == column || null == column.getRawData() || column.asString().equals(nullFormat)) {
                        // warn: it's all ok if nullFormat is null
                        splitRows.add(nullFormat);
                    }
                    else {
                        // warn: it's all ok if nullFormat is null
                        boolean isDateColumn = column instanceof DateColumn || column instanceof TimestampColumn;
                        if (!isDateColumn) {
                            splitRows.add(column.asString());
                        }
                        else {
                            if (null != dateParse) {
                                splitRows.add(dateParse.format(column.asDate()));
                            }
                            else {
                                splitRows.add(column.asString());
                            }
                        }
                    }
                }
            }
            return splitRows;
        }
        catch (Exception e) {
            // warn: dirty data
            taskPluginCollector.collectDirtyRecord(record, e);
            return null;
        }
    }

    public static void writeToSql(RecordReceiver lineReceiver, BufferedWriter writer, Configuration config)
            throws IOException
    {
        // sql format required table and column name and optional extendedInsert and optional batchSize
        String tableName = config.getNecessaryValue(Key.TABLE, REQUIRED_VALUE);
        String existColumns = config.getString(Key.COLUMN, null);
        List<String> columns = null;
        if (existColumns != null) {
            columns = config.getList(Key.COLUMN, String.class);
        }
        boolean extendedInsert = config.getBool(Key.EXTENDED_INSERT, true);
        int batchSize = config.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);

        String sqlHeader = existColumns != null
                ? "INSERT INTO %s(%s)".formatted(tableName, String.join(",", columns))
                : "INSERT INTO %s".formatted(tableName);

        var sb = new StringBuilder(sqlHeader).append(" VALUES (");
        int curNum = 0;

        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            if (columns != null && record.getColumnNumber() != columns.size()) {
                throw AddaxException.asAddaxException(
                        CONFIG_ERROR,
                        "Column count mismatch: record has %d columns but table has %d columns"
                                .formatted(record.getColumnNumber(), columns.size())
                );
            }

            appendRecordValues(record, sb);

            if (extendedInsert) {
                if (curNum >= batchSize) {
                    writeAndResetBuffer(writer, sb, sqlHeader);
                    curNum = 0;
                }
                else {
                    sb.append("), (");
                    curNum++;
                }
            }
            else {
                sb.append(");\n");
                writer.write(sb.toString());
                sb.setLength(0);
                sb.append(sqlHeader).append(" VALUES (");
            }
        }

        // Write remaining records
        if (curNum > 0) {
            sb.delete(sb.length() - 3, sb.length()).append(";");
            writer.write(sb.toString());
        }
    }

    private static void appendRecordValues(Record record, StringBuilder sb)
    {
        for (int i = 0; i < record.getColumnNumber(); i++) {
            var column = record.getColumn(i);
            if (column instanceof LongColumn || column instanceof BoolColumn) {
                sb.append(column.asString());
            }
            else {
                sb.append("'").append(column.asString()).append("'");
            }
            if (i < record.getColumnNumber() - 1) {
                sb.append(",");
            }
        }
    }

    private static void writeAndResetBuffer(BufferedWriter writer, StringBuilder sb, String sqlHeader)
            throws IOException
    {
        sb.append(";\n");
        writer.write(sb.toString());
        sb.setLength(0);
        sb.append(sqlHeader).append(" VALUES (");
    }
}
