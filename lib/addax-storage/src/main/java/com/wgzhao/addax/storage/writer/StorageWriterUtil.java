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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.ENCODING_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/**
 * Utility class for writing storage files with various compression formats.
 * Provides methods for writing compressed streams, validating configurations,
 * and converting data records to different output formats.
 *
 * @author wgzhao
 * @since 1.0.0
 */
public final class StorageWriterUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(StorageWriterUtil.class);

    /**
     * Supported write modes for file output
     */
    private static final Set<String> SUPPORTED_WRITE_MODES = Set.of("truncate", "append", "nonConflict", "overwrite");

    /**
     * Private constructor to prevent instantiation
     */
    private StorageWriterUtil()
    {
        // Utility class
    }

    /**
     * Validate writer configuration parameters including write mode, encoding,
     * compression, and field delimiter.
     *
     * @param writerConfiguration the configuration to validate
     * @throws AddaxException if validation fails
     */
    public static void validateParameter(Configuration writerConfiguration)
    {
        validateWriteMode(writerConfiguration);
        validateEncoding(writerConfiguration);
        validateCompression(writerConfiguration);
        validateFieldDelimiter(writerConfiguration);
        validateFileFormat(writerConfiguration);
    }

    /**
     * Validate the write mode parameter.
     *
     * @param writerConfiguration configuration to validate
     * @throws AddaxException if write mode is invalid
     */
    private static void validateWriteMode(Configuration writerConfiguration)
    {
        String writeMode = writerConfiguration.getNecessaryValue(Key.WRITE_MODE, REQUIRED_VALUE);
        writeMode = writeMode.trim();
        if (!SUPPORTED_WRITE_MODES.contains(writeMode)) {
            throw AddaxException.illegalConfigValue(Key.WRITE_MODE, writeMode,
                    "valid write modes " + String.join(",", SUPPORTED_WRITE_MODES));
        }
        writerConfiguration.set(Key.WRITE_MODE, writeMode);
        LOG.debug("Validated write mode: {}", writeMode);
    }

    /**
     * Validate the encoding parameter.
     *
     * @param writerConfiguration configuration to validate
     * @throws AddaxException if encoding is not supported
     */
    private static void validateEncoding(Configuration writerConfiguration)
    {
        String encoding = writerConfiguration.getString(Key.ENCODING);
        if (StringUtils.isBlank(encoding)) {
            LOG.warn("The item encoding is empty, uses [{}] as default.", Constant.DEFAULT_ENCODING);
            writerConfiguration.set(Key.ENCODING, Constant.DEFAULT_ENCODING);
        }
        else {
            try {
                encoding = encoding.trim();
                writerConfiguration.set(Key.ENCODING, encoding);
                Charsets.toCharset(encoding);
                LOG.debug("Validated encoding: {}", encoding);
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(
                        NOT_SUPPORT_TYPE,
                        String.format("The encoding [%s] is unsupported.", encoding), e);
            }
        }
    }

    /**
     * Validate the compression parameter.
     *
     * @param writerConfiguration configuration to validate
     */
    private static void validateCompression(Configuration writerConfiguration)
    {
        String compress = writerConfiguration.getString(Key.COMPRESS);
        if (StringUtils.isBlank(compress)) {
            writerConfiguration.set(Key.COMPRESS, null);
            LOG.debug("No compression specified");
        }
        else {
            LOG.debug("Compression specified: {}", compress);
        }
    }

    /**
     * Validate the field delimiter parameter.
     *
     * @param writerConfiguration configuration to validate
     * @throws AddaxException if delimiter is invalid
     */
    private static void validateFieldDelimiter(Configuration writerConfiguration)
    {
        String delimiterInStr = writerConfiguration.getString(Key.FIELD_DELIMITER);
        if (delimiterInStr != null && delimiterInStr.length() != 1) {
            throw AddaxException.illegalConfigValue(Key.FIELD_DELIMITER, delimiterInStr);
        }
        if (delimiterInStr == null) {
            LOG.warn("The item delimiter is empty, uses {} as default.", Constant.DEFAULT_FIELD_DELIMITER);
            writerConfiguration.set(Key.FIELD_DELIMITER, Constant.DEFAULT_FIELD_DELIMITER);
        }
        else {
            LOG.debug("Validated field delimiter: {}", delimiterInStr);
        }
    }

    /**
     * Validate the file format parameter.
     *
     * @param writerConfiguration configuration to validate
     * @throws AddaxException if file format is invalid
     */
    private static void validateFileFormat(Configuration writerConfiguration)
    {
        String fileFormat = writerConfiguration.getString(Key.FILE_FORMAT, Constant.DEFAULT_FILE_FORMAT);
        if (!Constant.SUPPORTED_FILE_FORMAT.contains(fileFormat)) {
            throw AddaxException.illegalConfigValue(Key.FILE_FORMAT, fileFormat,
                    "valid file formats are " + Constant.SUPPORTED_FILE_FORMAT);
        }
        LOG.debug("Validated file format: {}", fileFormat);
    }

    /**
     * Split writer configuration into multiple configurations for parallel processing.
     *
     * @param writerSliceConfig the base configuration to split
     * @param originAllFileExists set of existing file names to avoid conflicts
     * @param mandatoryNumber number of configurations to create
     * @return list of split configurations
     */
    public static List<Configuration> split(Configuration writerSliceConfig, Set<String> originAllFileExists, int mandatoryNumber)
    {
        List<Configuration> writerSplitConfigs = new ArrayList<>();
        LOG.info("Begin to split writer configuration for {} instances", mandatoryNumber);

        if (mandatoryNumber == 1) {
            writerSplitConfigs.add(writerSliceConfig);
            return writerSplitConfigs;
        }

        Set<String> allFileExists = new HashSet<>(originAllFileExists);
        String filePrefix = writerSliceConfig.getString(Key.FILE_NAME);
        String suffix = extractFileSuffix(filePrefix);

        if (filePrefix.contains(".")) {
            filePrefix = filePrefix.substring(0, filePrefix.lastIndexOf('.'));
        }

        for (int i = 0; i < mandatoryNumber; i++) {
            Configuration splitTaskConfig = writerSliceConfig.clone();
            String fullFileName = generateUniqueFileName(filePrefix, suffix, allFileExists);
            allFileExists.add(fullFileName);
            splitTaskConfig.set(Key.FILE_NAME, fullFileName);
            LOG.info("split write file name:[{}]", fullFileName);
            writerSplitConfigs.add(splitTaskConfig);
        }

        LOG.info("Finished split into {} configurations", writerSplitConfigs.size());
        return writerSplitConfigs;
    }

    /**
     * Extract file suffix from a filename.
     *
     * @param fileName the filename to extract suffix from
     * @return the file suffix including the dot, or empty string if no suffix
     */
    private static String extractFileSuffix(String fileName)
    {
        if (fileName == null || !fileName.contains(".")) {
            return "";
        }
        return fileName.substring(fileName.lastIndexOf('.'));
    }

    /**
     * Generate a unique filename by appending a unique identifier.
     *
     * @param filePrefix the base filename without extension
     * @param suffix the file extension
     * @param existingFiles set of existing filenames to avoid conflicts
     * @return a unique filename
     */
    private static String generateUniqueFileName(String filePrefix, String suffix, Set<String> existingFiles)
    {
        String fullFileName;
        do {
            fullFileName = String.format("%s_%s%s", filePrefix, FileHelper.generateFileMiddleName(), suffix);
        } while (existingFiles.contains(fullFileName));
        return fullFileName;
    }

    /**
     * Build a complete file path from components.
     *
     * @param path the directory path
     * @param fileName the file name
     * @param suffix the file suffix (can be null)
     * @return the complete file path
     */
    public static String buildFilePath(String path, String fileName, String suffix)
    {
        if (StringUtils.isBlank(path) || StringUtils.isBlank(fileName)) {
            throw new IllegalArgumentException("Path and fileName cannot be blank");
        }

        boolean isEndWithSeparator = switch (IOUtils.DIR_SEPARATOR) {
            case IOUtils.DIR_SEPARATOR_UNIX -> path.endsWith(String.valueOf(IOUtils.DIR_SEPARATOR));
            case IOUtils.DIR_SEPARATOR_WINDOWS -> path.endsWith(String.valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
            default -> false;
        };

        if (!isEndWithSeparator) {
            path = path + IOUtils.DIR_SEPARATOR;
        }

        suffix = (suffix == null) ? "" : suffix.trim();
        return path + fileName + suffix;
    }

    /**
     * Write records to an output stream with optional compression.
     *
     * @param lineReceiver receiver for reading records
     * @param outputStream the output stream to write to
     * @param config configuration for writing
     * @param fileName file name for logging and compression
     * @param taskPluginCollector collector for error handling
     * @throws AddaxException if writing fails
     */
    public static void writeToStream(RecordReceiver lineReceiver,
            OutputStream outputStream, Configuration config, String fileName,
            TaskPluginCollector taskPluginCollector)
    {
        String encoding = config.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
        String compress = config.getString(Key.COMPRESS);

        LOG.debug("Writing to stream with encoding: {}, compression: {}", encoding, compress);

        try (BufferedWriter writer = createWriter(outputStream, encoding, compress, fileName)) {
            doWriteToStream(lineReceiver, writer, config, taskPluginCollector);
        }
        catch (UnsupportedEncodingException uee) {
            throw AddaxException.asAddaxException(ENCODING_ERROR,
                    "Unsupported encoding: " + encoding, uee);
        }
        catch (CompressorException e) {
            throw AddaxException.asAddaxException(
                    NOT_SUPPORT_TYPE,
                    "Unsupported compression algorithm: " + compress, e
            );
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(
                    IO_ERROR,
                    "IO exception occurred when writing: " + fileName, e);
        }
    }

    /**
     * Create a BufferedWriter with optional compression.
     *
     * @param outputStream the output stream
     * @param encoding character encoding
     * @param compress compression type (can be null)
     * @param fileName file name for ZIP compression
     * @return BufferedWriter with compression if specified
     * @throws IOException if writer creation fails
     * @throws CompressorException if compression is not supported
     */
    private static BufferedWriter createWriter(OutputStream outputStream, String encoding,
            String compress, String fileName)
            throws IOException, CompressorException
    {
        if (compress == null || "none".equalsIgnoreCase(compress)) {
            return new BufferedWriter(new OutputStreamWriter(outputStream, encoding));
        }

        // Normalize compress name for compatibility
        String normalizedCompress = switch (compress.toLowerCase()) {
            case "gzip" -> "gz";
            case "bz2" -> "bzip2";
            default -> compress.toLowerCase();
        };

        if ("zip".equals(normalizedCompress)) {
            ZipCycleOutputStream zos = new ZipCycleOutputStream(outputStream, fileName);
            return new BufferedWriter(new OutputStreamWriter(zos, encoding));
        }

        var compressorOutputStream = new CompressorStreamFactory()
                .createCompressorOutputStream(normalizedCompress, outputStream);
        return new BufferedWriter(new OutputStreamWriter(compressorOutputStream, encoding));
    }

    /**
     * Write records to a BufferedWriter using the specified format.
     *
     * @param lineReceiver receiver for reading records
     * @param writer the writer to write to
     * @param config configuration for writing
     * @param taskPluginCollector collector for error handling
     * @throws IOException if writing fails
     */
    private static void doWriteToStream(RecordReceiver lineReceiver,
            BufferedWriter writer, Configuration config,
            TaskPluginCollector taskPluginCollector)
            throws IOException
    {
        String fileFormat = config.getString(Key.FILE_FORMAT, Constant.DEFAULT_FILE_FORMAT);

        if (Constant.SQL_FORMAT.equals(fileFormat)) {
            writeToSql(lineReceiver, writer, config);
            return;
        }

        writeToCSV(lineReceiver, writer, config, taskPluginCollector);
    }

    /**
     * Write records in CSV format.
     *
     * @param lineReceiver receiver for reading records
     * @param writer the writer to write to
     * @param config configuration for writing
     * @param taskPluginCollector collector for error handling
     * @throws IOException if writing fails
     */
    private static void writeToCSV(RecordReceiver lineReceiver, BufferedWriter writer,
            Configuration config, TaskPluginCollector taskPluginCollector)
            throws IOException
    {
        CSVFormat.Builder csvBuilder = CSVFormat.DEFAULT.builder();
        csvBuilder.setRecordSeparator(IOUtils.LINE_SEPARATOR_UNIX);

        String nullFormat = config.getString(Key.NULL_FORMAT);
        csvBuilder.setNullString(nullFormat);

        String dateFormat = config.getString(Key.DATE_FORMAT);
        DateFormat dateParse = StringUtils.isNotBlank(dateFormat) ? new SimpleDateFormat(dateFormat) : null;

        // Validate and set field delimiter
        String delimiterInStr = config.getString(Key.FIELD_DELIMITER);
        if (delimiterInStr != null && delimiterInStr.length() != 1) {
            throw AddaxException.illegalConfigValue(Key.FIELD_DELIMITER, delimiterInStr);
        }
        char fieldDelimiter = config.getChar(Key.FIELD_DELIMITER, Constant.DEFAULT_FIELD_DELIMITER);
        csvBuilder.setDelimiter(fieldDelimiter);

        // Handle headers
        List<String> headers = config.getList(Key.HEADER, String.class);
        if (headers != null && !headers.isEmpty()) {
            csvBuilder.setHeader(headers.toArray(new String[0]));
        }

        Record record;
        try (CSVPrinter csvPrinter = new CSVPrinter(writer, csvBuilder.build())) {
            while ((record = lineReceiver.getFromReader()) != null) {
                List<String> result = recordToList(record, nullFormat, dateParse, taskPluginCollector);
                if (result != null) {
                    csvPrinter.printRecord(result);
                }
            }
        }
    }

    /**
     * Convert a record to a list of string values.
     *
     * @param record the record to convert
     * @param nullFormat format for null values
     * @param dateParse date formatter (can be null)
     * @param taskPluginCollector collector for error handling
     * @return list of string values, or null if conversion fails
     */
    public static List<String> recordToList(Record record, String nullFormat, DateFormat dateParse, TaskPluginCollector taskPluginCollector)
    {
        try {
            List<String> splitRows = new ArrayList<>();
            int recordLength = record.getColumnNumber();

            for (int i = 0; i < recordLength; i++) {
                Column column = record.getColumn(i);
                String value = convertColumnToString(column, nullFormat, dateParse);
                splitRows.add(value);
            }

            return splitRows;
        }
        catch (Exception e) {
            LOG.warn("Failed to convert record to list", e);
            taskPluginCollector.collectDirtyRecord(record, e.getMessage());
            return null;
        }
    }

    /**
     * Convert a column to its string representation.
     *
     * @param column the column to convert
     * @param nullFormat format for null values
     * @param dateParse date formatter (can be null)
     * @return string representation of the column
     */
    private static String convertColumnToString(Column column, String nullFormat, DateFormat dateParse)
    {
        if (column == null || column.getRawData() == null || column.asString().equals(nullFormat)) {
            return nullFormat;
        }

        boolean isDateColumn = column instanceof DateColumn || column instanceof TimestampColumn;
        if (!isDateColumn) {
            return column.asString();
        }

        if (dateParse != null) {
            return dateParse.format(column.asDate());
        }

        return column.asString();
    }

    /**
     * Write records in SQL INSERT format.
     *
     * @param lineReceiver receiver for reading records
     * @param writer the writer to write to
     * @param config configuration for writing
     * @throws IOException if writing fails
     * @throws AddaxException if configuration is invalid
     */
    public static void writeToSql(RecordReceiver lineReceiver, BufferedWriter writer, Configuration config)
            throws IOException
    {
        String tableName = config.getNecessaryValue(Key.TABLE, REQUIRED_VALUE);
        List<String> columns = config.getList(Key.COLUMN, String.class);
        boolean extendedInsert = config.getBool(Key.EXTENDED_INSERT, true);
        int batchSize = config.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);

        String sqlHeader = buildSqlHeader(tableName, columns);
        StringBuilder sb = new StringBuilder(sqlHeader).append(" VALUES (");
        int currentBatchSize = 0;

        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            appendRecordValues(record, sb);

            if (extendedInsert) {
                currentBatchSize++;
                if (currentBatchSize >= batchSize) {
                    writeAndResetBuffer(writer, sb, sqlHeader);
                    currentBatchSize = 0;
                }
                else {
                    sb.append("), (");
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
        if (currentBatchSize > 0) {
            sb.delete(sb.length() - 3, sb.length()).append(";\n");
            writer.write(sb.toString());
        }
    }

    /**
     * Build the SQL header for INSERT statements.
     *
     * @param tableName the table name
     * @param columns list of column names (can be null)
     * @return SQL header string
     */
    private static String buildSqlHeader(String tableName, List<String> columns)
    {
        if (columns != null && !columns.isEmpty()) {
            return String.format("INSERT INTO %s(%s)", tableName, String.join(",", columns));
        }
        return String.format("INSERT INTO %s", tableName);
    }

    /**
     * Validate that record column count matches expected columns.
     *
     * @param record the record to validate
     * @param columns expected columns (can be null)
     * @throws AddaxException if column count mismatch
     */
    private static void validateRecordColumns(Record record, List<String> columns)
    {
        if (columns != null && record.getColumnNumber() != columns.size()) {
            throw AddaxException.asAddaxException(
                    CONFIG_ERROR,
                    String.format("Column count mismatch: record has %d columns but table has %d columns",
                            record.getColumnNumber(), columns.size())
            );
        }
    }

    /**
     * Append record values to SQL statement.
     *
     * @param record the record to append
     * @param sb the StringBuilder to append to
     */
    private static void appendRecordValues(Record record, StringBuilder sb)
    {
        for (int i = 0; i < record.getColumnNumber(); i++) {
            Column column = record.getColumn(i);

            // Numeric and boolean columns don't need quotes
            if (column instanceof LongColumn || column instanceof BoolColumn) {
                sb.append(column.asString());
            }
            else {
                // Escape single quotes in string values
                String value = column.asString();
                if (value != null) {
                    value = value.replace("'", "''");
                }
                sb.append("'").append(value).append("'");
            }

            if (i < record.getColumnNumber() - 1) {
                sb.append(",");
            }
        }
    }

    /**
     * Write buffer content and reset for next batch.
     *
     * @param writer the writer to write to
     * @param sb the StringBuilder to write and reset
     * @param sqlHeader SQL header for next batch
     * @throws IOException if writing fails
     */
    private static void writeAndResetBuffer(BufferedWriter writer, StringBuilder sb, String sqlHeader)
            throws IOException
    {
        sb.delete(sb.length() - 3, sb.length()).append(";\n");
        writer.write(sb.toString());
        sb.setLength(0);
        sb.append(sqlHeader).append(" VALUES (");
    }
}
