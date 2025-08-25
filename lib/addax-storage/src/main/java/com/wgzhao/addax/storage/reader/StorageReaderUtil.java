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

package com.wgzhao.addax.storage.reader;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.compress.ExpandLzopInputStream;
import com.wgzhao.addax.core.compress.ZipCycleInputStream;
import com.wgzhao.addax.core.constant.Type;
import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.ColumnEntry;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.ENCODING_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

/**
 * Utility class for reading storage files with various compression formats.
 * Provides methods for reading compressed streams, validating configurations,
 * and converting data records.
 *
 * @author wgzhao
 * @since 1.0.0
 */
public final class StorageReaderUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(StorageReaderUtil.class);

    // Compression type constants
    private static final String COMPRESS_NONE = "none";
    private static final String COMPRESS_ZIP = "zip";
    private static final String COMPRESS_LZO = "lzo";

    /**
     * Private constructor to prevent instantiation
     */
    private StorageReaderUtil()
    {
        // Utility class
    }

    /**
     * Read data from an input stream with optional compression support.
     * This method handles various compression formats and delegates to
     * {@link #doReadFromStream} for actual data processing.
     *
     * @param inputStream the input stream to read from
     * @param fileName the name of the file being read (for logging)
     * @param readerSliceConfig configuration for the reader
     * @param recordSender sender for processed records
     * @param taskPluginCollector collector for error handling
     * @throws AddaxException if reading fails
     */
    public static void readFromStream(InputStream inputStream, String fileName,
            Configuration readerSliceConfig, RecordSender recordSender,
            TaskPluginCollector taskPluginCollector)
    {
        String compress = readerSliceConfig.getString(Key.COMPRESS, "");
        String encoding = readerSliceConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);

        // Handle blank encoding
        if (StringUtils.isBlank(encoding)) {
            encoding = Constant.DEFAULT_ENCODING;
            LOG.warn("The encoding: [{}] is illegal, uses [{}] by default", encoding, Constant.DEFAULT_ENCODING);
        }

        List<Configuration> column = readerSliceConfig.getListConfiguration(Key.COLUMN);
        // Handle ["*"] -> [], null
        if (column != null && column.size() == 1 && "\"*\"".equals(column.get(0).toString())) {
            readerSliceConfig.set(Key.COLUMN, null);
        }

        int bufferSize = readerSliceConfig.getInt(Key.BUFFER_SIZE, Constant.DEFAULT_BUFFER_SIZE);

        // Process with compression support
        try (BufferedReader reader = createBufferedReader(inputStream, compress, encoding, bufferSize)) {
            doReadFromStream(reader, fileName, readerSliceConfig, recordSender, taskPluginCollector);
        }
        catch (UnsupportedEncodingException uee) {
            throw AddaxException.asAddaxException(
                    ENCODING_ERROR,
                    String.format("%s is unsupported", encoding), uee);
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(
                    IO_ERROR, String.format("Failed to read stream [%s].", fileName), e);
        }
        catch (CompressorException e) {
            throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                    "The compress algorithm [" + compress + "] is unsupported yet"
            );
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
    }

    /**
     * Create a BufferedReader with appropriate compression handling.
     *
     * @param inputStream the input stream
     * @param compress compression type
     * @param encoding character encoding
     * @param bufferSize buffer size for the reader
     * @return BufferedReader with compression support
     * @throws IOException if stream creation fails
     * @throws CompressorException if compression is not supported
     */
    private static BufferedReader createBufferedReader(InputStream inputStream, String compress,
            String encoding, int bufferSize)
            throws IOException, CompressorException
    {
        if (StringUtils.isBlank(compress) || COMPRESS_NONE.equalsIgnoreCase(compress)) {
            return new BufferedReader(new InputStreamReader(inputStream, encoding), bufferSize);
        }

        return switch (compress.toLowerCase()) {
            case COMPRESS_ZIP -> {
                ZipCycleInputStream zipCycleInputStream = new ZipCycleInputStream(inputStream);
                yield new BufferedReader(new InputStreamReader(zipCycleInputStream, encoding), bufferSize);
            }
            case COMPRESS_LZO -> {
                ExpandLzopInputStream expandLzopInputStream = new ExpandLzopInputStream(inputStream);
                yield new BufferedReader(new InputStreamReader(expandLzopInputStream, encoding), bufferSize);
            }
            default -> {
                // Apache Commons Compress supports most compression algorithms
                CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(
                        compress.toUpperCase(), inputStream, true);
                yield new BufferedReader(new InputStreamReader(input, encoding), bufferSize);
            }
        };
    }

    /**
     * Read and process data from a BufferedReader using CSV parsing.
     *
     * @param reader the buffered reader to read from
     * @param fileName the file name for logging
     * @param readerSliceConfig configuration for reading
     * @param recordSender sender for processed records
     * @param taskPluginCollector collector for error handling
     */
    public static void doReadFromStream(BufferedReader reader, String fileName,
            Configuration readerSliceConfig, RecordSender recordSender,
            TaskPluginCollector taskPluginCollector)
    {
        // Configure CSV parser
        CSVFormat.Builder csvFormatBuilder = CSVFormat.DEFAULT.builder();

        // Get field delimiter
        String delimiterInStr = readerSliceConfig.getString(Key.FIELD_DELIMITER);
        if (delimiterInStr != null && delimiterInStr.length() != 1) {
            throw AddaxException.illegalConfigValue(Key.FIELD_DELIMITER, delimiterInStr);
        }
        if (delimiterInStr == null) {
            LOG.warn("Uses [{}] as delimiter by default", Constant.DEFAULT_FIELD_DELIMITER);
        }

        // Note: default value is ',', fieldDelimiter could be \n(lineDelimiter) for no fieldDelimiter
        Character fieldDelimiter = readerSliceConfig.getChar(Key.FIELD_DELIMITER, Constant.DEFAULT_FIELD_DELIMITER);
        csvFormatBuilder.setDelimiter(fieldDelimiter);

        // Null format configuration, note: no default value '\N'
        String nullFormat = readerSliceConfig.getString(Key.NULL_FORMAT);
        csvFormatBuilder.setNullString(nullFormat);

        // Header handling
        if (readerSliceConfig.getBool(Key.SKIP_HEADER, Constant.DEFAULT_SKIP_HEADER)) {
            csvFormatBuilder.setHeader();
            csvFormatBuilder.setSkipHeaderRecord(true);
        }

        List<ColumnEntry> column = getListColumnEntry(readerSliceConfig, Key.COLUMN);

        // Process each line
        try (CSVParser csvParser = CSVParser.parse(reader, csvFormatBuilder.get())) {
            csvParser.stream()
                    .filter(Objects::nonNull)
                    .forEach(csvRecord -> transportOneRecord(
                            recordSender, column, csvRecord.toList().toArray(new String[0]),
                            nullFormat, taskPluginCollector)
                    );
        }
        catch (UnsupportedEncodingException uee) {
            String encoding = readerSliceConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
            throw AddaxException.asAddaxException(
                    ENCODING_ERROR, String.format("The encoding %s is unsupported", encoding), uee);
        }
        catch (FileNotFoundException fnfe) {
            throw AddaxException.asAddaxException(
                    IO_ERROR, String.format("The file [%s] does not exist", fileName), fnfe);
        }
        catch (IOException ioe) {
            throw AddaxException.asAddaxException(
                    IO_ERROR, String.format("Failed to read file [%s]", fileName), ioe);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
    }

    /**
     * Transport one record by parsing a single line of text.
     *
     * @param recordSender sender for the processed record
     * @param configuration configuration containing column and delimiter info
     * @param taskPluginCollector collector for error handling
     * @param line the line of text to parse
     */
    public static void transportOneRecord(RecordSender recordSender, Configuration configuration,
            TaskPluginCollector taskPluginCollector, String line)
    {
        List<ColumnEntry> column = getListColumnEntry(configuration, Key.COLUMN);
        // The nullFormat has no default value
        String nullFormat = configuration.getString(Key.NULL_FORMAT);

        // Note: default value is ',', fieldDelimiter could be \n(lineDelimiter) for no fieldDelimiter
        Character fieldDelimiter = configuration.getChar(Key.FIELD_DELIMITER, Constant.DEFAULT_FIELD_DELIMITER);

        String[] sourceLine = StringUtils.split(line, fieldDelimiter);

        transportOneRecord(recordSender, column, sourceLine, nullFormat, taskPluginCollector);
    }

    /**
     * Transport one record by converting source data to appropriate column types.
     *
     * @param recordSender sender for the processed record
     * @param columnConfigs list of column configurations
     * @param sourceLine array of source values
     * @param nullFormat format string representing null values
     * @param taskPluginCollector collector for error handling
     */
    public static void transportOneRecord(RecordSender recordSender, List<ColumnEntry> columnConfigs,
            String[] sourceLine, String nullFormat, TaskPluginCollector taskPluginCollector)
    {
        Record record = recordSender.createRecord();

        if (columnConfigs == null || columnConfigs.isEmpty()) {
            // No column configuration - treat all as strings
            for (String columnValue : sourceLine) {
                // Note: not equalsIgnoreCase, it's all ok if nullFormat is null
                Column columnGenerated = columnValue.equals(nullFormat)
                    ? new StringColumn(null)
                    : new StringColumn(columnValue);
                record.addColumn(columnGenerated);
            }
            recordSender.sendToWriter(record);
            return;
        }

        try {
            for (ColumnEntry columnConfig : columnConfigs) {
                String columnType = columnConfig.getType();
                Integer columnIndex = columnConfig.getIndex();
                String columnConst = columnConfig.getValue();

                String columnValue = getColumnValue(columnIndex, columnConst, sourceLine);
                Column columnGenerated = createTypedColumn(columnValue, columnType, columnConfig, nullFormat);
                record.addColumn(columnGenerated);
            }
            recordSender.sendToWriter(record);
        }
        catch (IllegalArgumentException | IndexOutOfBoundsException iae) {
            LOG.error(iae.getMessage());
            taskPluginCollector.collectDirtyRecord(record, iae.getMessage());
        }
        catch (Exception e) {
            if (e instanceof AddaxException) {
                throw (AddaxException) e;
            }
            // Each record which transfer failed should be regarded as dirty
            taskPluginCollector.collectDirtyRecord(record, e.getMessage());
        }
    }

    /**
     * Get the column value based on configuration.
     *
     * @param columnIndex index of the column in source line
     * @param columnConst constant value if specified
     * @param sourceLine source data array
     * @return the column value
     * @throws AddaxException if configuration is invalid
     * @throws IndexOutOfBoundsException if column index is out of range
     */
    private static String getColumnValue(Integer columnIndex, String columnConst, String[] sourceLine)
    {
        if (columnIndex == null && columnConst == null) {
            throw AddaxException.asAddaxException(
                    CONFIG_ERROR, "The index or constant is required when type is present.");
        }

        if (columnIndex != null && columnConst != null) {
            throw AddaxException.asAddaxException(
                    CONFIG_ERROR, "The index and value are both present, choose one of them");
        }

        if (columnIndex != null) {
            if (columnIndex >= sourceLine.length) {
                throw new IndexOutOfBoundsException(String.format(
                        "The column index [%s] you try to read is out of range[%s]: [%s]",
                        columnIndex + 1, sourceLine.length, String.join(",", sourceLine)));
            }
            return sourceLine[columnIndex];
        }

        return columnConst;
    }

    /**
     * Create a typed column based on the configuration.
     *
     * @param columnValue the string value to convert
     * @param columnType the target column type
     * @param columnConfig column configuration
     * @param nullFormat format representing null values
     * @return the typed column
     * @throws AddaxException if type conversion fails
     */
    private static Column createTypedColumn(String columnValue, String columnType,
            ColumnEntry columnConfig, String nullFormat)
    {
        // Handle null values
        if (columnValue == null || columnValue.equals(nullFormat)) {
            return new StringColumn();
        }

        Type type = Type.valueOf(columnType.toUpperCase());

        try {
            return switch (type) {
                case STRING -> new StringColumn(columnValue);
                case LONG -> new LongColumn(columnValue);
                case DOUBLE -> new DoubleColumn(columnValue);
                case BOOLEAN -> new BoolColumn(columnValue);
                case DATE -> createDateColumn(columnValue, columnConfig);
            };
        }
        catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("Cast value [%s] to type [%s] failure", columnValue, type.name()), e);
        }
    }

    /**
     * Create a DateColumn with optional format parsing.
     *
     * @param columnValue the string value to parse
     * @param columnConfig column configuration containing format info
     * @return DateColumn instance
     * @throws ParseException if date parsing fails
     */
    private static DateColumn createDateColumn(String columnValue, ColumnEntry columnConfig)
            throws ParseException
    {
        String formatString = columnConfig.getFormat();
        if (StringUtils.isNotBlank(formatString)) {
            DateFormat format = columnConfig.getDateFormat();
            return new DateColumn(format.parse(columnValue));
        }
        else {
            return new DateColumn(new StringColumn(columnValue).asDate());
        }
    }

    /**
     * Convert a list of JSONObjects to ColumnEntry objects.
     *
     * @param configuration configuration containing the list
     * @param path the path to the list in configuration
     * @return list of ColumnEntry objects, or null if not found
     */
    public static List<ColumnEntry> getListColumnEntry(Configuration configuration, final String path)
    {
        List<JSONObject> lists = configuration.getList(path, JSONObject.class);
        if (lists == null) {
            return null;
        }

        List<ColumnEntry> result = new ArrayList<>();
        for (final JSONObject object : lists) {
            result.add(JSON.parseObject(object.toJSONString(), ColumnEntry.class));
        }
        return result;
    }

    /**
     * Validate reader configuration parameters including encoding, compression, and field delimiter.
     *
     * @param readerConfiguration the configuration to validate
     */
    public static void validateParameter(Configuration readerConfiguration)
    {
        validateEncoding(readerConfiguration);
        validateFieldDelimiter(readerConfiguration);
        validateColumn(readerConfiguration);
    }

    /**
     * Validate the encoding parameter.
     *
     * @param readerConfiguration configuration to validate
     * @throws AddaxException if encoding is not supported
     */
    public static void validateEncoding(Configuration readerConfiguration)
    {
        // Encoding check
        String encoding = readerConfiguration.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
        try {
            encoding = encoding.trim();
            readerConfiguration.set(Key.ENCODING, encoding);
            Charsets.toCharset(encoding);
        }
        catch (UnsupportedCharsetException uce) {
            throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                    String.format("The encoding [%s] is unsupported yet.", encoding), uce);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    String.format("Exception occurred while applying encoding [%s].", e.getMessage()), e);
        }
    }

    /**
     * Validate the field delimiter parameter.
     *
     * @param readerConfiguration configuration to validate
     * @throws AddaxException if delimiter is invalid
     */
    public static void validateFieldDelimiter(Configuration readerConfiguration)
    {
        // Field delimiter check
        String delimiterInStr = readerConfiguration.getString(Key.FIELD_DELIMITER, ",");
        if (delimiterInStr == null) {
            throw AddaxException.asAddaxException(REQUIRED_VALUE,
                    String.format("The item [%s] is required.", Key.FIELD_DELIMITER));
        }
        else if (delimiterInStr.length() != 1) {
            // Note: if it has value, length must be one
            throw AddaxException.illegalConfigValue(Key.FIELD_DELIMITER, delimiterInStr);
        }
    }

    /**
     * Validate column configuration.
     *
     * @param readerConfiguration configuration to validate
     */
    public static void validateColumn(Configuration readerConfiguration)
    {
        // Column validation: 1. index type 2. value type 3. when type is Date, may have format
        List<Configuration> columns = readerConfiguration.getListConfiguration(Key.COLUMN);
        if (columns == null || columns.isEmpty()) {
            throw AddaxException.missingConfig(Key.COLUMN);
        }

        // Handle ["*"]
        if (columns.size() == 1) {
            String columnsInStr = columns.get(0).toString();
            if ("\"*\"".equals(columnsInStr) || "'*'".equals(columnsInStr)) {
                readerConfiguration.set(Key.COLUMN, null);
                return;
            }
        }

        for (Configuration eachColumnConf : columns) {
            eachColumnConf.getNecessaryValue(Key.TYPE, REQUIRED_VALUE);
            Integer columnIndex = eachColumnConf.getInt(Key.INDEX);
            String columnValue = eachColumnConf.getString(Key.VALUE);

            if (columnIndex == null && columnValue == null) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "You must configure one of index or name or value");
            }

            if (columnIndex != null && columnValue != null) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "You both configure index, value, or name, you can ONLY specify one for each column");
            }

            if (columnIndex != null && columnIndex < 0) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("The value of index must be greater than 0, %s is illegal", columnIndex));
            }
        }
    }

    /**
     * Get the parent path of a path with wildcard, only the last level.
     *
     * @param regexPath path with potential wildcards
     * @return parent path without wildcards
     */
    public static String getRegexPathParentPath(String regexPath)
    {
        int lastDirSeparator = regexPath.lastIndexOf(IOUtils.DIR_SEPARATOR);
        String parentPath = regexPath.substring(0, lastDirSeparator + 1);

        if (parentPath.contains("*") || parentPath.contains("?")) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    String.format("The path '%s' is illegal, ONLY the trail folder can contain wildcard * or ?",
                            regexPath));
        }
        return parentPath;
    }
}
