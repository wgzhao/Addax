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

public class StorageReaderUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(StorageReaderUtil.class);

    // Compression type constants
    private static final String COMPRESS_NONE = "none";
    private static final String COMPRESS_ZIP = "zip";
    private static final String COMPRESS_LZO = "lzo";

    private StorageReaderUtil()
    {

    }

    public static void readFromStream(InputStream inputStream, String fileName,
            Configuration readerSliceConfig, RecordSender recordSender,
            TaskPluginCollector taskPluginCollector)
    {
        String compress = readerSliceConfig.getString(Key.COMPRESS, "");

        String encoding = readerSliceConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
        // handle blank encoding
        if (StringUtils.isBlank(encoding)) {
            encoding = Constant.DEFAULT_ENCODING;
            LOG.warn("The encoding: [{}] is illegal, uses [{}] by default", encoding, Constant.DEFAULT_ENCODING);
        }

        List<Configuration> column = readerSliceConfig.getListConfiguration(Key.COLUMN);
        // handle ["*"] -> [], null
        if (null != column && 1 == column.size() && "\"*\"".equals(column.get(0).toString())) {
            readerSliceConfig.set(Key.COLUMN, null);
        }

        int bufferSize = readerSliceConfig.getInt(Key.BUFFER_SIZE, Constant.DEFAULT_BUFFER_SIZE);

        // compress logic
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
        catch (NullPointerException e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
    }

    private static BufferedReader createBufferedReader(InputStream inputStream, String compress,
            String encoding, int bufferSize)
            throws IOException, CompressorException
    {

        if (StringUtils.isBlank(compress) || COMPRESS_NONE.equalsIgnoreCase(compress)) {
            return new BufferedReader(new InputStreamReader(inputStream, encoding), bufferSize);
        }
        else if (COMPRESS_ZIP.equalsIgnoreCase(compress)) {
            ZipCycleInputStream zipCycleInputStream = new ZipCycleInputStream(inputStream);
            return new BufferedReader(new InputStreamReader(zipCycleInputStream, encoding), bufferSize);
        }
        else if (COMPRESS_LZO.equalsIgnoreCase(compress)) {
            ExpandLzopInputStream expandLzopInputStream = new ExpandLzopInputStream(inputStream);
            return new BufferedReader(new InputStreamReader(expandLzopInputStream, encoding), bufferSize);
        }
        else {
            // common-compress supports almost compress alg
            CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(
                    compress.toUpperCase(), inputStream, true);
            return new BufferedReader(new InputStreamReader(input, encoding), bufferSize);
        }
    }

    public static void doReadFromStream(BufferedReader reader, String fileName,
            Configuration readerSliceConfig, RecordSender recordSender,
            TaskPluginCollector taskPluginCollector)
    {
        // Configure CSV parser
        CSVFormat.Builder csvFormatBuilder = CSVFormat.DEFAULT.builder();

        // Get field delimiter
        String delimiterInStr = readerSliceConfig.getString(Key.FIELD_DELIMITER);
        if (null != delimiterInStr && 1 != delimiterInStr.length()) {
            throw AddaxException.illegalConfigValue(Key.FIELD_DELIMITER, delimiterInStr);
        }
        if (null == delimiterInStr) {
            LOG.warn("Uses [{}] as delimiter by default", Constant.DEFAULT_FIELD_DELIMITER);
        }

        // warn: default value ',', fieldDelimiter could be \n(lineDelimiter) for no fieldDelimiter
        Character fieldDelimiter = readerSliceConfig.getChar(Key.FIELD_DELIMITER, Constant.DEFAULT_FIELD_DELIMITER);
        csvFormatBuilder.setDelimiter(fieldDelimiter);

        // Null format configuration, warn: no default value '\N'
        String nullFormat = readerSliceConfig.getString(Key.NULL_FORMAT);
        csvFormatBuilder.setNullString(nullFormat);

        // Header handling
        if (readerSliceConfig.getBool(Key.SKIP_HEADER, Constant.DEFAULT_SKIP_HEADER)) {
            csvFormatBuilder.setHeader();
            csvFormatBuilder.setSkipHeaderRecord(true);
        }
        List<ColumnEntry> column = StorageReaderUtil.getListColumnEntry(readerSliceConfig, Key.COLUMN);

        // every line logic
        try (CSVParser csvParser = new CSVParser(reader, csvFormatBuilder.build())) {
            csvParser.stream()
                    .filter(Objects::nonNull)
                    .forEach(csvRecord -> StorageReaderUtil.transportOneRecord(
                            recordSender, column, csvRecord.toList().toArray(new String[0]), nullFormat, taskPluginCollector)
                    );
        }
        catch (UnsupportedEncodingException uee) {
            String encoding = readerSliceConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
            throw AddaxException.asAddaxException(
                    ENCODING_ERROR, String.format("The encoding %s is unsupported", encoding), uee);
        }
        catch (FileNotFoundException fnfe) {
            throw AddaxException.asAddaxException(
                    IO_ERROR, String.format("The file [%s] does not exists ", fileName), fnfe);
        }
        catch (IOException ioe) {
            throw AddaxException.asAddaxException(
                    IO_ERROR, String.format("Failed to ead file [%s]", fileName), ioe);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    RUNTIME_ERROR, e);
        }
    }

    public static void transportOneRecord(RecordSender recordSender, Configuration configuration,
            TaskPluginCollector taskPluginCollector, String line)
    {
        List<ColumnEntry> column = StorageReaderUtil.getListColumnEntry(configuration, Key.COLUMN);
        // the nullFormat has not defaulted value
        String nullFormat = configuration.getString(Key.NULL_FORMAT);

        // warn: default value ',', fieldDelimiter could be \n(lineDelimiter)
        // for no fieldDelimiter
        Character fieldDelimiter = configuration.getChar(Key.FIELD_DELIMITER, Constant.DEFAULT_FIELD_DELIMITER);

        String[] sourceLine = StringUtils.split(line, fieldDelimiter);

        transportOneRecord(recordSender, column, sourceLine, nullFormat, taskPluginCollector);
    }

    public static void transportOneRecord(RecordSender recordSender, List<ColumnEntry> columnConfigs, String[] sourceLine,
            String nullFormat, TaskPluginCollector taskPluginCollector)
    {
        Record record = recordSender.createRecord();
        Column columnGenerated;

        if (null == columnConfigs || columnConfigs.isEmpty()) {
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
                for (ColumnEntry columnConfig : columnConfigs) {
                    String columnType = columnConfig.getType();
                    Integer columnIndex = columnConfig.getIndex();
                    String columnConst = columnConfig.getValue();

                    String columnValue;

                    if (null == columnIndex && null == columnConst) {
                        throw AddaxException.asAddaxException(
                                CONFIG_ERROR, "The index or constant is required when type is present.");
                    }

                    if (null != columnIndex && null != columnConst) {
                        throw AddaxException.asAddaxException(
                                CONFIG_ERROR, "The index and value are both present, choose one of them");
                    }

                    if (null != columnIndex) {
                        if (columnIndex >= sourceLine.length) {
                            throw new IndexOutOfBoundsException(String.format("The column index [%s] you try to read is out of range[%s]: [%s]",
                                    columnIndex + 1, sourceLine.length, StringUtils.join(sourceLine, ",")));
                        }
                        columnValue = sourceLine[columnIndex];
                    }
                    else {
                        columnValue = columnConst;
                    }
                    Type type = Type.valueOf(columnType.toUpperCase());
                    // it's all ok if nullFormat is null
                    if (columnValue == null || columnValue.equals(nullFormat)) {
                        record.addColumn(new StringColumn());
                        continue;
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
                                String formatString = columnConfig.getFormat();
                                if (StringUtils.isNotBlank(formatString)) {
                                    DateFormat format = columnConfig.getDateFormat();
                                    columnGenerated = new DateColumn(format.parse(columnValue));
                                }
                                else {
                                    columnGenerated = new DateColumn(new StringColumn(columnValue).asDate());
                                }
                                break;
                            default:
                                String errorMessage = String.format("The column type [%s] is unsupported", columnType);
                                LOG.error(errorMessage);
                                throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE, errorMessage);
                        }
                    }
                    catch (Exception e) {
                        throw new IllegalArgumentException(String.format("Cast value [%s] to type [%s] failure", columnValue, type.name()));
                    }

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
                // each record which transfer failed  should be regarded as dirty one
                taskPluginCollector.collectDirtyRecord(record, e.getMessage());
            }
        }
    }

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
     * check parameter:encoding, compress, filedDelimiter
     *
     * @param readerConfiguration {@link Configuration}
     */
    public static void validateParameter(Configuration readerConfiguration)
    {

        validateEncoding(readerConfiguration);
        validateFieldDelimiter(readerConfiguration);

        // column: 1. index type 2.value type 3.when type is Date, may have format
        validateColumn(readerConfiguration);
    }

    public static void validateEncoding(Configuration readerConfiguration)
    {
        // encoding check
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

    public static void validateFieldDelimiter(Configuration readerConfiguration)
    {
        //fieldDelimiter check
        String delimiterInStr = readerConfiguration.getString(Key.FIELD_DELIMITER, ",");
        if (null == delimiterInStr) {
            throw AddaxException.asAddaxException(REQUIRED_VALUE,
                    String.format("The item [%s] is required.",
                            Key.FIELD_DELIMITER));
        }
        else if (1 != delimiterInStr.length()) {
            // warn: if it has, length must be one
            throw AddaxException.illegalConfigValue(Key.FIELD_DELIMITER, delimiterInStr);
        }
    }

    public static void validateColumn(Configuration readerConfiguration)
    {
        // column: 1. index type 2.value type 3.when type is Date, may have
        // format
        List<Configuration> columns = readerConfiguration.getListConfiguration(Key.COLUMN);
        if (null == columns || columns.isEmpty()) {
            throw AddaxException.missingConfig(Key.COLUMN);
        }
        // handle ["*"]
        if (1 == columns.size()) {
            String columnsInStr = columns.get(0).toString();
            if ("\"*\"".equals(columnsInStr) || "'*'".equals(columnsInStr)) {
                readerConfiguration.set(Key.COLUMN, null);
                columns = null;
            }
        }

        if (null != columns && !columns.isEmpty()) {
            for (Configuration eachColumnConf : columns) {
                eachColumnConf.getNecessaryValue(Key.TYPE, REQUIRED_VALUE);
                Integer columnIndex = eachColumnConf.getInt(Key.INDEX);
                String columnValue = eachColumnConf.getString(Key.VALUE);

                if (null == columnIndex && null == columnValue) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR,
                            "You must configure one of index or name or value");
                }

                if (null != columnIndex && null != columnValue) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR,
                            "You both configure index, value, or name, you can ONLY specify the one each column");
                }
                if (null != columnIndex && columnIndex < 0) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            String.format("The value of index must be greater than 0, %s is illegal", columnIndex));
                }
            }
        }
    }

    /**
     * get the parent path of the path with wildcard, only the last level
     *
     * @param regexPath path
     * @return String
     */
    public static String getRegexPathParentPath(String regexPath)
    {
        int lastDirSeparator = regexPath.lastIndexOf(IOUtils.DIR_SEPARATOR);
        String parentPath;
        parentPath = regexPath.substring(0, lastDirSeparator + 1);
        if (parentPath.contains("*") || parentPath.contains("?")) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    String.format("The path '%s' is illegal, ONLY the trail folder can container wildcard * or ? ",
                            regexPath));
        }
        return parentPath;
    }
}
