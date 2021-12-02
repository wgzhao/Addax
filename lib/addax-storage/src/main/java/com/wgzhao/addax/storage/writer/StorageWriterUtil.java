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

import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.compress.ZipCycleOutputStream;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
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
import java.util.Set;
import java.util.UUID;

public class StorageWriterUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(StorageWriterUtil.class);
    private static final Set<String> supportedCompress = new HashSet<>(Arrays.asList("gzip", "bzip2", "zip"));
    private static final Set<String> supportedWriteModes = new HashSet<>(Arrays.asList("truncate", "append", "nonConflict"));

    private StorageWriterUtil()
    {

    }

    /*
     * check parameter: writeMode, encoding, compress, filedDelimiter
     */
    public static void validateParameter(Configuration writerConfiguration)
    {
        // writeMode check
        String writeMode = writerConfiguration.getNecessaryValue(Key.WRITE_MODE, StorageWriterErrorCode.REQUIRED_VALUE);
        writeMode = writeMode.trim();
        if (!supportedWriteModes.contains(writeMode)) {
            throw AddaxException
                    .asAddaxException(
                            StorageWriterErrorCode.ILLEGAL_VALUE,
                            String.format(
                                    "'%s' is unsupported, supported write mode: [%s]",
                                    writeMode, StringUtils.join(supportedWriteModes, ",")));
        }
        writerConfiguration.set(Key.WRITE_MODE, writeMode);

        // encoding check
        String encoding = writerConfiguration.getString(Key.ENCODING);
        if (StringUtils.isBlank(encoding)) {
            // like "  ", null
            LOG.warn(String.format("您的encoding配置为空, 将使用默认值[%s]", Constant.DEFAULT_ENCODING));
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
                        StorageWriterErrorCode.ILLEGAL_VALUE,
                        String.format("不支持您配置的编码格式:[%s]", encoding), e);
            }
        }

        // only support compress types
        String compress = writerConfiguration.getString(Key.COMPRESS);
        if (StringUtils.isBlank(compress)) {
            writerConfiguration.set(Key.COMPRESS, null);
        }
        else {
            if (!supportedCompress.contains(compress.toLowerCase().trim())) {
                String message = String.format(
                        "'%s' is unsupported, supported compress format: [%s] ",
                        compress, StringUtils.join(supportedCompress, ","));
                throw AddaxException.asAddaxException(
                        StorageWriterErrorCode.ILLEGAL_VALUE,
                        String.format(message, compress));
            }
        }

        // fieldDelimiter check
        String delimiterInStr = writerConfiguration
                .getString(Key.FIELD_DELIMITER);
        // warn: if it has, length must be one
        if (null != delimiterInStr && 1 != delimiterInStr.length()) {
            throw AddaxException.asAddaxException(
                    StorageWriterErrorCode.ILLEGAL_VALUE,
                    String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", delimiterInStr));
        }
        if (null == delimiterInStr) {
            LOG.warn(String.format("您没有配置列分隔符, 使用默认值[%s]",
                    Constant.DEFAULT_FIELD_DELIMITER));
            writerConfiguration.set(Key.FIELD_DELIMITER,
                    Constant.DEFAULT_FIELD_DELIMITER);
        }

        // fileFormat check
        String fileFormat = writerConfiguration.getString(Key.FILE_FORMAT, Constant.DEFAULT_FILE_FORMAT);
        if (!Constant.SUPPORTED_FILE_FORMAT.contains(fileFormat)) {
            throw AddaxException.asAddaxException(
                    StorageWriterErrorCode.ILLEGAL_VALUE,
                    String.format("您配置的fileFormat [%s]错误, 支持[%s]两种.", fileFormat, Constant.SUPPORTED_FILE_FORMAT));
        }
    }

    public static List<Configuration> split(Configuration writerSliceConfig,
            Set<String> originAllFileExists, int mandatoryNumber)
    {
        LOG.info("begin do split...");
        Set<String> allFileExists = new HashSet<>(originAllFileExists);
        List<Configuration> writerSplitConfigs = new ArrayList<>();
        String filePrefix = writerSliceConfig.getString(Key.FILE_NAME);

        String fileSuffix;
        for (int i = 0; i < mandatoryNumber; i++) {
            // handle same file name
            Configuration splitTaskConfig = writerSliceConfig.clone();
            String fullFileName;
            fileSuffix = UUID.randomUUID().toString().replace('-', '_');
            fullFileName = String.format("%s__%s", filePrefix, fileSuffix);
            while (allFileExists.contains(fullFileName)) {
                fileSuffix = UUID.randomUUID().toString().replace('-', '_');
                fullFileName = String.format("%s__%s", filePrefix, fileSuffix);
            }
            allFileExists.add(fullFileName);
            splitTaskConfig.set(Key.FILE_NAME, fullFileName);
            LOG.info(String.format("split write file name:[%s]", fullFileName));
            writerSplitConfigs.add(splitTaskConfig);
        }
        LOG.info("end do split.");
        return writerSplitConfigs;
    }

    public static String buildFilePath(String path, String fileName,
            String suffix)
    {
        boolean isEndWithSeparator = false;
        switch (IOUtils.DIR_SEPARATOR) {
            case IOUtils.DIR_SEPARATOR_UNIX:
                isEndWithSeparator = path.endsWith(String.valueOf(IOUtils.DIR_SEPARATOR));
                break;
            case IOUtils.DIR_SEPARATOR_WINDOWS:
                isEndWithSeparator = path.endsWith(String.valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
                break;
            default:
                break;
        }
        if (!isEndWithSeparator) {
            path = path + IOUtils.DIR_SEPARATOR;
        }
        if (null == suffix) {
            suffix = "";
        }
        else {
            suffix = suffix.trim();
        }
        return String.format("%s%s%s", path, fileName, suffix);
    }

    public static void writeToStream(RecordReceiver lineReceiver,
            OutputStream outputStream, Configuration config, String fileName,
            TaskPluginCollector taskPluginCollector)
    {
        String encoding = config.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
        // handle blank encoding
        if (StringUtils.isBlank(encoding)) {
            LOG.warn("您配置的encoding为[{}], 使用默认值[{}]", encoding, Constant.DEFAULT_ENCODING);
            encoding = Constant.DEFAULT_ENCODING;
        }
        String compress = config.getString(Key.COMPRESS);

        BufferedWriter writer = null;
        // compress logic
        try {
            if (null == compress) {
                writer = new BufferedWriter(new OutputStreamWriter(outputStream, encoding));
            }
            else {
                if ("gzip".equalsIgnoreCase(compress)) {
                    CompressorOutputStream compressorOutputStream = new GzipCompressorOutputStream(outputStream);
                    writer = new BufferedWriter(new OutputStreamWriter(compressorOutputStream, encoding));
                }
                else if ("bzip2".equalsIgnoreCase(compress)) {
                    CompressorOutputStream compressorOutputStream = new BZip2CompressorOutputStream(outputStream);
                    writer = new BufferedWriter(new OutputStreamWriter(compressorOutputStream, encoding));
                }
                else if ("zip".equals(compress)) {
                    ZipCycleOutputStream zis = new ZipCycleOutputStream(outputStream, fileName);
                    writer = new BufferedWriter(new OutputStreamWriter(zis, encoding));
                }
                else {
                    throw AddaxException
                            .asAddaxException(
                                    StorageWriterErrorCode.ILLEGAL_VALUE,
                                    String.format("'%s' is supported, supported compress format: [%s]",
                                            compress, StringUtils.join(supportedCompress, ",")));
                }
            }
            StorageWriterUtil.doWriteToStream(lineReceiver, writer, fileName, config, taskPluginCollector);
        }
        catch (UnsupportedEncodingException uee) {
            throw AddaxException
                    .asAddaxException(
                            StorageWriterErrorCode.WRITE_FILE_WITH_CHARSET_ERROR,
                            String.format("不支持的编码格式 : [%s]", encoding), uee);
        }
        catch (NullPointerException e) {
            throw AddaxException.asAddaxException(
                    StorageWriterErrorCode.RUNTIME_EXCEPTION,
                    "运行时错误, 请联系我们", e);
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(
                    StorageWriterErrorCode.WRITE_FILE_IO_ERROR,
                    String.format("流写入错误 : [%s]", fileName), e);
        }
        finally {
            IOUtils.closeQuietly(writer, null);
        }
    }

    private static void doWriteToStream(RecordReceiver lineReceiver,
            BufferedWriter writer, String context, Configuration config,
            TaskPluginCollector taskPluginCollector)
            throws IOException
    {

        String nullFormat = config.getString(Key.NULL_FORMAT);

        // 兼容format & dataFormat
        String dateFormat = config.getString(Key.DATE_FORMAT);
        DateFormat dateParse = null; // warn: 可能不兼容
        if (StringUtils.isNotBlank(dateFormat)) {
            dateParse = new SimpleDateFormat(dateFormat);
        }

        // warn: default false
        String fileFormat = config.getString(Key.FILE_FORMAT, Constant.DEFAULT_FILE_FORMAT);

        String delimiterInStr = config.getString(Key.FIELD_DELIMITER);
        if (null != delimiterInStr && 1 != delimiterInStr.length()) {
            throw AddaxException.asAddaxException(
                    StorageWriterErrorCode.ILLEGAL_VALUE,
                    String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", delimiterInStr));
        }
        if (null == delimiterInStr) {
            LOG.warn(String.format("您没有配置列分隔符, 使用默认值[%s]",
                    Constant.DEFAULT_FIELD_DELIMITER));
        }

        // warn: fieldDelimiter could not be '' for no fieldDelimiter
        char fieldDelimiter = config.getChar(Key.FIELD_DELIMITER,
                Constant.DEFAULT_FIELD_DELIMITER);

        Writer unstructuredWriter = TextCsvWriterManager
                .produceUnstructuredWriter(fileFormat, fieldDelimiter, writer);

        List<String> headers = config.getList(Key.HEADER, String.class);
        if (null != headers && !headers.isEmpty()) {
            unstructuredWriter.writeOneRecord(headers);
        }

        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            StorageWriterUtil.transportOneRecord(record,
                    nullFormat, dateParse, taskPluginCollector,
                    unstructuredWriter);
        }

        // warn:由调用方控制流的关闭
        // IOUtils.closeQuietly(unstructuredWriter);
    }

    /*
     * 异常表示脏数据
     */
    public static void transportOneRecord(Record record, String nullFormat,
            DateFormat dateParse, TaskPluginCollector taskPluginCollector,
            Writer writer)
    {
//        // warn: default is null
//        if (null == nullFormat) {
//            nullFormat = "null";
//        }
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
                        boolean isDateColumn = column instanceof DateColumn;
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
            writer.writeOneRecord(splitRows);
        }
        catch (Exception e) {
            // warn: dirty data
            taskPluginCollector.collectDirtyRecord(record, e);
        }
    }
}
