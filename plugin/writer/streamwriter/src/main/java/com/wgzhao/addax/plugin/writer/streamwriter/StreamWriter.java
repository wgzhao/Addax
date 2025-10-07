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

package com.wgzhao.addax.plugin.writer.streamwriter;

import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.PERMISSION_ERROR;

public class StreamWriter
        extends Writer
{

    private static String buildFilePath(String path, String fileName)
    {
        boolean isEndWithSeparator = switch (IOUtils.DIR_SEPARATOR) {
            case IOUtils.DIR_SEPARATOR_UNIX -> path.endsWith(String.valueOf(IOUtils.DIR_SEPARATOR));
            case IOUtils.DIR_SEPARATOR_WINDOWS -> path.endsWith(String.valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
            default -> false;
        };

        if (!isEndWithSeparator) {
            path = path + IOUtils.DIR_SEPARATOR;
        }
        return "%s%s".formatted(path, fileName);
    }

    public static class Job
            extends Writer.Job
    {

        private Configuration originalConfig;

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();

            String path = this.originalConfig.getString(StreamKey.PATH, null);
            String fileName = this.originalConfig.getString(StreamKey.FILE_NAME, null);

            if (StringUtils.isNoneBlank(path, fileName)) {
                validateParameter(path, fileName);
            }
        }

        private void validateParameter(String path, String fileName)
        {
            try {
                Path dirPath = Path.of(path);
                if (Files.isRegularFile(dirPath)) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "The path you configured is a file, not a directory.");
                }

                Files.createDirectories(dirPath);

                Path filePath = Path.of(buildFilePath(path, fileName));
                Files.deleteIfExists(filePath);
            }
            catch (IOException e) {
                throw AddaxException.asAddaxException(IO_ERROR, "Failed to create directory or delete file", e);
            }
            catch (SecurityException se) {
                throw AddaxException.asAddaxException(PERMISSION_ERROR,
                        "The permission is denied to create file", se);
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            List<Configuration> writerSplitConfigs = new ArrayList<>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(this.originalConfig);
            }
            return writerSplitConfigs;
        }

        @Override
        public void destroy()
        {
            //
        }
    }

    public static class Task
            extends Writer.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private static final String NEWLINE_FLAG = System.lineSeparator();
        // The max length of binary preview is 64 bytes
        private static final int MAX_BINARY_PREVIEW = 64;

        private String fieldDelimiter;
        private boolean print;

        private String path;
        private String fileName;

        private long recordNumBeforeSleep;
        private long sleepTime;

        private String nullFormat;

        @Override
        public void init()
        {
            Configuration writerSliceConfig = getPluginJobConf();

            this.fieldDelimiter = writerSliceConfig.getString(StreamKey.FIELD_DELIMITER, "\t");
            this.print = writerSliceConfig.getBool(StreamKey.PRINT, true);

            this.path = writerSliceConfig.getString(StreamKey.PATH, null);
            this.fileName = writerSliceConfig.getString(StreamKey.FILE_NAME, null);
            this.recordNumBeforeSleep = writerSliceConfig.getLong(StreamKey.RECORD_NUM_BEFORE_SLEEP, 0);
            this.sleepTime = writerSliceConfig.getLong(StreamKey.SLEEP_TIME, 0);
            this.nullFormat = writerSliceConfig.getString(StreamKey.NULL_FORMAT, StreamKey.NULL_FLAG);

            if (recordNumBeforeSleep < 0) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, "recordNumber must be greater than 0");
            }
            if (sleepTime < 0) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, "sleep time must be greater than 0");
            }
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver)
        {
            if (StringUtils.isNoneBlank(path, fileName)) {
                writeToFile(recordReceiver);
            }
            else if (this.print) {
               writeToConsole(recordReceiver);
            }
        }

        private void writeToConsole(RecordReceiver recordReceiver)
        {
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    String output = recordToString(record);
                    System.out.print(output);
                }
                System.out.flush();
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(IO_ERROR, e);
            }
        }

        private void writeToFile(RecordReceiver recordReceiver)
        {
            LOG.info("begin do write...");
            String fileFullPath = buildFilePath(path, fileName);
            LOG.info("write to file : [{}]", fileFullPath);

            try (BufferedWriter writer = Files.newBufferedWriter(
                    Path.of(fileFullPath), StandardCharsets.UTF_8)) {
                Record record;
                int count = 0;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if (recordNumBeforeSleep > 0 && sleepTime > 0 && count == recordNumBeforeSleep) {
                        LOG.info("StreamWriter start to sleep ... recordNumBeforeSleep={},sleepTime={}",
                                recordNumBeforeSleep, sleepTime);
                        TimeUnit.SECONDS.sleep(sleepTime);
                        count = 0;
                    }
                    writer.write(recordToString(record));
                    count++;
                }
                writer.flush();
            }
            catch (IOException | InterruptedException e) {
                throw AddaxException.asAddaxException(IO_ERROR, e);
            }
        }

        private String recordToString(Record record)
        {
            int recordLength = record.getColumnNumber();
            if (0 == recordLength) {
                return NEWLINE_FLAG;
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < recordLength; i++) {
                Column column = record.getColumn(i);
                sb.append(formatColumn(column)).append(fieldDelimiter);
            }

            if (!sb.isEmpty()) {
                sb.setLength(sb.length() - fieldDelimiter.length());
            }
            return sb.append(NEWLINE_FLAG).toString();
        }

        private String formatColumn(Column column)
        {
            if (column == null || column.getRawData() == null) {
                return nullFormat;
            }
            Object raw = column.getRawData();
            // handle bytes , using hex to display
            if (raw instanceof byte[] bytes) {
                int len = bytes.length;
                if (len == 0) {
                    return "0x";
                }
                int show = Math.min(len, MAX_BINARY_PREVIEW);
                StringBuilder hex = new StringBuilder(2 + show * 2 + (len > show ? 16 : 0));
                hex.append("0x");
                for (int i = 0; i < show; i++) {
                    hex.append(String.format("%02X", bytes[i]));
                }
                if (len > show) {
                    hex.append("...(").append(len).append(" bytes)");
                }
                return hex.toString();
            }
            return column.asString();
        }

        @Override
        public void destroy()
        {
            // No resources to clean up
        }
    }
}
