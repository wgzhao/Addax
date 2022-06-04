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

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class StreamWriter
        extends Writer
{
    private static String buildFilePath(String path, String fileName)
    {
        boolean isEndWithSeparator = false;
        switch (IOUtils.DIR_SEPARATOR) {
            case IOUtils.DIR_SEPARATOR_UNIX:
                isEndWithSeparator = path.endsWith(String
                        .valueOf(IOUtils.DIR_SEPARATOR));
                break;
            case IOUtils.DIR_SEPARATOR_WINDOWS:
                isEndWithSeparator = path.endsWith(String
                        .valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
                break;
            default:
                break;
        }
        if (!isEndWithSeparator) {
            path = path + IOUtils.DIR_SEPARATOR;
        }
        return String.format("%s%s", path, fileName);
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

            if (StringUtils.isNoneBlank(path) && StringUtils.isNoneBlank(fileName)) {
                validateParameter(path, fileName);
            }
        }

        private void validateParameter(String path, String fileName)
        {
            try {
                // warn: 这里用户需要配一个目录
                File dir = new File(path);
                if (dir.isFile()) {
                    throw AddaxException
                            .asAddaxException(
                                    StreamWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
                                            path));
                }
                if (!dir.exists()) {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw AddaxException
                                .asAddaxException(
                                        StreamWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                        String.format("您指定的文件路径 : [%s] 创建失败.",
                                                path));
                    }
                }

                String fileFullPath = buildFilePath(path, fileName);
                File newFile = new File(fileFullPath);
                if (newFile.exists()) {
                    try {
                        FileUtils.forceDelete(newFile);
                    }
                    catch (IOException e) {
                        throw AddaxException.asAddaxException(
                                StreamWriterErrorCode.RUNTIME_EXCEPTION,
                                String.format("删除文件失败 : [%s] ", fileFullPath), e);
                    }
                }
            }
            catch (SecurityException se) {
                throw AddaxException.asAddaxException(
                        StreamWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件路径 : [%s] ", path), se);
            }
        }

        @Override
        public void prepare()
        {
            //
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            List<Configuration> writerSplitConfigs = new ArrayList<>();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(this.originalConfig);
            }

            return writerSplitConfigs;
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
    }

    public static class Task
            extends Writer.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private static final String NULL_FLAG = "NULL";
        private static final String NEWLINE_FLAG = System.getProperty("line.separator", "\n");

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
            this.nullFormat = writerSliceConfig.getString(StreamKey.NULL_FORMAT, NULL_FLAG);
            if (recordNumBeforeSleep < 0) {
                throw AddaxException.asAddaxException(StreamWriterErrorCode.CONFIG_INVALID_EXCEPTION, "recordNumber 不能为负值");
            }
            if (sleepTime < 0) {
                throw AddaxException.asAddaxException(StreamWriterErrorCode.CONFIG_INVALID_EXCEPTION, "sleep 不能为负值");
            }
        }

        @Override
        public void prepare()
        {
            //
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver)
        {

            if (StringUtils.isNoneBlank(path) && StringUtils.isNoneBlank(fileName)) {
                writeToFile(recordReceiver, path, fileName, recordNumBeforeSleep, sleepTime);
            }
            else if (this.print) {
                try {
                    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));

                    Record record;
                    while ((record = recordReceiver.getFromReader()) != null) {
                        writer.write(recordToString(record));
                    }
                    writer.flush();
                }
                catch (Exception e) {
                    throw AddaxException.asAddaxException(StreamWriterErrorCode.RUNTIME_EXCEPTION, e);
                }
            }
        }

        private void writeToFile(RecordReceiver recordReceiver, String path, String fileName,
                long recordNumBeforeSleep, long sleepTime)
        {

            LOG.info("begin do write...");
            String fileFullPath = buildFilePath(path, fileName);
            LOG.info("write to file : [{}]", fileFullPath);
            File newFile = new File(fileFullPath);
            try {
                if (!newFile.createNewFile()) {
                    LOG.error("failed to create file {}", fileFullPath);
                }
            }
            catch (IOException ioe) {
                LOG.error("failed to create file {}", fileFullPath);
            }
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(newFile, true), StandardCharsets.UTF_8))) {
                Record record;
                int count = 0;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if (recordNumBeforeSleep > 0 && sleepTime > 0 && count == recordNumBeforeSleep) {
                        LOG.info("StreamWriter start to sleep ... recordNumBeforeSleep={},sleepTime={}", recordNumBeforeSleep, sleepTime);
                        Thread.sleep(sleepTime * 1000L);
                    }
                    writer.write(recordToString(record));
                    count++;
                }
                writer.flush();
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(StreamWriterErrorCode.RUNTIME_EXCEPTION, e);
            }
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

        private String recordToString(Record record)
        {
            int recordLength = record.getColumnNumber();
            if (0 == recordLength) {
                return NEWLINE_FLAG;
            }

            Column column;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                if (column != null && column.getRawData() != null) {
                    if (column.getType() == Column.Type.TIMESTAMP) {
                        // timestamp always use UTC timezone
                        Instant instant = Instant.ofEpochMilli(column.asLong());
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC"));
                        sb.append(formatter.format(instant));
                    } else {
                        sb.append(column.asString());
                    }
                }
                else {
                    // use NULL FLAG to replace null value
                    sb.append(nullFormat);
                }
                sb.append(fieldDelimiter);
            }
            sb.setLength(sb.length() - 1);
            sb.append(NEWLINE_FLAG);

            return sb.toString();
        }
    }
}
