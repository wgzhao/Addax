/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.wgzhao.addax.plugin.writer.hdfswriter;

import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;

public class TextWriter
        extends HdfsHelper
        implements IHDFSWriter
{
    private final static Logger logger = LoggerFactory.getLogger(TextWriter.class.getName());

    public TextWriter(Configuration conf)
    {
        super();
        getFileSystem(conf);
    }

    @Override
    public void write(RecordReceiver lineReceiver, Configuration config, String fileName, TaskPluginCollector taskPluginCollector)
    {
        char fieldDelimiter = config.getChar(Key.FIELD_DELIMITER);
        List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, "NONE").toUpperCase().trim();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
        String attempt = "attempt_" + dateFormat.format(new Date()) + "_0001_m_000000_0";
        conf.set(JobContext.TASK_ATTEMPT_ID, attempt);
        if (!"NONE".equals(compress)) {
            // fileName must remove suffix, because the FileOutputFormat will add suffix
            fileName = fileName.substring(0, fileName.lastIndexOf("."));
            Class<? extends CompressionCodec> codecClass = getCompressCodec(compress);
            if (null != codecClass) {
                FileOutputFormat.setOutputCompressorClass(conf, codecClass);
            }
        }
        Path outputPath = new Path(fileName);
        FileOutputFormat.setOutputPath(conf, outputPath);
        FileOutputFormat.setWorkOutputPath(conf, outputPath);
        try {
            RecordWriter<NullWritable, Text> writer = new TextOutputFormat<NullWritable, Text>()
                    .getRecordWriter(fileSystem, conf, outputPath.toString(), Reporter.NULL);
            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                MutablePair<Text, Boolean> transportResult = transportOneRecord(record, fieldDelimiter, columns, taskPluginCollector);
                if (Boolean.FALSE.equals(transportResult.getRight())) {
                    writer.write(NullWritable.get(), transportResult.getLeft());
                }
            }
            writer.close(Reporter.NULL);
        }
        catch (IOException e) {
            logger.error("IO exception occurred while writing text file [{}]", fileName);
            Path path = new Path(fileName);
            deleteDir(path.getParent());
            throw AddaxException.asAddaxException(IO_ERROR, e);
        }
    }

    public static MutablePair<Text, Boolean> transportOneRecord(
            Record record, char fieldDelimiter, List<Configuration> columnsConfiguration, TaskPluginCollector taskPluginCollector)
    {
        MutablePair<List<Object>, Boolean> transportResultList = transportOneRecord(record, columnsConfiguration, taskPluginCollector);
        MutablePair<Text, Boolean> transportResult = new MutablePair<>();
        transportResult.setRight(false);
        Text recordResult = new Text(StringUtils.join(transportResultList.getLeft(), fieldDelimiter));
        transportResult.setRight(transportResultList.getRight());
        transportResult.setLeft(recordResult);
        return transportResult;
    }

    public static MutablePair<List<Object>, Boolean> transportOneRecord(
            Record record, List<Configuration> columnsConfiguration,
            TaskPluginCollector taskPluginCollector)
    {

        MutablePair<List<Object>, Boolean> transportResult = new MutablePair<>();
        transportResult.setRight(false);
        List<Object> recordList = new ArrayList<>();
        int recordLength = record.getColumnNumber();
        if (0 != recordLength) {
            Column column;
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                if (null != column.getRawData()) {
                    String rowData = column.getRawData().toString();
                    SupportHiveDataType columnType = SupportHiveDataType.valueOf(
                            columnsConfiguration.get(i).getString(Key.TYPE).toUpperCase());
                    try {
                        switch (columnType) {
                            case TINYINT -> recordList.add(Byte.valueOf(rowData));
                            case SMALLINT -> recordList.add(Short.valueOf(rowData));
                            case INT, INTEGER -> recordList.add(Integer.valueOf(rowData));
                            case BIGINT -> recordList.add(column.asLong());
                            case FLOAT -> recordList.add(Float.valueOf(rowData));
                            case DOUBLE -> recordList.add(column.asDouble());
                            case STRING, VARCHAR, CHAR -> recordList.add(column.asString());
                            case DECIMAL -> recordList.add(HiveDecimal.create(column.asBigDecimal()));
                            case BOOLEAN -> recordList.add(column.asBoolean());
                            case DATE -> recordList.add(org.apache.hadoop.hive.common.type.Date.valueOf(column.asString()));
                            case TIMESTAMP -> recordList.add(Timestamp.valueOf(column.asString()));
                            case BINARY -> recordList.add(column.asBytes());
                            default -> throw AddaxException.asAddaxException(
                                    NOT_SUPPORT_TYPE,
                                    String.format(
                                            "The configuration is incorrect. The database does not support writing this type of field. " +
                                                    "Field name: [%s], field type: [%s].",
                                            columnsConfiguration.get(i).getString(Key.NAME),
                                            columnsConfiguration.get(i).getString(Key.TYPE)));
                        }
                    }
                    catch (Exception e) {
                        logger.warn("Warn: convert field[{}] from [{}] to [{}] error.",
                                columnsConfiguration.get(i).getString(Key.NAME),
                                column.getRawData(), columnType);
                        String message = String.format(
                                "Type conversion error：target field type: [%s], field value: [%s].",
                                columnsConfiguration.get(i).getString(Key.TYPE), column.getRawData());
                        taskPluginCollector.collectDirtyRecord(record, message);
                        transportResult.setRight(true);
                        break;
                    }
                }
                else {
                    // warn: it's all ok if nullFormat is null
                    recordList.add(null);
                }
            }
        }
        transportResult.setLeft(recordList);
        return transportResult;
    }
}
