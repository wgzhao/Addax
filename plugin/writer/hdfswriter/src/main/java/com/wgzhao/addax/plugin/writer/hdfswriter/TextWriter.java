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

import com.wgzhao.addax.core.base.Constant;
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
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;

/** Text Writer. */
public class TextWriter
        extends HdfsHelper
        implements IHDFSWriter
{
    private final static Logger logger = LoggerFactory.getLogger(TextWriter.class.getName());

    /**
     * The per-task options that shape every output line.
     *
     * @param fieldDelimiter the delimiter placed between two fields
     * @param encoding the charset the line is written in
     * @param nullFormat the text written for a null field, or null to keep it empty
     */
    private record TextOptions(char fieldDelimiter, Charset encoding, String nullFormat)
    {
    }

    /** Textwriter. */
    public TextWriter(Configuration conf)
    {
        super();
        getFileSystem(conf);
    }

    @Override
    public void write(RecordReceiver lineReceiver, Configuration config, String fileName, TaskPluginCollector taskPluginCollector)
    {
        List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, "NONE").toUpperCase(Locale.ROOT).trim();
        TextOptions options = new TextOptions(
                config.getChar(Key.FIELD_DELIMITER),
                Charset.forName(config.getString(Key.ENCODING, Constant.DEFAULT_ENCODING)),
                config.getString(Key.NULL_FORMAT, null));

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
                MutablePair<Text, Boolean> transportResult = transportOneRecord(record, columns, taskPluginCollector, options);
                if (Boolean.FALSE.equals(transportResult.getRight())) {
                    writer.write(NullWritable.get(), transportResult.getLeft());
                }
            }
            writer.close(Reporter.NULL);
        }
        catch (IOException e) {
            logger.error("IO exception occurred while writing text file [{}]", fileName);
            // no per-task cleanup here: the parent is the staging directory shared by every split
            // task, and Job.destroy() removes the whole staging directory on failure anyway
            throw AddaxException.asAddaxException(IO_ERROR, e);
        }
    }

    /** Transportonerecord. */
    public MutablePair<Text, Boolean> transportOneRecord(
            Record record, List<Configuration> columnsConfiguration,
            TaskPluginCollector taskPluginCollector, TextOptions options)
    {
        MutablePair<List<Object>, Boolean> transportResultList =
                toFieldList(record, columnsConfiguration, taskPluginCollector, options);
        MutablePair<Text, Boolean> transportResult = new MutablePair<>();
        transportResult.setRight(false);
        // a Text is a plain byte container: encoding the line here is what makes the configured
        // encoding reach the file, because LineRecordWriter writes the bytes through untouched
        Text recordResult = new Text(StringUtils.join(transportResultList.getLeft(), options.fieldDelimiter())
                .getBytes(options.encoding()));
        transportResult.setRight(transportResultList.getRight());
        transportResult.setLeft(recordResult);
        return transportResult;
    }

    /** Transportonerecord. */
    public MutablePair<List<Object>, Boolean> toFieldList(
            Record record, List<Configuration> columnsConfiguration,
            TaskPluginCollector taskPluginCollector, TextOptions options)
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
                    try {
                        // resolve inside the try: an unsupported type is a per-record conversion
                        // failure that belongs in the dirty records, not a task-killing throw
                        SupportHiveDataType columnType = SupportHiveDataType.of(
                                columnsConfiguration.get(i).getString(Key.TYPE));
                        switch (columnType) {
                            case TINYINT -> recordList.add(Byte.valueOf(rowData));
                            case SMALLINT -> recordList.add(Short.valueOf(rowData));
                            case INT, INTEGER -> recordList.add(Integer.valueOf(rowData));
                            case BIGINT -> recordList.add(column.asLong());
                            case FLOAT -> recordList.add(Float.valueOf(rowData));
                            case DOUBLE -> recordList.add(column.asDouble());
                            case STRING, VARCHAR, CHAR -> recordList.add(formatTimeWithNanos(column, columnTimeZone));
                            case DECIMAL -> recordList.add(HiveDecimal.create(column.asBigDecimal()));
                            case BOOLEAN -> recordList.add(column.asBoolean());
                            case DATE -> recordList.add(org.apache.hadoop.hive.common.type.Date.valueOf(column.asString()));
                            case TIMESTAMP -> recordList.add(Timestamp.valueOf(column.asString()));
                            // base64 rather than the raw array: joining a byte[] would render its
                            // identity hash and lose the content entirely
                            case BINARY -> recordList.add(Base64.getEncoder().encodeToString(column.asBytes()));
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
                                column.getRawData(), columnsConfiguration.get(i).getString(Key.TYPE));
                        String message = String.format(
                                "Type conversion error：target field type: [%s], field value: [%s].",
                                columnsConfiguration.get(i).getString(Key.TYPE), column.getRawData());
                        taskPluginCollector.collectDirtyRecord(record, message);
                        transportResult.setRight(true);
                        break;
                    }
                }
                else {
                    // nullFormat stays null when it is not configured, which renders as an empty
                    // field exactly as before
                    recordList.add(options.nullFormat());
                }
            }
        }
        transportResult.setLeft(recordList);
        return transportResult;
    }
}
