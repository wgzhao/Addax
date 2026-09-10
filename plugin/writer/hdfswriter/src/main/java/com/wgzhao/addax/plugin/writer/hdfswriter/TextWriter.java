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
     * The resolved shape of one configured column.
     *
     * @param name the field name, for diagnostics
     * @param declaredType the type as configured, for diagnostics
     * @param type the resolved destination type
     */
    private record ColumnPlan(String name, String declaredType, SupportHiveDataType type)
    {
    }

    /**
     * Everything a task needs to render a record, resolved once instead of for every field of
     * every record.
     *
     * @param fieldDelimiter the delimiter placed between two fields
     * @param encoding the charset the line is written in
     * @param nullFormat the text written for a null field, or null to keep it empty
     * @param columns the resolved columns, in the configured order
     */
    private record TextPlan(char fieldDelimiter, Charset encoding, String nullFormat,
            List<ColumnPlan> columns)
    {
    }

    /**
     * Resolve the task options and every column type up front.
     * <p>
     * A column type that cannot be resolved fails for every record, so it belongs here rather than
     * in the per-record dirty collection, where it would leave an empty file and a job that still
     * reported success.
     *
     * @param config the task configuration
     * @return the resolved plan
     */
    private static TextPlan buildPlan(Configuration config)
    {
        List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        List<ColumnPlan> resolved = new ArrayList<>(columns.size());
        for (Configuration column : columns) {
            String declaredType = column.getString(Key.TYPE);
            SupportHiveDataType type;
            try {
                type = SupportHiveDataType.of(declaredType);
            }
            catch (IllegalArgumentException e) {
                type = null;
            }

            // the text writer has no representation for a collection column either
            if (type == null || type == SupportHiveDataType.ARRAY || type == SupportHiveDataType.MAP) {
                throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                        String.format("Unsupported field type. Field name: [%s], Field type: [%s].",
                                column.getString(Key.NAME), declaredType));
            }
            resolved.add(new ColumnPlan(column.getString(Key.NAME), declaredType, type));
        }

        return new TextPlan(
                config.getChar(Key.FIELD_DELIMITER),
                Charset.forName(config.getString(Key.ENCODING, Constant.DEFAULT_ENCODING)),
                config.getString(Key.NULL_FORMAT, null),
                resolved);
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
        String compress = config.getString(Key.COMPRESS, "NONE").toUpperCase(Locale.ROOT).trim();
        TextPlan plan = buildPlan(config);

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
                MutablePair<Text, Boolean> transportResult = transportOneRecord(record, plan, taskPluginCollector);
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

    /**
     * Render one record into the line that will be written.
     *
     * @param record the record to render
     * @param plan the resolved task options
     * @param taskPluginCollector collects the record when a field cannot be converted
     * @return the line, and whether the record was rejected as dirty
     */
    public MutablePair<Text, Boolean> transportOneRecord(
            Record record, TextPlan plan, TaskPluginCollector taskPluginCollector)
    {
        MutablePair<Text, Boolean> transportResult = new MutablePair<>();
        transportResult.setRight(false);

        StringBuilder line = new StringBuilder(64);
        int recordLength = record.getColumnNumber();
        for (int i = 0; i < recordLength; i++) {
            if (i > 0) {
                line.append(plan.fieldDelimiter());
            }

            Column column = record.getColumn(i);
            if (null == column || null == column.getRawData()) {
                // nullFormat stays null when it is not configured, which renders as an empty
                // field exactly as an unset field always has
                if (null != plan.nullFormat()) {
                    line.append(plan.nullFormat());
                }
                continue;
            }

            ColumnPlan columnPlan = plan.columns().get(i);
            try {
                line.append(formatValue(column, columnPlan.type()));
            }
            catch (Exception e) {
                logger.warn("Warn: convert field[{}] from [{}] to [{}] error.",
                        columnPlan.name(), column.getRawData(), columnPlan.declaredType());
                taskPluginCollector.collectDirtyRecord(record, String.format(
                        "Type conversion error: target field type: [%s], field value: [%s].",
                        columnPlan.declaredType(), column.getRawData()));
                transportResult.setRight(true);
                return transportResult;
            }
        }

        // a Text is a plain byte container: encoding the line here is what makes the configured
        // encoding reach the file, because LineRecordWriter writes the bytes through untouched
        transportResult.setLeft(new Text(line.toString().getBytes(plan.encoding())));
        return transportResult;
    }

    /**
     * Convert one field into the value the line renders.
     *
     * @param column the column holding the value
     * @param columnType the resolved destination type
     * @return the converted value
     */
    private Object formatValue(Column column, SupportHiveDataType columnType)
    {
        String rowData = column.getRawData().toString();
        return switch (columnType) {
            case TINYINT -> Byte.valueOf(rowData);
            case SMALLINT -> Short.valueOf(rowData);
            case INT -> Integer.valueOf(rowData);
            case BIGINT -> column.asLong();
            case FLOAT -> Float.valueOf(rowData);
            case DOUBLE -> column.asDouble();
            case STRING, VARCHAR, CHAR -> formatTimeWithNanos(column, columnTimeZone);
            case DECIMAL -> HiveDecimal.create(column.asBigDecimal());
            case BOOLEAN -> column.asBoolean();
            case DATE -> org.apache.hadoop.hive.common.type.Date.valueOf(column.asString());
            case TIMESTAMP -> Timestamp.valueOf(column.asString());
            // base64 rather than the raw array: appending a byte[] would render its identity hash
            // and lose the content entirely
            case BINARY -> Base64.getEncoder().encodeToString(column.asBytes());
            default -> throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                    String.format("Unsupported field type: [%s].", columnType));
        };
    }
}
