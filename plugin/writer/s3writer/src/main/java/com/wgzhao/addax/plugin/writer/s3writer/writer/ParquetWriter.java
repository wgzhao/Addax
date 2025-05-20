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

package com.wgzhao.addax.plugin.writer.s3writer.writer;

import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.plugin.writer.s3writer.S3Key;
import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;

public class ParquetWriter
        implements IFormatWriter
{
    private static final Logger logger = LoggerFactory.getLogger(ParquetWriter.class.getName());
    private char fieldDelimiter;
    private String nullFormat;
    private String dateFormat;
    private String encoding;
    private String bucket;
    private String object;
    private List<String> header;
    private String sslEnabled;
    private S3Client s3Client;

    private String fileName;

    private org.apache.hadoop.conf.Configuration hadoopConf = null;

    public ParquetWriter()
    {
    }

    public char getFieldDelimiter()
    {
        return fieldDelimiter;
    }

    public ParquetWriter setFieldDelimiter(char fieldDelimiter)
    {
        this.fieldDelimiter = fieldDelimiter;
        return this;
    }

    public String getNullFormat()
    {
        return nullFormat;
    }

    public ParquetWriter setNullFormat(String nullFormat)
    {
        this.nullFormat = nullFormat;
        return this;
    }

    public String getSslEnabled()
    {
        return sslEnabled;
    }

    public ParquetWriter setSslEnabled(String sslEnabled)
    {
        this.sslEnabled = sslEnabled;
        return this;
    }

    public String getDateFormat()
    {
        return dateFormat;
    }

    public ParquetWriter setDateFormat(String dateFormat)
    {
        this.dateFormat = dateFormat;
        return this;
    }

    public String getEncoding()
    {
        return encoding;
    }

    public ParquetWriter setEncoding(String encoding)
    {
        this.encoding = encoding;
        return this;
    }

    public String getBucket()
    {
        return bucket;
    }

    public ParquetWriter setBucket(String bucket)
    {
        this.bucket = bucket;
        return this;
    }

    public String getObject()
    {
        return object;
    }

    public ParquetWriter setObject(String object)
    {
        this.object = object;
        return this;
    }

    public List<String> getHeader()
    {
        return header;
    }

    public ParquetWriter setHeader(List<String> header)
    {
        this.header = header;
        return this;
    }

    public S3Client getS3Client()
    {
        return s3Client;
    }

    public ParquetWriter setS3Client(S3Client s3Client)
    {
        this.s3Client = s3Client;
        return this;
    }

    @Override
    public void init(Configuration config)
    {

        this.fileName = "s3a://" + this.bucket + "/" + this.object;

        hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.s3a.access.key", config.getString(S3Key.ACCESS_ID));
        hadoopConf.set("fs.s3a.secret.key", config.getString(S3Key.ACCESS_KEY));
        hadoopConf.set("fs.s3a.endpoint", config.getString(S3Key.ENDPOINT));
        hadoopConf.set("fs.s3a.ssl.enabled", config.getString(S3Key.SSL_ENABLED, "true"));
        hadoopConf.set("fs.s3a.path.style.access", config.getString(S3Key.PATH_STYLE_ACCESS_ENABLED, "false"));
    }

    /*
     * the schema of a parquet file is as follows:
     * {
     *    "type":	"record",
     *    "name":	"testFile",
     *    "doc":	"test records",
     *    "fields":
     *      [{
     *        "name":	"id",
     *        "type":	["null", "int"]
     *
     *      },
     *      {
     *        "name":	"empName",
     *        "type":	"string"
     *      }
     *    ]
     *  }
     * "null" 表示该字段允许为空
     */
    @Override
    public void write(RecordReceiver lineReceiver, Configuration config, TaskPluginCollector taskPluginCollector)
    {
        List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, "UNCOMPRESSED").toUpperCase().trim();
        if ("NONE".equals(compress)) {
            compress = "UNCOMPRESSED";
        }
        CompressionCodecName codecName = CompressionCodecName.fromConf(compress);
        // construct parquet schema
        MessageType s = generateParquetSchema(columns);
        Path path = new Path(fileName);
        logger.info("Begin to write parquet file [{}]", fileName);

        GenericData decimalSupport = new GenericData();
        decimalSupport.addLogicalTypeConversion(new Conversions.DecimalConversion());
        hadoopConf.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);
        hadoopConf.setBoolean(AvroWriteSupport.WRITE_FIXED_AS_INT96, true);
        GroupWriteSupport.setSchema(s, hadoopConf);
        Map<String, String> extraMeta = new HashMap<>();
        // hive need timezone info to handle timestamp
        extraMeta.put("writer.time.zone", ZoneId.systemDefault().toString());
        try (org.apache.parquet.hadoop.ParquetWriter<Group> writer = ExampleParquetWriter.builder(HadoopOutputFile.fromPath(path, hadoopConf))
                .withCompressionCodec(codecName)
                .withConf(hadoopConf)
                .enableDictionaryEncoding()
                .withPageSize(1024)
                .withDictionaryPageSize(512)
                .withValidation(false)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withExtraMetaData(extraMeta)
                .build()) {
            SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(s);
            Group group;
            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                group = buildRecord(record, columns, taskPluginCollector, simpleGroupFactory);
                writer.write(group);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Group buildRecord(
            Record record, List<Configuration> columns,
            TaskPluginCollector taskPluginCollector, SimpleGroupFactory simpleGroupFactory)
    {
        Column column;
        Group group = simpleGroupFactory.newGroup();
        for (int i = 0; i < record.getColumnNumber(); i++) {
            column = record.getColumn(i);
            String colName = columns.get(i).getString(Key.NAME);
            String typename = columns.get(i).getString(Key.TYPE).toUpperCase();
            if (null == column || column.getRawData() == null) {
                continue;
            }
            SupportHiveDataType columnType = SupportHiveDataType.valueOf(typename);
            switch (columnType) {
                case INT:
                case INTEGER:
                    group.append(colName, Integer.parseInt(column.getRawData().toString()));
                    break;
                case BIGINT:
                case LONG:
                    group.append(colName, column.asLong());
                    break;
                case FLOAT:
                    group.append(colName, column.asDouble().floatValue());
                    break;
                case DOUBLE:
                    group.append(colName, column.asDouble());
                    break;
                case STRING:
                    group.append(colName, column.asString());
                    break;
                case BOOLEAN:
                    group.append(colName, column.asBoolean());
                    break;
                case DECIMAL:
                    int scale = columns.get(i).getInt(Key.SCALE, Constant.DEFAULT_DECIMAL_MAX_SCALE);
                    group.append(colName, decimalToBinary(column.asString(), scale));
                    break;
                case TIMESTAMP:
                    group.append(colName, tsToBinary(column.asTimestamp()));
                    break;
                case DATE:
                    group.append(colName, (int) Math.round(column.asLong() * 1.0 / 86400000));
                    break;
                default:
                    logger.debug("convert type[{}] into string", column.getType());
                    group.append(colName, column.asString());
                    break;
            }
        }
        return group;
    }

    /**
     * convert timestamp to parquet INT96
     *
     * @param ts the {@link Timestamp} want to convert
     * @return {@link Binary}
     */
    private Binary tsToBinary(Timestamp ts)
    {
        long millis = ts.getTime();
        int julianDays = (int) (millis / 86400000L) + 2440588;
        long nanosOfDay = (millis % 86400000L) * 1000000L;

        // Write INT96 timestamp
        byte[] timestampBuffer = new byte[12];
        ByteBuffer buf = ByteBuffer.wrap(timestampBuffer);
        buf.order(ByteOrder.LITTLE_ENDIAN).putLong(nanosOfDay).putInt(julianDays).flip();

        // This is the properly encoded INT96 timestamp
        return Binary.fromReusedByteArray(timestampBuffer);
    }

    /**
     * convert Decimal to {@link Binary} using fix 16 bytes array
     *
     * @param bigDecimal the decimal value string want to convert
     * @return {@link Binary}
     */
    private Binary decimalToBinary(String bigDecimal, int scale)
    {
        int realScale = new BigDecimal(bigDecimal).scale();
        RoundingMode mode = scale >= realScale ? RoundingMode.UNNECESSARY : RoundingMode.HALF_UP;
        byte[] decimalBytes = new BigDecimal(bigDecimal)
                .setScale(scale, mode)
                .unscaledValue()
                .toByteArray();

        byte[] myDecimalBuffer = new byte[16];
        if (myDecimalBuffer.length >= decimalBytes.length) {
            //Because we set our fixed byte array size as 16 bytes, we need to
            //pad-left our original value's bytes with zeros
            int myDecimalBufferIndex = myDecimalBuffer.length - 1;
            for (int i = decimalBytes.length - 1; i >= 0; i--) {
                myDecimalBuffer[myDecimalBufferIndex] = decimalBytes[i];
                myDecimalBufferIndex--;
            }
            return Binary.fromConstantByteArray(myDecimalBuffer);
        }
        else {
            throw new IllegalArgumentException(String.format("Decimal size: %d was greater than the allowed max: %d",
                    decimalBytes.length, myDecimalBuffer.length));
        }
    }

    private MessageType generateParquetSchema(List<Configuration> columns)
    {
        String type;
        String fieldName;
        Type t;
        Types.MessageTypeBuilder builder = Types.buildMessage();
        Type.Repetition repetition = Type.Repetition.OPTIONAL;
        for (Configuration column : columns) {
            type = column.getString(Key.TYPE).trim().toUpperCase();
            fieldName = column.getString(Key.NAME);
            switch (type) {
                case "INT":
                    t = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition).named(fieldName);
                    break;
                case "BIGINT":
                case "LONG":
                    t = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition).named(fieldName);
                    break;
                case "DECIMAL":
                    // use fixed 16 bytes array
                    int prec = column.getInt(Key.PRECISION, Constant.DEFAULT_DECIMAL_MAX_PRECISION);
                    int scale = column.getInt(Key.SCALE, Constant.DEFAULT_DECIMAL_MAX_SCALE);
                    t = Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
                            .length(16)
                            .as(decimalType(scale, prec))
                            .named(fieldName);
                    break;
                case "STRING":
                    t = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition).as(LogicalTypeAnnotation.stringType()).named(fieldName);
                    break;
                case "BYTES":
                    t = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition).named(fieldName);
                    break;
                case "DATE":
                    t = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition).as(LogicalTypeAnnotation.dateType()).named(fieldName);
                    break;
                case "TIMESTAMP":
                    t = Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, repetition).named(fieldName);
                    break;
                default:
                    t = Types.primitive(PrimitiveType.PrimitiveTypeName.valueOf(type), Type.Repetition.OPTIONAL).named(fieldName);
                    break;
            }
            builder.addField(t);
        }
        return builder.named("addax");
    }
}
