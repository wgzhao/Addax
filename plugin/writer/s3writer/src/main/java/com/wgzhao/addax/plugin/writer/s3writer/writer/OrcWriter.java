package com.wgzhao.addax.plugin.writer.s3writer.writer;

import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.plugin.writer.s3writer.S3Key;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Base64;
import java.util.List;
import java.util.StringJoiner;

import static com.wgzhao.addax.core.spi.ErrorCode.*;

public class OrcWriter
        implements IFormatWriter
{
    private final Logger logger = LoggerFactory.getLogger(OrcWriter.class.getName());
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

    public OrcWriter()
    {
    }

    public char getFieldDelimiter()
    {
        return fieldDelimiter;
    }

    public OrcWriter setFieldDelimiter(char fieldDelimiter)
    {
        this.fieldDelimiter = fieldDelimiter;
        return this;
    }

    public String getNullFormat()
    {
        return nullFormat;
    }

    public OrcWriter setNullFormat(String nullFormat)
    {
        this.nullFormat = nullFormat;
        return this;
    }

    public String getSslEnabled()
    {
        return sslEnabled;
    }

    public OrcWriter setSslEnabled(String sslEnabled)
    {
        this.sslEnabled = sslEnabled;
        return this;
    }

    public String getDateFormat()
    {
        return dateFormat;
    }

    public OrcWriter setDateFormat(String dateFormat)
    {
        this.dateFormat = dateFormat;
        return this;
    }

    public String getEncoding()
    {
        return encoding;
    }

    public OrcWriter setEncoding(String encoding)
    {
        this.encoding = encoding;
        return this;
    }

    public String getBucket()
    {
        return bucket;
    }

    public OrcWriter setBucket(String bucket)
    {
        this.bucket = bucket;
        return this;
    }

    public String getObject()
    {
        return object;
    }

    public OrcWriter setObject(String object)
    {
        this.object = object;
        return this;
    }

    public List<String> getHeader()
    {
        return header;
    }

    public OrcWriter setHeader(List<String> header)
    {
        this.header = header;
        return this;
    }

    public S3Client getS3Client()
    {
        return s3Client;
    }

    public OrcWriter setS3Client(S3Client s3Client)
    {
        this.s3Client = s3Client;
        return this;
    }

    /**
     * write an orc record
     *
     * @param batch {@link VectorizedRowBatch}
     * @param row row number
     * @param record {@link Record}
     * @param columns table columns, {@link List}
     * @param taskPluginCollector {@link TaskPluginCollector}
     */
    private void setRow(VectorizedRowBatch batch, int row, Record record, List<Configuration> columns,
            TaskPluginCollector taskPluginCollector)
    {
        for (int i = 0; i < columns.size(); i++) {
            Configuration eachColumnConf = columns.get(i);
            String type = eachColumnConf.getString(Key.TYPE).trim().toUpperCase();
            SupportHiveDataType columnType;
            ColumnVector col = batch.cols[i];
            if (type.startsWith("DECIMAL")) {
                columnType = SupportHiveDataType.DECIMAL;
            }
            else {
                columnType = SupportHiveDataType.valueOf(type);
            }
            if (record.getColumn(i) == null || record.getColumn(i).getRawData() == null) {
                col.isNull[row] = true;
                col.noNulls = false;
                continue;
            }

            try {
                switch (columnType) {
                    case TINYINT:
                    case SMALLINT:
                    case INT:
                    case BIGINT:
                    case BOOLEAN:
                        ((LongColumnVector) col).vector[row] = record.getColumn(i).asLong();
                        break;
                    case DATE:
                        ((LongColumnVector) col).vector[row] = LocalDate.parse(record.getColumn(i).asString()).toEpochDay();
                        break;
                    case FLOAT:
                    case DOUBLE:
                        ((DoubleColumnVector) col).vector[row] = record.getColumn(i).asDouble();
                        break;
                    case DECIMAL:
                        HiveDecimalWritable hdw = new HiveDecimalWritable();
                        hdw.set(HiveDecimal.create(record.getColumn(i).asBigDecimal())
                                .setScale(eachColumnConf.getInt(Key.SCALE), HiveDecimal.ROUND_HALF_UP));
                        ((DecimalColumnVector) col).set(row, hdw);
                        break;
                    case TIMESTAMP:
                        ((TimestampColumnVector) col).set(row, record.getColumn(i).asTimestamp());
                        break;
                    case STRING:
                    case VARCHAR:
                    case CHAR:
                        byte[] buffer;
                        Column column = record.getColumn(i);
                        Column.Type colType = column.getType();
                        if (colType == Column.Type.BYTES) {
                            //convert bytes to base64 string
                            buffer = Base64.getEncoder().encode((byte[]) column.getRawData());
                        }
                        else if (colType == Column.Type.DATE) {
                            if (((DateColumn) column).getSubType() == DateColumn.DateType.TIME) {
                                buffer = column.asString().getBytes(StandardCharsets.UTF_8);
                            }
                            else {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                buffer = sdf.format(record.getColumn(i).asDate()).getBytes(StandardCharsets.UTF_8);
                            }
                        }
                        else {
                            buffer = record.getColumn(i).getRawData().toString().getBytes(StandardCharsets.UTF_8);
                        }
                        ((BytesColumnVector) col).setRef(row, buffer, 0, buffer.length);
                        break;
                    case BINARY:
                        byte[] content = (byte[]) record.getColumn(i).getRawData();
                        ((BytesColumnVector) col).setRef(row, content, 0, content.length);
                        break;
                    default:
                        throw AddaxException
                                .asAddaxException(
                                        NOT_SUPPORT_TYPE,
                                        String.format("The columns configuration is incorrect. the field type is unsupported yet. Field name: [%s], Field type name:[%s].",
                                                eachColumnConf.getString(Key.NAME),
                                                eachColumnConf.getString(Key.TYPE)));
                }
            }
            catch (Exception e) {
                taskPluginCollector.collectDirtyRecord(record, e.getMessage());
                throw AddaxException.asAddaxException(RUNTIME_ERROR,
                        String.format("Failed to set ORC row, source field type: %s, destination field original type: %s, " +
                                        "destination field hive type: %s, field name: %s, source field value: %s, root cause:%n%s",
                                record.getColumn(i).getType(), columnType, eachColumnConf.getString(Key.TYPE),
                                eachColumnConf.getString(Key.NAME),
                                record.getColumn(i).getRawData(), e));
            }
        }
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

    @Override
    public void write(RecordReceiver lineReceiver, Configuration config,
            TaskPluginCollector taskPluginCollector)
    {
        List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, "NONE").toUpperCase();
        StringJoiner joiner = new StringJoiner(",");
        for (Configuration column : columns) {
            if ("decimal".equals(column.getString(Key.TYPE))) {
                joiner.add(String.format("%s:%s(%s,%s)", column.getString(Key.NAME), "decimal",
                        column.getInt(Key.PRECISION, Constant.DEFAULT_DECIMAL_MAX_PRECISION),
                        column.getInt(Key.SCALE, Constant.DEFAULT_DECIMAL_MAX_SCALE)));
            }
            else {
                joiner.add(String.format("%s:%s", column.getString(Key.NAME), column.getString(Key.TYPE)));
            }
        }
        TypeDescription schema = TypeDescription.fromString("struct<" + joiner + ">");
        try (Writer writer = OrcFile.createWriter(new Path(fileName),
                OrcFile.writerOptions(hadoopConf)
                        .setSchema(schema)
                        .compress(CompressionKind.valueOf(compress)))) {
            Record record;
            VectorizedRowBatch batch = schema.createRowBatch(1024);
            while ((record = lineReceiver.getFromReader()) != null) {
                int row = batch.size++;
                setRow(batch, row, record, columns, taskPluginCollector);
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        catch (IOException e) {
            logger.error("IO exception occurred while writing file [{}}.", fileName);
            DeleteObjectsRequest dor = DeleteObjectsRequest.builder()
                    .bucket(bucket)
                    .delete(Delete.builder().objects(ObjectIdentifier.builder().key(object).build()).build())
                    .build();
            s3Client.deleteObjects(dor);
            throw AddaxException.asAddaxException(IO_ERROR, e);
        }
    }
}
