package com.wgzhao.addax.plugin.writer.hdfswriter;

import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Base64;
import java.util.List;
import java.util.StringJoiner;

public class OrcWriter
        extends HdfsHelper
        implements IHDFSWriter
{
    private final Logger logger = LoggerFactory.getLogger(OrcWriter.class.getName());

    public OrcWriter(Configuration conf)
    {
        super();
        getFileSystem(conf);
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
                                        HdfsWriterErrorCode.ILLEGAL_VALUE,
                                        String.format("The columns configuration is incorrect. the field type is unsupported yet. Field name: [%s], Field type name:[%s].",
                                                eachColumnConf.getString(Key.NAME),
                                                eachColumnConf.getString(Key.TYPE)));
                }
            }
            catch (Exception e) {
                taskPluginCollector.collectDirtyRecord(record, e.getMessage());
                throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("Failed to set ORC row, source field type: %s, destination field original type: %s, " +
                                        "destination field hive type: %s, field name: %s, source field value: %s, root cause:%n%s",
                                record.getColumn(i).getType(), columnType, eachColumnConf.getString(Key.TYPE),
                                eachColumnConf.getString(Key.NAME),
                                record.getColumn(i).getRawData(), e));
            }
        }
    }

    @Override
    public void write(RecordReceiver lineReceiver, Configuration config, String fileName,
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
                OrcFile.writerOptions(conf)
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
            Path path = new Path(fileName);
            deleteDir(path.getParent());
            throw AddaxException.asAddaxException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
    }
}
