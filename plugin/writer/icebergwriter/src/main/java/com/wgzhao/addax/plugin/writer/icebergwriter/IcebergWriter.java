package com.wgzhao.addax.plugin.writer.icebergwriter;

import com.alibaba.fastjson2.JSON;
import com.google.common.collect.ImmutableList;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;


public class IcebergWriter extends Writer {
    public static class Job
            extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration conf = null;
        private Catalog catalog = null;
        private String tableName = null;

        @Override
        public void init() {
            this.conf = this.getPluginJobConf();
            try {
                this.catalog = IcebergHelper.getCatalog(conf);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            tableName = this.conf.getString("tableName");
            if (tableName == null || tableName.trim().isEmpty()) {
                throw new RuntimeException("tableName is not set");
            }


        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(conf);
            }
            return configurations;
        }

        @Override
        public void prepare() {
            String writeMode = this.conf.getString("writeMode");
            if ("truncate".equalsIgnoreCase(writeMode)) {
                Table table = catalog.loadTable(TableIdentifier.of(tableName.split("\\.")));
                table.newDelete().deleteFromRowFilter(org.apache.iceberg.expressions.Expressions.alwaysTrue()).commit();
            }
        }

        @Override
        public void destroy() {
            if (this.catalog != null) {
                try {
                    if (this.catalog instanceof HiveCatalog) {
                        ((HiveCatalog) this.catalog).close();
                    }
                    if (this.catalog instanceof HadoopCatalog) {
                        ((HadoopCatalog) this.catalog).close();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

        }
    }

    public static class Task
            extends Writer.Task {

        private static final Logger log = LoggerFactory.getLogger(Task.class);
        private Catalog catalog = null;
        private Integer batchSize = 1000;
        private Table table = null;
        private org.apache.iceberg.Schema schema = null;
        private String fileFormat = "parquet";
        private List<org.apache.iceberg.types.Types.NestedField> columnList = null;

        @Override
        public void startWrite(RecordReceiver recordReceiver) {

            List<Record> writerBuffer = new ArrayList<>(this.batchSize);
            Record record;
            long total = 0;
            while ((record = recordReceiver.getFromReader()) != null) {
                writerBuffer.add(record);
                if (writerBuffer.size() >= this.batchSize) {
                    total += doBatchInsert(writerBuffer);
                    writerBuffer.clear();
                }
            }

            if (!writerBuffer.isEmpty()) {
                total += doBatchInsert(writerBuffer);
                writerBuffer.clear();
            }

            String msg = String.format("task end, write size :%d", total);
            getTaskPluginCollector().collectMessage("writeSize", String.valueOf(total));
            log.info(msg);
        }

        @Override
        public void init() {
            Configuration conf = super.getPluginJobConf();

            batchSize = conf.getInt("batchSize", 1000);

            try {
                this.catalog = IcebergHelper.getCatalog(conf);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            String tableName = conf.getString("tableName");
            if (tableName == null || tableName.trim().isEmpty()) {
                throw new RuntimeException("tableName is not set");
            }

            table = catalog.loadTable(TableIdentifier.of(tableName.split("\\.")));
            schema = table.schema();

            fileFormat = table.properties().get("write.format.default");
            if (fileFormat == null || fileFormat.trim().isEmpty()) {
                fileFormat = "parquet";
            }


            columnList = schema.columns();
        }

        @Override
        public void destroy() {
            if (this.catalog != null) {
                try {
                    if (this.catalog instanceof HiveCatalog) {
                        ((HiveCatalog) this.catalog).close();
                    }
                    if (this.catalog instanceof HadoopCatalog) {
                        ((HadoopCatalog) this.catalog).close();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

        }

        private long doBatchInsert(final List<Record> writerBuffer) {
            ImmutableList.Builder<GenericRecord> builder = ImmutableList.builder();


            for (Record record : writerBuffer) {
                GenericRecord data = GenericRecord.create(schema);
                for (int i = 0; i < record.getColumnNumber(); i++) {
                    Column column = record.getColumn(i);
                    if (column == null) {
                        continue;
                    }
                    if (i >= columnList.size()) {
                        throw new RuntimeException("columnList size is " + columnList.size() + ", but record column number is " + record.getColumnNumber());
                    }
                    Types.NestedField field = columnList.get(i);
                    org.apache.iceberg.types.Type columnType = field.type();
                    //如果是数组类型，那它传入的必是字符串类型
                    if (columnType.isListType()) {
                        if (null == column.asString()) {
                            data.setField(field.name(), null);
                        } else {
                            String[] dataList = column.asString().split(",");
                            data.setField(field.name(), dataList);
                        }
                    } else {
                        switch (columnType.typeId()) {

                            case DATE:
                                try {
                                    if (column.asLong() != null) {
                                        data.setField(field.name(), column.asTimestamp().toLocalDateTime());
                                    } else {
                                        data.setField(field.name(), null);
                                    }
                                } catch (Exception e) {
                                    getTaskPluginCollector().collectDirtyRecord(record, String.format("日期类型解析失败 [%s:%s] exception: %s", field.name(), column, e));
                                }
                                break;
                            case TIME:
                            case TIMESTAMP:
                            case TIMESTAMP_NANO:
                                try {
                                    if (column.asLong() != null) {
                                        data.setField(field.name(), column.asTimestamp().toLocalDateTime());
                                    } else {
                                        data.setField(field.name(), null);
                                    }
                                } catch (Exception e) {
                                    getTaskPluginCollector().collectDirtyRecord(record, String.format("时间类型解析失败 [%s:%s] exception: %s", field.name(), column, e));
                                }
                                break;
                            case STRING:
                                data.setField(field.name(), column.asString());
                                break;
                            case BOOLEAN:
                                data.setField(field.name(), column.asBoolean());
                                break;
                            case FIXED:
                            case BINARY:
                                data.setField(field.name(), column.asBytes());
                                break;
                            case LONG:
                                data.setField(field.name(), column.asLong());
                                break;
                            case INTEGER:
                                data.setField(field.name(), column.asBigInteger() == null ? null : column.asBigInteger().intValue());
                                break;
                            case FLOAT:
                                data.setField(field.name(), column.asDouble().floatValue());
                                break;
                            case DOUBLE:

                                data.setField(field.name(), column.asDouble());
                                break;
                            case DECIMAL:
                                if (column.asBigDecimal() != null) {
                                    data.setField(field.name(), column.asBigDecimal());
                                } else {
                                    data.setField(field.name(), null);
                                }
                                break;
                            case MAP:
                                try {
                                    data.setField(field.name(), JSON.parseObject(column.asString(), Map.class));
                                } catch (Exception e) {
                                    getTaskPluginCollector().collectDirtyRecord(record, String.format("MAP类型解析失败 [%s:%s] exception: %s", field.name(), column, e));
                                }
                                break;
                            case VARIANT:
                                try {
                                    data.setField(field.name(), JSON.parseObject(column.asString(), Map.class));
                                } catch (Exception e) {
                                    getTaskPluginCollector().collectDirtyRecord(record, String.format("VARIANT类型解析失败 [%s:%s] exception: %s", field.name(), column, e));
                                }
                                break;
                            default:
                                getTaskPluginCollector().collectDirtyRecord(record, "类型错误:不支持的类型:" + columnType + " " + field.name());
                        }
                    }

                }


                builder.add(data);
            }

            String filepath = table.location() + "/" + UUID.randomUUID();
            OutputFile file = table.io().newOutputFile(filepath);

            DataWriter<GenericRecord> dataWriter = null;

            if ("parquet".equals(fileFormat)) {
                try {
                    dataWriter = Parquet.writeData(file).overwrite().forTable(table).createWriterFunc(GenericParquetWriter::buildWriter).build();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else if ("orc".equals(fileFormat)) {
                dataWriter = ORC.writeData(file).overwrite().forTable(table).createWriterFunc(GenericOrcWriter::buildWriter).build();
            } else {
                throw new RuntimeException("不支持的文件格式:" + fileFormat);
            }
            ImmutableList<GenericRecord> rows = builder.build();

            if (dataWriter != null) {
                dataWriter.write(rows);
            }


            if (dataWriter != null) {
                try {
                    dataWriter.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            DataFile dataFile = dataWriter.toDataFile();
            table.newAppend().appendFile(dataFile).commit();
            return rows.size();
        }
    }
}
