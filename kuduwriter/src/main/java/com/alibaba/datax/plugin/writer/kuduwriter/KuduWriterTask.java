package com.alibaba.datax.plugin.writer.kuduwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import org.apache.kudu.Type;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Upsert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class KuduWriterTask
{
    private static final Logger LOG = LoggerFactory.getLogger(KuduWriterTask.class);
    public KuduClient kuduClient;
    public KuduSession session;
    private List<Configuration> columns;
    private List<List<Configuration>> columnLists;
    private List<Map<String, Type>> needColumns;
    private ThreadPoolExecutor pool;
    private String encoding;
    private Double batchSize;
    private Boolean isUpsert;
    private Boolean isSkipFail;
    private KuduTable table;
    private Integer primaryKeyIndexUntil;

    public KuduWriterTask(Configuration configuration)
    {
        columns = configuration.getListConfiguration(Key.COLUMN);
        columnLists = KuduHelper.getColumnLists(columns);
        pool = KuduHelper.createRowAddThreadPool(columnLists.size());

        this.encoding = configuration.getString(Key.ENCODING);
        this.batchSize = configuration.getDouble(Key.WRITE_BATCH_SIZE);
        this.isUpsert = !"insert".equalsIgnoreCase(configuration.getString(Key.INSERT_MODE));
        this.isSkipFail = configuration.getBool(Key.SKIP_FAIL);
        long mutationBufferSpace = configuration.getLong(Key.MUTATION_BUFFER_SPACE);

        this.kuduClient = KuduHelper.getKuduClient(configuration);
        this.table = KuduHelper.getKuduTable(this.kuduClient,
                configuration.getString(Key.KUDU_TABLE_NAME));
        needColumns = KuduHelper.getColumns(this.kuduClient, configuration);
        this.session = kuduClient.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace((int) mutationBufferSpace);
        this.primaryKeyIndexUntil = KuduHelper.getPrimaryKeyIndexUntil(columns);
//        tableName = configuration.getString(Key.TABLE);
    }

    public void startWriter(RecordReceiver lineReceiver, TaskPluginCollector taskPluginCollector)
    {
        LOG.info("kuduwriter begin to write!");
        Record record;
        try {
            while ((record = lineReceiver.getFromReader()) != null) {
//                if (record.getColumnNumber() != columns.size()) {
//                    throw DataXException.asDataXException(KuduWriterErrorCode.PARAMETER_NUM_ERROR,
//                            " number of record fields:" + record.getColumnNumber()
//                                    + " number of configuration fields:" + columns.size());
//                }
                boolean isDirtyRecord = false;

//                for (int i = 0; i < primaryKeyIndexUntil && !isDirtyRecord; i++) {
//                    Column column = record.getColumn(i);
//                    isDirtyRecord = StringUtils.isBlank(column.asString());
//                }
//
//                if (isDirtyRecord) {
//                    taskPluginCollector.collectDirtyRecord(record, "primarykey field is null");
//                    continue;
//                }
                Upsert upsert = table.newUpsert();
                Insert insert = table.newInsert();
                PartialRow row;
                if (isUpsert) {
                    //覆盖更新
                    row = upsert.getRow();
                }
                else {
                    //增量更新
                    row = insert.getRow();
                }
                for (int i = 0; i < record.getColumnNumber(); i++) {
                    Column column = record.getColumn(i);
                    String name = needColumns.get(i).keySet().toArray()[0].toString();
                    Type type = (Type) needColumns.get(i).keySet().toArray()[1];
                    if (column.getRawData() == null) {
                        row.setNull(name);
                        continue;
                    }
                    switch (type) {
                        case INT8:
                        case INT16:
                        case INT32:
                            row.addInt(name, Integer.parseInt(column.asString()));
                            break;
                        case FLOAT:
                        case DOUBLE:
                            row.addDouble(name, column.asDouble());
                            break;
                        case STRING:
                            row.addString(name, column.asString());
                            break;
                        case BOOL:
                            row.addBoolean(name, column.asBoolean());
                            break;
                        case BINARY:
                            row.addBinary(name, column.asBytes());
                            break;
                        case DECIMAL:
                            row.addDecimal(name, new BigDecimal(column.asString()));
                            break;
                        case UNIXTIME_MICROS:
                            row.addTimestamp(name, new Timestamp(column.asLong()));
                            break;
                        case DATE:
                            row.addDate(name,
                                    (Date) new java.util.Date(column.asDate().getTime()));
                            break;
                        default:
                            row.addString(name, column.asString());
                            break;
                    }
                    try {
                        if (isUpsert) {
                            session.apply(upsert);
                        }
                        else {
                            session.apply(insert);
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        LOG.error("Record Write Failure!", e);
                        if (isSkipFail) {
                            LOG.warn("Since you have configured \"skipFail\" to be true, this record will be skipped !");
                            taskPluginCollector.collectDirtyRecord(record, e.getMessage());
                        }
                        else {
                            throw DataXException.asDataXException(KuduWriterErrorCode.PUT_KUDU_ERROR, e.getMessage());
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            LOG.error("write failure! the task will exit!");
            throw DataXException.asDataXException(KuduWriterErrorCode.PUT_KUDU_ERROR, e.getMessage());
        }
        AtomicInteger i = new AtomicInteger(10);
        try {
            while (i.get() > 0) {
                if (session.hasPendingOperations()) {
                    session.flush();
                    break;
                }
                Thread.sleep(20L);
                i.decrementAndGet();
            }
        }
        catch (Exception e) {
            LOG.info("Waiting for data to be written to kudu...... " + i + "s");
        }
        finally {
            try {
                pool.shutdown();
                //强制刷写
                session.flush();
            }
            catch (KuduException e) {
                LOG.error("kuduwriter flush error! The results may be incomplete！");
                throw DataXException.asDataXException(KuduWriterErrorCode.PUT_KUDU_ERROR, e.getMessage());
            }
        }
    }
}
