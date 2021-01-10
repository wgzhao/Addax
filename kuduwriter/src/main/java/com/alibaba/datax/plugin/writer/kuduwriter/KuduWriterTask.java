package com.alibaba.datax.plugin.writer.kuduwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
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

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class KuduWriterTask
{
    private static final Logger LOG = LoggerFactory.getLogger(KuduWriterTask.class);
    public KuduClient kuduClient;
    public KuduSession session;
    private List<Configuration> columns;
    private List<List<Configuration>> columnLists;
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

        this.kuduClient = KuduHelper.getKuduClient(configuration.getString(Key.KUDU_CONFIG));
        this.table = KuduHelper.getKuduTable(configuration, kuduClient);
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
                if (record.getColumnNumber() != columns.size()) {
                    throw DataXException.asDataXException(KuduWriterErrorCode.PARAMETER_NUM_ERROR,
                            " number of record fields:" + record.getColumnNumber()
                                    + " number of configuration fields:" + columns.size());
                }
                boolean isDirtyRecord = false;

                for (int i = 0; i < primaryKeyIndexUntil && !isDirtyRecord; i++) {
                    Column column = record.getColumn(i);
                    isDirtyRecord = StringUtils.isBlank(column.asString());
                }

                if (isDirtyRecord) {
                    taskPluginCollector.collectDirtyRecord(record, "primarykey field is null");
                    continue;
                }

                PartialRow row;
                if (isUpsert) {
                    //覆盖更新
                    Upsert insert = table.newUpsert();
                    row = insert.getRow();
                }
                else {
                    //增量更新
                    Insert insert = table.newInsert();
                    row = insert.getRow();
                }
                for (List<Configuration> columnList : columnLists) {

                    for (Configuration col : columnList) {
                        String name = col.getString(Key.NAME);
                        ColumnType type = ColumnType.getByTypeName(col.getString(Key.TYPE, "string"));
                        Column column = record.getColumn(col.getInt(Key.INDEX));
                        String rawData = column.asString();
                        if (rawData == null) {
                            row.setNull(name);
                            continue;
                        }
                        switch (type) {
                            case INT:
                                row.addInt(name, Integer.parseInt(rawData));
                                break;
                            case LONG:
                            case BIGINT:
                                row.addLong(name, Long.parseLong(rawData));
                                break;
                            case FLOAT:
                                row.addFloat(name, Float.parseFloat(rawData));
                                break;
                            case DOUBLE:
                                row.addDouble(name, Double.parseDouble(rawData));
                                break;
                            case BOOLEAN:
                                row.addBoolean(name, Boolean.getBoolean(rawData));
                                break;
                            case STRING:
                            default:
                                row.addString(name, rawData);
                        }
                        try {
                                session.apply(insert);
                        }
                        catch (Exception e) {
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
