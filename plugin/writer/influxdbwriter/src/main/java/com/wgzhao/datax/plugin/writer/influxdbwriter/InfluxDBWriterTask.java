package com.wgzhao.datax.plugin.writer.influxdbwriter;

import com.wgzhao.datax.common.element.Column;
import com.wgzhao.datax.common.element.Record;
import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.plugin.RecordReceiver;
import com.wgzhao.datax.common.plugin.TaskPluginCollector;
import com.wgzhao.datax.common.util.Configuration;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InfluxDBWriterTask
{
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBWriterTask.class);

    private final Configuration configuration;
    protected List<Configuration> columns;
    protected int columnNumber = 0;
    private String database;
    private String table;
    private int batchSize;
    private InfluxDB influxDB;

    public InfluxDBWriterTask(Configuration configuration) {this.configuration = configuration;}

    public void init()
    {
        List<Object> connList = configuration.getList(Key.CONNECTION);
        Configuration conn = Configuration.from(connList.get(0).toString());
        String endpoint = conn.getString(Key.ENDPOINT);
        this.table = conn.getString(Key.TABLE);
        this.database = conn.getString(Key.DATABASE);
        this.columns = configuration.getListConfiguration(Key.COLUMN);
        this.columnNumber = this.columns.size();
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD, null);
        this.influxDB = InfluxDBFactory.connect(endpoint, username, password);
        this.batchSize = configuration.getInt(Key.BATCH_SIZE, 1024);
        influxDB.setDatabase(database);
    }

    public void prepare()
    {
        //
    }

    public void post()
    {
        List<String> postSqls = configuration.getList(Key.POST_SQL, String.class);
        if (!postSqls.isEmpty()) {
            for (String sql :postSqls) {
                this.influxDB.query(new Query(sql));
            }
        }
    }

    public void destroy()
    {
        //
    }

    public void startWrite( RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector)
    {
        influxDB.enableBatch(batchSize, 100, TimeUnit.MILLISECONDS);
        // Retention policy
        String rp = "autogen";
        Record record = null;
        try {
            while ((record = recordReceiver.getFromReader()) != null ) {
                if (record.getColumnNumber() != this.columnNumber) {
                    throw DataXException.asDataXException(
                            InfluxDBWriterErrorCode.CONF_ERROR,
                            String.format(
                                    "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                    record.getColumnNumber(),
                                    this.columnNumber)
                    );
                }
                Point.Builder builder = Point.measurement(table);
                Map<String, Object> fields = new HashMap<>();
                // 第一个字符必须是时间戳类型
                builder.time(record.getColumn(0).asLong(), TimeUnit.MILLISECONDS);
                for (int i=1; i< columnNumber; i++) {
                    String name = this.columns.get(i).getString("name");
                    String type = this.columns.get(i).getString("type").toUpperCase();
                    Column column = record.getColumn(i);
                    switch (type) {
                        case "INT":
                        case "LONG":
                            fields.put(name, column.asLong());
                            break;
                        case "DATE":
                            fields.put(name, column.asDate());
                            break;
                        case "DOUBLE":
                            fields.put(name, column.asDouble());
                            break;
                        case "DECIMAL":
                            fields.put(name, column.asBigDecimal());
                            break;
                        case "BINARY":
                            fields.put(name, column.asBytes());
                            break;
                        default:
                            fields.put(name, column.asString());
                            break;
                    }
                }
                builder.fields(fields);
                influxDB.write(database, rp, builder.build());
            }
        }
        catch (Exception e) {
            taskPluginCollector.collectDirtyRecord(record, e);
            throw DataXException.asDataXException(InfluxDBWriterErrorCode.ILLEGAL_VALUE, e);
        }
    }
}
