package com.alibaba.datax.plugin.reader.influxdbreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;

public class InfluxDBReaderTask
{
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBReaderTask.class);

    private final Configuration configuration;

    public InfluxDBReaderTask(Configuration configuration) {this.configuration = configuration;}

    public void post()
    {
    }

    public void destroy()
    {
    }

    public void startRead(Configuration readerSliceConfig, RecordSender recordSender,
            TaskPluginCollector taskPluginCollector)
    {
        List<Object> connList = readerSliceConfig.getList(Key.CONNECTION);
        Configuration conn = Configuration.from(connList.get(0).toString());
        String querySql = readerSliceConfig.getString(Key.QUERY_SQL, null);
        String table = conn.getString(Key.TABLE);
        String database = conn.getString(Key.DATABASE);
        String endpoint = conn.getString(Key.ENDPOINT);
        String username = readerSliceConfig.getString(Key.USERNAME);
        String password = readerSliceConfig.getString(Key.PASSWORD, null);
        String where = readerSliceConfig.getString(Key.WHERE, null);
        String basicMsg = String.format("http server:[%s]", endpoint);
        LOG.info("connect influxdb: {} with username: {}", endpoint, username);
        InfluxDB influxDB = InfluxDBFactory.connect(endpoint, username, password);
        influxDB.setDatabase(database);
        if (querySql == null) {

            if (where != null) {
                querySql = "select * from " + table + " where " + where;
            } else {
                querySql = "select * from " + table;
            }
        }

        QueryResult rs;
        Record record = null;
        try {
            rs = influxDB.query(new Query(querySql));
            QueryResult.Result  result  = rs.getResults().get(0);
            List<QueryResult.Series> seriesList = result.getSeries();
            if (seriesList == null) {
                return;
            }
            QueryResult.Series series = seriesList.get(0);
            List<List<Object>> values = series.getValues();
            for(List<Object> row : values){
                record = recordSender.createRecord();
                for(Object v : row) {
                    if (v == null) {
                        record.addColumn(new StringColumn(null));
                    } else {
                        record.addColumn(new StringColumn(v.toString()));
                    }
                }
                recordSender.sendToWriter(record);
            }

        }
        catch (Exception e) {
            taskPluginCollector.collectDirtyRecord(record, e);
            throw DataXException.asDataXException(InfluxDBReaderErrorCode.ILLEGAL_VALUE, e);
        }
        finally {
            influxDB.close();
        }
    }
}
