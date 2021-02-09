package com.wgzhao.datax.plugin.reader.cassandrareader;

import com.wgzhao.datax.common.plugin.RecordSender;
import com.wgzhao.datax.common.spi.Reader;
import com.wgzhao.datax.common.util.Configuration;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.wgzhao.datax.common.element.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CassandraReader
        extends Reader
{
    private static final Logger LOG = LoggerFactory
            .getLogger(CassandraReader.class);

    public static class Job
            extends Reader.Job
    {

        private Configuration jobConfig = null;
        private Cluster cluster = null;

        @Override
        public void init()
        {
            this.jobConfig = super.getPluginJobConf();
            this.jobConfig = super.getPluginJobConf();
            String username = jobConfig.getString(Key.USERNAME);
            String password = jobConfig.getString(Key.PASSWORD);
            String hosts = jobConfig.getString(Key.HOST);
            Integer port = jobConfig.getInt(Key.PORT, 9042);
            boolean useSSL = jobConfig.getBool(Key.USESSL);

            if ((username != null) && !username.isEmpty()) {
                Cluster.Builder clusterBuilder = Cluster.builder().withCredentials(username, password)
                        .withPort(port).addContactPoints(hosts.split(","));
                if (useSSL) {
                    clusterBuilder = clusterBuilder.withSSL();
                }
                cluster = clusterBuilder.build();
            }
            else {
                cluster = Cluster.builder().withPort(port)
                        .addContactPoints(hosts.split(",")).build();
            }
            CassandraReaderHelper.checkConfig(jobConfig, cluster);
        }

        @Override
        public void destroy()
        {

        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            return CassandraReaderHelper.splitJob(adviceNumber, jobConfig, cluster);
        }
    }

    public static class Task
            extends Reader.Task
    {
        private Session session = null;
        private String queryString = null;
        private ConsistencyLevel consistencyLevel;
        private int columnNumber = 0;

        @Override
        public void init()
        {
            Configuration taskConfig = super.getPluginJobConf();
            String username = taskConfig.getString(Key.USERNAME);
            String password = taskConfig.getString(Key.PASSWORD);
            String hosts = taskConfig.getString(Key.HOST);
            Integer port = taskConfig.getInt(Key.PORT);
            boolean useSSL = taskConfig.getBool(Key.USESSL);
            String keyspace = taskConfig.getString(Key.KEYSPACE);
            List<String> columnMeta = taskConfig.getList(Key.COLUMN, String.class);
            columnNumber = columnMeta.size();

            Cluster cluster;
            if ((username != null) && !username.isEmpty()) {
                Cluster.Builder clusterBuilder = Cluster.builder().withCredentials(username, password)
                        .withPort(port).addContactPoints(hosts.split(","));
                if (useSSL) {
                    clusterBuilder = clusterBuilder.withSSL();
                }
                cluster = clusterBuilder.build();
            }
            else {
                cluster = Cluster.builder().withPort(port)
                        .addContactPoints(hosts.split(",")).build();
            }
            session = cluster.connect(keyspace);
            String cl = taskConfig.getString(Key.CONSITANCY_LEVEL);
            if (cl != null && !cl.isEmpty()) {
                consistencyLevel = ConsistencyLevel.valueOf(cl);
            }
            else {
                consistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
            }

            queryString = CassandraReaderHelper.getQueryString(taskConfig, cluster);
            LOG.info("query = " + queryString);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            ResultSet r = session.execute(new SimpleStatement(queryString).setConsistencyLevel(consistencyLevel));
            for (Row row : r) {
                Record record = recordSender.createRecord();
                record = CassandraReaderHelper.buildRecord(record, row, r.getColumnDefinitions(), columnNumber,
                        super.getTaskPluginCollector());
                if (record != null) {
                    recordSender.sendToWriter(record);
                }
            }
        }

        @Override
        public void destroy()
        {

        }
    }
}
