package com.wgzhao.addax.plugin.writer.starrockswriter;

import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.plugin.writer.starrockswriter.manager.StarRocksWriterManager;
import com.wgzhao.addax.plugin.writer.starrockswriter.row.StarRocksISerializer;
import com.wgzhao.addax.plugin.writer.starrockswriter.row.StarRocksSerializerFactory;
import com.wgzhao.addax.plugin.writer.starrockswriter.util.StarRocksWriterUtil;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.exception.CommonErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.exception.CommonErrorCode.EXECUTE_FAIL;

public class StarRocksWriter
        extends Writer
{

    public static class Job
            extends Writer.Job
    {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig = null;
        private StarRocksWriterOptions options;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
            options = new StarRocksWriterOptions(super.getPluginJobConf());
            options.doPretreatment();
        }

        @Override
        public void preCheck()
        {
            this.init();
            StarRocksWriterUtil.preCheckPrePareSQL(options);
            StarRocksWriterUtil.preCheckPostSQL(options);
        }

        @Override
        public void prepare()
        {
            String username = options.getUsername();
            String password = options.getPassword();
            String jdbcUrl = options.getJdbcUrl();
            List<String> renderedPreSqls = StarRocksWriterUtil.renderPreOrPostSqls(options.getPreSqlList(), options.getTable());
            if (!renderedPreSqls.isEmpty()) {
                Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);
                LOG.info("Begin to execute preSqls:[{}]. context info:{}.", String.join(";", renderedPreSqls), jdbcUrl);
                StarRocksWriterUtil.executeSqls(conn, renderedPreSqls);
                DBUtil.closeDBResources(null, null, conn);
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            List<Configuration> configurations = new ArrayList<>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(originalConfig);
            }
            return configurations;
        }

        @Override
        public void post()
        {
            String username = options.getUsername();
            String password = options.getPassword();
            String jdbcUrl = options.getJdbcUrl();
            List<String> renderedPostSqls = StarRocksWriterUtil.renderPreOrPostSqls(options.getPostSqlList(), options.getTable());
            if (!renderedPostSqls.isEmpty()) {
                Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);
                LOG.info("Begin to execute postSqls:[{}]. context info:{}.", String.join(";", renderedPostSqls), jdbcUrl);
                StarRocksWriterUtil.executeSqls(conn, renderedPostSqls);
                DBUtil.closeDBResources(null, null, conn);
            }
        }

        @Override
        public void destroy()
        {
        }
    }

    public static class Task
            extends Writer.Task
    {
        private StarRocksWriterManager writerManager;
        private StarRocksWriterOptions options;
        private StarRocksISerializer rowSerializer;

        @Override
        public void init()
        {
            options = new StarRocksWriterOptions(super.getPluginJobConf());
            if (options.isWildcardColumn()) {
                Connection conn = DBUtil.getConnection(DataBaseType.MySql, options.getJdbcUrl(), options.getUsername(), options.getPassword());
                List<String> columns = StarRocksWriterUtil.getStarRocksColumns(conn, options.getDatabase(), options.getTable());
                options.setInfoCchemaColumns(columns);
            }
            writerManager = new StarRocksWriterManager(options);
            rowSerializer = StarRocksSerializerFactory.createSerializer(options);
        }

        @Override
        public void prepare()
        {
        }

        public void startWrite(RecordReceiver recordReceiver)
        {
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if (record.getColumnNumber() != options.getColumns().size()) {
                        throw AddaxException
                                .asAddaxException(
                                        CONFIG_ERROR,
                                        String.format(
                                                "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                                record.getColumnNumber(),
                                                options.getColumns().size()));
                    }
                    writerManager.writeRecord(rowSerializer.serialize(record));
                }
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL, e);
            }
        }

        @Override
        public void post()
        {
            try {
                writerManager.close();
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL, e);
            }
        }

        @Override
        public void destroy() {}

        @Override
        public boolean supportFailOver()
        {
            return false;
        }
    }
}
