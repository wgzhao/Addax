package com.wgzhao.addax.plugin.writer.databendwriter;

import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.plugin.writer.databendwriter.manager.DatabendWriterManager;
import com.wgzhao.addax.plugin.writer.databendwriter.row.DatabendISerializer;
import com.wgzhao.addax.plugin.writer.databendwriter.row.DatabendSerializerFactory;
import com.wgzhao.addax.plugin.writer.databendwriter.util.DatabendWriterUtil;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DBUtilErrorCode;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DatabendWriter
        extends Writer
{

    public static class Job
            extends Writer.Job
    {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig = null;
        private DatabendWriterOptions options;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
            options = new DatabendWriterOptions(super.getPluginJobConf());
            options.doPretreatment();
        }

        @Override
        public void preCheck()
        {
            this.init();
            DatabendWriterUtil.preCheckPrePareSQL(options);
            DatabendWriterUtil.preCheckPostSQL(options);
        }

        @Override
        public void prepare()
        {
            String username = options.getUsername();
            String password = options.getPassword();
            String jdbcUrl = options.getJdbcUrl();
            List<String> renderedPreSqls = DatabendWriterUtil.renderPreOrPostSqls(options.getPreSqlList(), options.getTable());
            if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
                Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);
                LOG.info("Begin to execute preSqls:[{}]. context info:{}.", String.join(";", renderedPreSqls), jdbcUrl);
                DatabendWriterUtil.executeSqls(conn, renderedPreSqls);
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
            List<String> renderedPostSqls = DatabendWriterUtil.renderPreOrPostSqls(options.getPostSqlList(), options.getTable());
            if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {
                Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, username, password);
                LOG.info("Begin to execute preSqls:[{}]. context info:{}.", String.join(";", renderedPostSqls), jdbcUrl);
                DatabendWriterUtil.executeSqls(conn, renderedPostSqls);
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
        private DatabendWriterManager writerManager;
        private DatabendWriterOptions options;
        private DatabendISerializer rowSerializer;

        @Override
        public void init()
        {
            options = new DatabendWriterOptions(super.getPluginJobConf());
            if (options.isWildcardColumn()) {
                options.setInfoCchemaColumns(Collections.singletonList("*"));
            }
            writerManager = new DatabendWriterManager(options);
            rowSerializer = DatabendSerializerFactory.createSerializer(options);
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
                    if (!options.isWildcardColumn() && record.getColumnNumber() != options.getColumns().size()) {
                        throw AddaxException
                                .asAddaxException(
                                        DBUtilErrorCode.CONF_ERROR,
                                        String.format(
                                                "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                                record.getColumnNumber(),
                                                options.getColumns().size()));
                    }
                    writerManager.writeRecord(rowSerializer.serialize(record));
                }
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
            }
        }

        @Override
        public void post()
        {
            try {
                writerManager.close();
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
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
