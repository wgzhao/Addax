package com.wgzhao.addax.plugin.writer.databendwriter.util;

import com.alibaba.druid.sql.parser.ParserException;
import com.google.common.base.Strings;
import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.plugin.writer.databendwriter.DatabendWriterOptions;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.util.RdbmsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public final class DatabendWriterUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(DatabendWriterUtil.class);

    private DatabendWriterUtil() {}

    public static List<String> renderPreOrPostSqls(List<String> preOrPostSqls, String tableName)
    {
        List<String> renderedSqls = new ArrayList<>();
        if (null == preOrPostSqls) {
            return renderedSqls;
        }
        for (String sql : preOrPostSqls) {
            if (!Strings.isNullOrEmpty(sql)) {
                renderedSqls.add(sql.replace(Constant.TABLE_NAME_PLACEHOLDER, tableName));
            }
        }
        return renderedSqls;
    }

    public static void executeSqls(Connection conn, List<String> sqls)
    {
        Statement stmt = null;
        String currentSql = null;
        try {
            stmt = conn.createStatement();
            for (String sql : sqls) {
                currentSql = sql;
                stmt.execute(sql);
            }
        }
        catch (Exception e) {
            throw RdbmsException.asQueryException(e, currentSql);
        }
        finally {
            DBUtil.closeDBResources(null, stmt, null);
        }
    }

    public static void preCheckPrePareSQL(DatabendWriterOptions options)
    {
        String table = options.getTable();
        List<String> preSqls = options.getPreSqlList();
        List<String> renderedPreSqls = DatabendWriterUtil.renderPreOrPostSqls(preSqls, table);
        if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
            LOG.info("Begin to preCheck preSqls:[{}].", String.join(";", renderedPreSqls));
            for (String sql : renderedPreSqls) {
                try {
                    DBUtil.sqlValid(sql, DataBaseType.MySql);
                }
                catch (ParserException e) {
                    throw RdbmsException.asPreSQLParserException(e, sql);
                }
            }
        }
    }

    public static void preCheckPostSQL(DatabendWriterOptions options)
    {
        String table = options.getTable();
        List<String> postSqls = options.getPostSqlList();
        List<String> renderedPostSqls = DatabendWriterUtil.renderPreOrPostSqls(postSqls, table);
        if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {
            LOG.info("Begin to preCheck postSqls:[{}].", String.join(";", renderedPostSqls));
            for (String sql : renderedPostSqls) {
                try {
                    DBUtil.sqlValid(sql, DataBaseType.MySql);
                }
                catch (ParserException e) {
                    throw RdbmsException.asPostSQLParserException(e, sql);
                }
            }
        }
    }
}
