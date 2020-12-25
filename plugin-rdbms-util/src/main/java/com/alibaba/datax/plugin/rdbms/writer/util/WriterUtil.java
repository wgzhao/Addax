package com.alibaba.datax.plugin.rdbms.writer.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.druid.sql.parser.ParserException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class WriterUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(WriterUtil.class);

    private WriterUtil() {}

    public static List<Configuration> doSplit(Configuration simplifiedConf,
            int adviceNumber)
    {

        List<Configuration> splitResultConfigs = new ArrayList<>();

        int tableNumber = simplifiedConf.getInt(Constant.TABLE_NUMBER_MARK);

        //处理单表的情况
        if (tableNumber == 1) {
            //由于在之前的  master prepare 中已经把 table,jdbcUrl 提取出来，所以这里处理十分简单
            for (int j = 0; j < adviceNumber; j++) {
                splitResultConfigs.add(simplifiedConf.clone());
            }

            return splitResultConfigs;
        }

        if (tableNumber != adviceNumber) {
            throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                    String.format("您的配置文件中的列配置信息有误. 您要写入的目的端的表个数是:%s , 但是根据系统建议需要切分的份数是：%s. 请检查您的配置并作出修改.",
                            tableNumber, adviceNumber));
        }

        String jdbcUrl;
        List<String> preSqls = simplifiedConf.getList(Key.PRE_SQL, String.class);
        List<String> postSqls = simplifiedConf.getList(Key.POST_SQL, String.class);

        List<Object> conns = simplifiedConf.getList(Constant.CONN_MARK,
                Object.class);

        for (Object conn : conns) {
            Configuration sliceConfig = simplifiedConf.clone();

            Configuration connConf = Configuration.from(conn.toString());
            jdbcUrl = connConf.getString(Key.JDBC_URL);
            sliceConfig.set(Key.JDBC_URL, jdbcUrl);

            sliceConfig.remove(Constant.CONN_MARK);

            List<String> tables = connConf.getList(Key.TABLE, String.class);

            for (String table : tables) {
                Configuration tempSlice = sliceConfig.clone();
                tempSlice.set(Key.TABLE, table);
                tempSlice.set(Key.PRE_SQL, renderPreOrPostSqls(preSqls, table));
                tempSlice.set(Key.POST_SQL, renderPreOrPostSqls(postSqls, table));

                splitResultConfigs.add(tempSlice);
            }
        }

        return splitResultConfigs;
    }

    public static List<String> renderPreOrPostSqls(List<String> preOrPostSqls, String tableName)
    {
        if (null == preOrPostSqls) {
            return Collections.emptyList();
        }

        List<String> renderedSqls = new ArrayList<>();
        for (String sql : preOrPostSqls) {
            //preSql为空时，不加入执行队列
            if (StringUtils.isNotBlank(sql)) {
                renderedSqls.add(sql.replace(Constant.TABLE_NAME_PLACEHOLDER, tableName));
            }
        }

        return renderedSqls;
    }

    public static void executeSqls(Connection conn, List<String> sqls, String basicMessage, DataBaseType dataBaseType)
    {
        Statement stmt = null;
        String currentSql = null;
        try {
            stmt = conn.createStatement();
            for (String sql : sqls) {
                currentSql = sql;
                DBUtil.executeSqlWithoutResultSet(stmt, sql);
            }
        }
        catch (Exception e) {
            throw RdbmsException.asQueryException(dataBaseType, e, currentSql, null, null);
        }
        finally {
            DBUtil.closeDBResources(null, stmt, null);
        }
    }

    public static String getWriteTemplate(List<String> columnHolders, List<String> valueHolders,
            String writeMode, DataBaseType dataBaseType, boolean forceUseUpdate)
    {
        String mode = writeMode.trim().toLowerCase();
        boolean isWriteModeLegal = mode.startsWith("insert") || mode.startsWith("replace") || mode.startsWith("update");

        if (!isWriteModeLegal) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    String.format("您所配置的 writeMode:%s 错误. 因为DataX 目前仅支持replace,update 或 insert 方式. 请检查您的配置并作出修改.", writeMode));
        }
        String writeDataSqlTemplate;
        if (forceUseUpdate || mode.startsWith("update")) {
            if (dataBaseType == DataBaseType.MySql) {
                writeDataSqlTemplate = "INSERT INTO %s (" + StringUtils.join(columnHolders, ",") +
                        ") VALUES(" + StringUtils.join(valueHolders, ",") +
                        ")" +
                        onDuplicateKeyUpdateString(columnHolders);
            }
            else if (dataBaseType == DataBaseType.Oracle) {
                writeDataSqlTemplate = onMergeIntoDoString(writeMode, columnHolders, valueHolders) + "INSERT (" +
                        StringUtils.join(columnHolders, ",") +
                        ") VALUES(" + StringUtils.join(valueHolders, ",") +
                        ")";
            }
            else {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                        String.format("当前数据库不支持 writeMode:%s 模式.", writeMode));
            }
        }
        else {
            //这里是保护,如果其他错误的使用了update,需要更换为replace
            if (mode.startsWith("update")) {
                writeMode = "replace";
            }
            writeDataSqlTemplate = writeMode +
                    " INTO %s (" + StringUtils.join(columnHolders, ",") +
                    ") VALUES(" + StringUtils.join(valueHolders, ",") +
                    ")";
        }

        return writeDataSqlTemplate;
    }

    public static String onDuplicateKeyUpdateString(List<String> columnHolders)
    {
        if (columnHolders == null || columnHolders.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(" ON DUPLICATE KEY UPDATE ");
        boolean first = true;
        for (String column : columnHolders) {
            if (!first) {
                sb.append(",");
            }
            else {
                first = false;
            }
            sb.append(column);
            sb.append("=VALUES(");
            sb.append(column);
            sb.append(")");
        }

        return sb.toString();
    }

    public static String onMergeIntoDoString(String merge, List<String> columnHolders, List<String> valueHolders)
    {
        String[] sArray = getStrings(merge);
        StringBuilder sb = new StringBuilder();
        sb.append("MERGE INTO %s A USING ( SELECT ");

        boolean first = true;
        boolean first1 = true;
        StringBuilder str = new StringBuilder();
        StringBuilder update = new StringBuilder();
        for (String columnHolder : columnHolders) {
            if (Arrays.asList(sArray).contains(columnHolder)) {
                if (!first) {
                    sb.append(",");
                    str.append(" AND ");
                }
                else {
                    first = false;
                }
                str.append("TMP.").append(columnHolder);
                sb.append("?");
                str.append(" = ");
                sb.append(" AS ");
                str.append("A.").append(columnHolder);
                sb.append(columnHolder);
            }
        }

        for (String columnHolder : columnHolders) {
            if (!Arrays.asList(sArray).contains(columnHolder)) {
                if (!first1) {
                    update.append(",");
                }
                else {
                    first1 = false;
                }
                update.append(columnHolder);
                update.append(" = ");
                update.append("?");
            }
        }

        sb.append(" FROM DUAL ) TMP ON (");
        sb.append(str);
        sb.append(" ) WHEN MATCHED THEN UPDATE SET ");
        sb.append(update);
        sb.append(" WHEN NOT MATCHED THEN ");
        return sb.toString();
    }

    public static String[] getStrings(String merge)
    {
        merge = merge.replace("update", "");
        merge = merge.replace("(", "");
        merge = merge.replace(")", "");
        merge = merge.replace(" ", "");
        return merge.split(",");
    }

    public static void preCheckPrePareSQL(Configuration originalConfig, DataBaseType type)
    {
        List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
        Configuration connConf = Configuration.from(conns.get(0).toString());
        String table = connConf.getList(Key.TABLE, String.class).get(0);

        List<String> preSqls = originalConfig.getList(Key.PRE_SQL,
                String.class);
        List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(
                preSqls, table);

        if (!renderedPreSqls.isEmpty()) {
            LOG.info("Begin to preCheck preSqls:[{}].",
                    StringUtils.join(renderedPreSqls, ";"));
            for (String sql : renderedPreSqls) {
                try {
                    DBUtil.sqlValid(sql, type);
                }
                catch (ParserException e) {
                    throw RdbmsException.asPreSQLParserException(type, e, sql);
                }
            }
        }
    }

    public static void preCheckPostSQL(Configuration originalConfig, DataBaseType type)
    {
        List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
        Configuration connConf = Configuration.from(conns.get(0).toString());
        String table = connConf.getList(Key.TABLE, String.class).get(0);

        List<String> postSqls = originalConfig.getList(Key.POST_SQL,
                String.class);
        List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(
                postSqls, table);
        if (!renderedPostSqls.isEmpty()) {

            LOG.info("Begin to preCheck postSqls:[{}].",
                    StringUtils.join(renderedPostSqls, ";"));
            for (String sql : renderedPostSqls) {
                try {
                    DBUtil.sqlValid(sql, type);
                }
                catch (ParserException e) {
                    throw RdbmsException.asPostSQLParserException(type, e, sql);
                }
            }
        }
    }
}
