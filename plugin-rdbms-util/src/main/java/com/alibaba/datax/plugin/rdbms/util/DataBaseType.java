package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.exception.DataXException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * refer:http://blog.csdn.net/ring0hx/article/details/6152528
 * <p/>
 */
public enum DataBaseType
{
    MySql("mysql", "com.mysql.jdbc.Driver"),
    Hive("hive2", "org.apache.hive.jdbc.HiveDriver"),
    Oracle("oracle", "oracle.jdbc.OracleDriver"),
    Presto("presto", "io.prestosql.jdbc.PrestoDriver"),
    ClickHouse("clickhouse", "ru.yandex.clickhouse.ClickHouseDriver"),
    SQLServer("sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
    PostgreSQL("postgresql", "org.postgresql.Driver"),
    RDBMS("rdbms", "com.alibaba.datax.plugin.rdbms.util.DataBaseType"),
    DB2("db2", "com.ibm.db2.jcc.DB2Driver"),
    Inceptor2("inceptor2", "org.apache.hive.jdbc.HiveDriver"),
    Impala("impala", "com.cloudera.impala.jdbc41.Driver");

    private static final Pattern mysqlPattern = Pattern.compile("jdbc:mysql://(.+):\\d+/.+");
    private static final Pattern oraclePattern = Pattern.compile("jdbc:oracle:thin:@(.+):\\d+:.+");
    private final String driverClassName;
    private String typeName;

    DataBaseType(String typeName, String driverClassName)
    {
        this.typeName = typeName;
        this.driverClassName = driverClassName;
    }

    /**
     * 注意：目前只实现了从 mysql/oracle 中识别出ip 信息.未识别到则返回 null.
     */
    public static String parseIpFromJdbcUrl(String jdbcUrl)
    {
        Matcher mysql = mysqlPattern.matcher(jdbcUrl);
        if (mysql.matches()) {
            return mysql.group(1);
        }
        Matcher oracle = oraclePattern.matcher(jdbcUrl);
        if (oracle.matches()) {
            return oracle.group(1);
        }
        return null;
    }

    public String getDriverClassName()
    {
        return driverClassName;
    }

    public String appendJDBCSuffixForReader(String jdbc)
    {
        String result = jdbc;
        String suffix;
        switch (this) {
            case MySql:
                suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&rewriteBatchedStatements=true";
                if (jdbc.contains("?")) {
                    result = jdbc + "&" + suffix;
                }
                else {
                    result = jdbc + "?" + suffix;
                }
                break;
            case Oracle:
            case SQLServer:
            case DB2:
            case Hive:
            case Impala:
            case Presto:
            case ClickHouse:
            case PostgreSQL:
            case RDBMS:
            case Inceptor2:
                break;
            default:
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type.");
        }

        return result;
    }

    public String appendJDBCSuffixForWriter(String jdbc)
    {
        String result = jdbc;
        String suffix;
        switch (this) {
            case MySql:
                suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&tinyInt1isBit=false";
                if (jdbc.contains("?")) {
                    result = jdbc + "&" + suffix;
                }
                else {
                    result = jdbc + "?" + suffix;
                }
                break;
            case Oracle:
            case Hive:
            case SQLServer:
            case ClickHouse:
            case Presto:
            case DB2:
            case PostgreSQL:
            case RDBMS:
            case Inceptor2:
                break;
            default:
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type.");
        }

        return result;
    }

    public String formatPk(String splitPk)
    {
        String result = splitPk;

        String result1 = splitPk.substring(1, splitPk.length() - 1).toLowerCase();
        switch (this) {
            case MySql:
            case Oracle:
                if (splitPk.startsWith("`") && splitPk.endsWith("`")) {
                    result = result1;
                }
                break;
            case SQLServer:
                if (splitPk.startsWith("[") && splitPk.endsWith("]")) {
                    result = result1;
                }
                break;
            case DB2:
            case PostgreSQL:
                break;
            default:
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type.");
        }

        return result;
    }

    public String quoteColumnName(String columnName)
    {
        String result = columnName;

        switch (this) {
            case MySql:
                result = "`" + columnName.replace("`", "``") + "`";
                break;
            case Oracle:
            case DB2:
            case PostgreSQL:
                break;
            case SQLServer:
                result = "[" + columnName + "]";
                break;
            default:
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type");
        }

        return result;
    }

    public String quoteTableName(String tableName)
    {
        String result = tableName;

        switch (this) {
            case MySql:
                result = "`" + tableName.replace("`", "``") + "`";
                break;
            case Oracle:
            case SQLServer:
            case DB2:
            case PostgreSQL:
                break;
            default:
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, "unsupported database type");
        }

        return result;
    }

    public String getTypeName()
    {
        return typeName;
    }

    public void setTypeName(String typeName)
    {
        this.typeName = typeName;
    }
}
