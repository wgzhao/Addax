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
    MySql("mysql", "com.mysql.cj.jdbc.Driver"),
    Hive("hive2", "org.apache.hive.jdbc.HiveDriver"),
    Oracle("oracle", "oracle.jdbc.OracleDriver"),
    Presto("presto", "io.prestosql.jdbc.PrestoDriver"),
    ClickHouse("clickhouse", "ru.yandex.clickhouse.ClickHouseDriver"),
    SQLServer("sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
    PostgreSQL("postgresql", "org.postgresql.Driver"),
    RDBMS("rdbms", "com.alibaba.datax.plugin.rdbms.util.DataBaseType"),
    DB2("db2", "com.ibm.db2.jcc.DB2Driver"),
    Inceptor2("inceptor2", "org.apache.hive.jdbc.HiveDriver"),
    InfluxDB("influxdb", "org.influxdb.influxdb-java"),
    Impala("impala", "com.cloudera.impala.jdbc41.Driver"),
    Trino("trino", "io.trino.jdbc.TrinoDriver");

    private static final Pattern mysqlPattern = Pattern.compile("jdbc:mysql://(.+):\\d+/.+");
    private static final Pattern oraclePattern = Pattern.compile("jdbc:oracle:thin:@(.+):\\d+:.+");
    private String driverClassName;
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
        if (this == MySql) {
            String suffix;
            if ("com.mysql.jdbc.Driver".equals(this.driverClassName)) {
                suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&rewriteBatchedStatements=true";
            } else {
                suffix = "yearIsDateType=false&zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1isBit=false&rewriteBatchedStatements=true";
            }
            if (jdbc.contains("?")) {
                return jdbc + "&" + suffix;
            }
            else {
                return jdbc + "?" + suffix;
            }
        }
        return jdbc;
    }

    public String appendJDBCSuffixForWriter(String jdbc)
    {
        if (this == MySql) {
            String suffix;
            if ("com.mysql.jdbc.Driver".equals(this.driverClassName)) {
                suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&tinyInt1isBit=false";
            } else {
                suffix = "yearIsDateType=false&zeroDateTimeBehavior=CONVERT_TO_NULL&rewriteBatchedStatements=true&tinyInt1isBit=false";
            }
            if (jdbc.contains("?")) {
                return jdbc + "&" + suffix;
            }
            else {
                return jdbc + "?" + suffix;
            }
        }
        return jdbc;
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
        if (this == MySql || this == Hive) {
            return "`" + columnName.replace("`", "``") + "`";
        }
        if (this == Presto || this == Trino || this == Oracle) {
            return "\"" + columnName + "\"";
        }
        if (this == SQLServer) {
            return "[" + columnName + "]";
        }
        return columnName;
    }

    public String quoteTableName(String tableName)
    {
        if (this == MySql || this == Hive) {
            return "`" + tableName.replace("`", "``") + "`";
        }
        if (this == Presto || this == Trino || this == Oracle) {
            return "\"" + tableName + "\"";
        }
        return tableName;
    }

    public String getTypeName()
    {
        return typeName;
    }

    public void setTypeName(String typeName)
    {
        this.typeName = typeName;
    }

    public String getDriveClassName()
    {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName)
    {
        this.driverClassName = driverClassName;
    }
}
