/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.wgzhao.addax.rdbms.util;

import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static com.wgzhao.addax.core.base.Constant.SQL_RESERVED_WORDS;

/**
 * Enumeration of supported database types with their corresponding JDBC drivers and type names.
 * Provides utility methods for JDBC URL manipulation and column name quoting.
 */
public enum DataBaseType
{
    /** MySQL database with modern Connector/J driver */
    MySql("mysql", "com.mysql.cj.jdbc.Driver"),

    /** Apache Doris via Arrow Flight SQL (MySQL-compatible SQL dialect) */
    Doris("mysql", "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"),

    /** Apache Hive data warehouse software using HiveServer2 */
    Hive("hive2", "org.apache.hive.jdbc.HiveDriver"),

    /** Oracle Database with native JDBC driver */
    Oracle("oracle", "oracle.jdbc.OracleDriver"),

    /** PrestoDB distributed SQL query engine */
    Presto("presto", "io.prestosql.jdbc.PrestoDriver"),

    /** ClickHouse columnar database management system */
    ClickHouse("clickhouse", "com.clickhouse.jdbc.ClickHouseDriver"),

    /** SQLite embedded relational database */
    SQLite("sqlite", "org.sqlite.JDBC"),

    /** Microsoft SQL Server database */
    SQLServer("sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),

    /** PostgreSQL open source relational database */
    PostgreSQL("postgresql", "org.postgresql.Driver"),

    /** Generic RDBMS type for common relational database operations */
    RDBMS("rdbms", DataBaseType.class.getName()),

    /** Generic RDBMS reader type for data reading operations */
    RDBMS_READER("rdbms_reader", DataBaseType.class.getName()),

    /** Generic RDBMS writer type for data writing operations */
    RDBMS_WRITER("rdbms_writer", DataBaseType.class.getName()),

    /** IBM DB2 database system */
    DB2("db2", "com.ibm.db2.jcc.DB2Driver"),

    /** Transwarp Inceptor2 Hadoop-based data platform */
    Inceptor2("inceptor2", "org.apache.hive.jdbc.HiveDriver"),

    /** InfluxDB time series database */
    InfluxDB("influxdb", "org.influxdb.influxdb-java"),

    /** Cloudera Impala massively parallel processing SQL query engine */
    Impala("impala", "com.cloudera.impala.jdbc41.Driver"),

    /** TDengine time-series database */
    TDengine("tdengine", "com.taosdata.jdbc.TSDBDriver"),

    /** Trino distributed SQL query engine (successor to PrestoDB) */
    Trino("trino", "io.trino.jdbc.TrinoDriver"),

    /** SAP Sybase database management system */
    Sybase("sybase", "com.sybase.jdbc4.jdbc.SybDriver"),

    /** Databend cloud-native data warehouse */
    Databend("databend", "com.databend.jdbc.DatabendDriver"),

    /** Microsoft Access database via UCanAccess driver */
    Access("access","net.ucanaccess.jdbc.UcanaccessDriver"),

    /** SAP HANA in-memory database platform */
    HANA("hana", "com.sap.db.jdbc.Driver");

    private static final Pattern jdbcUrlPattern = Pattern.compile("jdbc:[\\w-]+:(?:thin:url=|//|thin:@|)([\\w\\d.,]+).*");

    /** Digit-only column names are never quoted */
    private static final Pattern DIGIT_ONLY = Pattern.compile("\\d+");

    private String driverClassName;
    private final String typeName;

    /**
     * Constructor for DataBaseType enum.
     *
     * @param typeName The type name identifier
     * @param driverClassName The fully qualified JDBC driver class name
     */
    DataBaseType(String typeName, String driverClassName)
    {
        this.typeName = typeName;
        this.driverClassName = driverClassName;
    }

    /**
     * Extracts IP/host from JDBC URL, returns null if not recognized.
     * <p>
     * Examples of supported URL formats:
     * <ul>
     * <li>jdbc:phoenix:thin:url=dn01,dn02:6667</li>
     * <li>jdbc:phoenix:dn01,dn02,dn03:2181:/hbase-secure:trino@DOMAIN.COM:/etc/trino/trino.headless.keytab</li>
     * <li>jdbc:mysql://127.0.0.1:3306</li>
     * <li>jdbc:sqlserver://192.168.1.1:1433; DatabaseName=teammate_pro</li>
     * <li>jdbc:mysql://127.0.0.1</li>
     * <li>jdbc:oracle:thin:@192.168.1.11:1521/hgdb</li>
     * </ul>
     *
     * @param jdbcUrl JDBC URL to parse
     * @return IP address or host, null if pattern doesn't match
     */
    public static String parseIpFromJdbcUrl(String jdbcUrl)
    {
        Matcher matcher = jdbcUrlPattern.matcher(jdbcUrl);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return null;
    }

    /**
     * Gets the JDBC driver class name for this database type.
     *
     * @return The fully qualified driver class name
     */
    public String getDriverClassName()
    {
        return driverClassName;
    }

    /**
     * Appends MySQL-specific JDBC parameters for reader operations.
     * Includes timezone, SSL, and date handling configurations.
     *
     * @param jdbc Original JDBC URL
     * @return JDBC URL with appended reader-specific parameters
     */
    public String appendJDBCSuffixForReader(String jdbc)
    {
        if (this == MySql) {
            String suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&rewriteBatchedStatements=true";
            if (!"com.mysql.jdbc.Driver".equals(this.driverClassName) && !jdbc.contains("useSSL=")) {
                suffix += "&useSSL=false";
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

    /**
     * Appends MySQL-specific JDBC parameters for writer operations.
     * Optimizes for batch operations and data consistency.
     *
     * @param jdbc Original JDBC URL
     * @return JDBC URL with appended writer-specific parameters
     */
    public String appendJDBCSuffixForWriter(String jdbc)
    {
        if (this == MySql) {
            String suffix;
            if ("com.mysql.jdbc.Driver".equals(this.driverClassName) || "com.mysql.cj.jdbc.Driver".equals(this.driverClassName)) {
                suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&tinyInt1isBit=false";
            }
            else {
                suffix = "yearIsDateType=false&zeroDateTimeBehavior=CONVERT_TO_NULL&rewriteBatchedStatements=true&tinyInt1isBit=false&useSSL=false";
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

    /**
     * Quotes a column name only when needed (reserved word, digit-only, etc.).
     *
     * @param columnName the column name to quote; null returns null
     * @return the quoted or original column name
     */
    public String quoteColumnName(String columnName) {
        return quoteColumnName(columnName, false);
    }

    /**
     * Quotes column names according to database-specific conventions.
     * When forceQuote is false, only reserved words, digit-only names and
     * already-quoted names are touched, so ordinary identifiers stay as-is.
     *
     * @param columnName the column name to quote; null returns null
     * @param forceQuote quote unconditionally
     * @return the properly quoted column name, or the original if no quoting is needed
     */
    public String quoteColumnName(String columnName, boolean forceQuote)
    {
        if (columnName == null || columnName.length() < 2) {
            return columnName;
        }
        if (forceQuote) {
            String quoted = quoteColumn(columnName);
            return quoted != null ? quoted : columnName;
        }
        // If the column already has quote characters or is a wildcard, return directly
        String quoteChar = "'`\"";
        if (quoteChar.contains(String.valueOf(columnName.charAt(0))) || "*".equals(columnName)) {
            return columnName;
        }
        // If the column is null string, means use null as column value
        if ("null".equalsIgnoreCase(columnName)) {
            return columnName;
        }
        // If the column consists of only numbers, return directly
        if (DIGIT_ONLY.matcher(columnName).matches()) {
            return columnName;
        }
        // If the column is not a reserved word, no quoting is needed
        if (!SQL_RESERVED_WORDS.contains(columnName.toUpperCase())) {
            return columnName;
        }
        String columnName1 = quoteColumn(columnName);
        return columnName1 != null ? columnName1 : columnName;
    }

    private String quoteColumn(String columnName)
    {
        if (this == MySql || this == Doris || this == Hive) {
            return "`" + columnName.replace("`", "``") + "`";
        }
        if (this == Presto || this == Trino || this == Oracle || this == PostgreSQL) {
            return columnName.startsWith("\"") ? columnName : "\"" + columnName + "\"";
        }
        if (this == SQLServer) {
            return columnName.startsWith("[") ? columnName : "[" + columnName + "]";
        }
        return null;
    }

    /**
     * Removes the surrounding quotes of a quoted identifier, if any.
     *
     * @param field the identifier to unquote
     * @return the identifier without its surrounding quote characters
     */
    public String unQuote(String field) {
        if (field == null || field.length() < 2) {
            return field;
        }
        char firstChar = field.charAt(0);
        char lastChar = field.charAt(field.length() - 1);
        if ((firstChar == '`' && lastChar == '`')
                || (firstChar == '"' && lastChar == '"')
                || (firstChar == '[' && lastChar == ']')) {
            return field.substring(1, field.length() - 1);
        }
        return field;
    }

    /**
     * Gets the type name identifier for this database type.
     *
     * @return The type name string
     */
    public String getTypeName()
    {
        return typeName;
    }

    /**
     * Sets the JDBC driver class name for this database type.
     * Used for runtime driver configuration overrides.
     *
     * @param driverClassName The new driver class name
     */
    public void setDriverClassName(String driverClassName)
    {
        this.driverClassName = driverClassName;
    }
}
