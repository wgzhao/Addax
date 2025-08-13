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

public enum DataBaseType
{
    MySql("mysql", "com.mysql.cj.jdbc.Driver"),
    Hive("hive2", "org.apache.hive.jdbc.HiveDriver"),
    Oracle("oracle", "oracle.jdbc.OracleDriver"),
    Presto("presto", "io.prestosql.jdbc.PrestoDriver"),
    ClickHouse("clickhouse", "com.clickhouse.jdbc.ClickHouseDriver"),
    SQLite("sqlite", "org.sqlite.JDBC"),
    SQLServer("sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
    PostgreSQL("postgresql", "org.postgresql.Driver"),
    RDBMS("rdbms", DataBaseType.class.getName()),
    RDBMS_READER("rdbms_reader", DataBaseType.class.getName()),
    RDBMS_WRITER("rdbms_writer", DataBaseType.class.getName()),
    DB2("db2", "com.ibm.db2.jcc.DB2Driver"),
    Inceptor2("inceptor2", "org.apache.hive.jdbc.HiveDriver"),
    InfluxDB("influxdb", "org.influxdb.influxdb-java"),
    Impala("impala", "com.cloudera.impala.jdbc41.Driver"),
    TDengine("tdengine", "com.taosdata.jdbc.TSDBDriver"),
    Trino("trino", "io.trino.jdbc.TrinoDriver"),
    Sybase("sybase", "com.sybase.jdbc4.jdbc.SybDriver"),
    Databend("databend", "com.databend.jdbc.DatabendDriver"),
    Access("access","net.ucanaccess.jdbc.UcanaccessDriver"),
    HANA("hana", "com.sap.db.jdbc.Driver");

    private static final Pattern jdbcUrlPattern = Pattern.compile("jdbc:\\w+:(?:thin:url=|//|thin:@|)([\\w\\d.,]+).*");

    private String driverClassName;
    private final String typeName;

    DataBaseType(String typeName, String driverClassName)
    {
        this.typeName = typeName;
        this.driverClassName = driverClassName;
    }

    /**
     * extract IP/host from jdbc url, return null if not recognize
     * <code>
     * jdbc:phoenix:thin:url=dn01,dn02:6667
     * jdbc:phoenix:dn01,dn02,dn03:2181:/hbase-secure:trino@DOMAIN.COM:/etc/trino/trino.headless.keytab
     * jdbc:mysql://127.0.0.1:3306
     * jdbc:sqlserver://192.168.1.1:1433; DatabaseName=teammate_pro
     * jdbc:mysql://127.0.0.1
     * jdbc:oracle:thin:@192.168.1.11:1521/hgdb
     * </code>
     *
     * @param jdbcUrl java jdbc url
     * @return ip address
     */
    public static String parseIpFromJdbcUrl(String jdbcUrl)
    {
        Matcher matcher = jdbcUrlPattern.matcher(jdbcUrl);
        if (matcher.matches()) {
            return matcher.group(1);
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
            String suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&rewriteBatchedStatements=true";
            // take timezone
            if (!jdbc.contains("serverTimezone")) {
                int offsetHours = TimeZone.getDefault().getRawOffset() / 3600000;
                suffix += "&serverTimezone=GMT" + (offsetHours < 0 ? "-" : "%2B") + offsetHours;
            }
            if (!"com.mysql.jdbc.Driver".equals(this.driverClassName)) {
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

    public String quoteColumnName(String columnName)
    {
        String quoteChar = "'`\"";
        // if the column is not reserved words , it's constant value
        if (! SQL_RESERVED_WORDS.contains(columnName.toUpperCase())) {
            return columnName;
        }
        // if the column has quote char, return directly
        if (quoteChar.contains(columnName.charAt(0) + "")) {
            return columnName;
        }
        if (columnName.equals("*")) {
            return columnName;
        }
        // if the column consists of only numbers, return directly
        if (columnName.matches("\\d+")) {
            return columnName;
        }
        // if the column is null string, means use null as column value
        if (columnName.equals("null")) {
            return columnName;
        }
        if (this == MySql || this == Hive) {
            return "`" + columnName.replace("`", "``") + "`";
        }
        if (this == Presto || this == Trino || this == Oracle) {
            return columnName.startsWith("\"") ? columnName: "\"" + columnName + "\"";
        }
        if (this == SQLServer) {
            return columnName.startsWith("[") ? columnName: "[" + columnName + "]";
        }
        return columnName;
    }

    public String getTypeName()
    {
        return typeName;
    }

    public void setDriverClassName(String driverClassName)
    {
        this.driverClassName = driverClassName;
    }
}
