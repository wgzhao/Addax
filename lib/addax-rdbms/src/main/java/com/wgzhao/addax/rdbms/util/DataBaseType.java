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

/**
 * refer:http://blog.csdn.net/ring0hx/article/details/6152528
 */
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
    RDBMS("rdbms", "com.wgzhao.addax.rdbms.util.DataBaseType"),
    DB2("db2", "com.ibm.db2.jcc.DB2Driver"),
    Inceptor2("inceptor2", "org.apache.hive.jdbc.HiveDriver"),
    InfluxDB("influxdb", "org.influxdb.influxdb-java"),
    Impala("impala", "com.cloudera.impala.jdbc41.Driver"),
    //    TDengine("tdengine","com.taosdata.jdbc.rs.RestfulDriver"),
    TDengine("tdengine", "com.taosdata.jdbc.TSDBDriver"),
    Trino("trino", "io.trino.jdbc.TrinoDriver");

    private static final Pattern jdbcUrlPattern = Pattern.compile("jdbc:\\w+:(?:thin:url=|//|thin:@|)([\\w\\d.,]+).*");

    private final String driverClassName;
    private final String typeName;

    DataBaseType(String typeName, String driverClassName)
    {
        this.typeName = typeName;
        this.driverClassName = driverClassName;
    }

    /**
     * 注意：从JDBC连接串中识别出 ip/host 信息.未识别到则返回 null.
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

    public String quoteTableName(String tableName)
    {
        return quoteColumnName(tableName);
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
