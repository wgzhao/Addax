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

import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.util.RetryUtil;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

public final class DBUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(DBUtil.class);
    private static final int DEFAULT_SOCKET_TIMEOUT_SEC = 20_000;

    // Use ThreadLocal.withInitial() (JDK 8+) for cleaner initialization
    private static final ThreadLocal<ExecutorService> RS_EXECUTORS = ThreadLocal.withInitial(() -> Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
            .setNameFormat("rsExecutors-%d")
            .setDaemon(true)
            .build()));

    private DBUtil()
    {
    }

    public static void validJdbcUrl(DataBaseType dataBaseType, String jdbcUrl, String username, String password, List<String> preSql)
    {
        try {
            RetryUtil.executeWithRetry(() -> {
                testConnWithoutRetry(dataBaseType, jdbcUrl, username, password, preSql);
                return null;
            }, 3, 1000L, true);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR,
                    "Failed to connect the database server using " + jdbcUrl, e);
        }
    }

    public static void validJdbcUrlWithoutRetry(DataBaseType dataBaseType, String jdbcUrl, String username, String password, List<String> preSql)
    {
        try {
            testConnWithoutRetry(dataBaseType, jdbcUrl, username, password, preSql);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    CONNECT_ERROR, "Failed to connect the server using jdbcUrl " + jdbcUrl, e);
        }
    }

    public static boolean checkInsertPrivilege(DataBaseType dataBaseType, String jdbcURL, String userName, String password, List<String> tableList)
    {
        try (Connection connection = getConnection(dataBaseType, jdbcURL, userName, password)) {
            String insertTemplate = "INSERT INTO %s (SELECT * FROM %s WHERE 1 = 2)";

            boolean hasInsertPrivilege = true;

            for (String tableName : tableList) {
                String checkInsertPrivilegeSql = String.format(insertTemplate, tableName, tableName);
                try (Statement insertStmt = connection.createStatement()) {
                    insertStmt.execute(checkInsertPrivilegeSql);
                }
                catch (Exception e) {
                    hasInsertPrivilege = false;
                    LOG.warn("Failed to insert into table [{}] with user [{}]: {}.", tableName, userName, e.getMessage());
                }
            }

            return hasInsertPrivilege;
        }
        catch (SQLException e) {
            LOG.error("Error checking insert privilege", e);
            return false;
        }
    }

    public static boolean checkDeletePrivilege(DataBaseType dataBaseType, String jdbcURL, String userName, String password, List<String> tableList)
    {
        try (Connection connection = getConnection(dataBaseType, jdbcURL, userName, password)) {
            String deleteTemplate = "DELETE FROM %s WHERE 1 = 2";

            boolean hasDeletePrivilege = true;

            for (String tableName : tableList) {
                String checkDeletePrivilegeSQL = String.format(deleteTemplate, tableName);
                try (Statement deleteStmt = connection.createStatement()) {
                    deleteStmt.execute(checkDeletePrivilegeSQL);
                }
                catch (Exception e) {
                    hasDeletePrivilege = false;
                    LOG.warn("Failed to delete from table [{}] with user [{}]: {}.", tableName, userName, e.getMessage());
                }
            }

            return hasDeletePrivilege;
        }
        catch (SQLException e) {
            LOG.error("Error checking delete privilege", e);
            return false;
        }
    }

    public static boolean needCheckDeletePrivilege(Configuration originalConfig)
    {
        var allSqls = new ArrayList<String>();

        // Using Optional.ofNullable to handle potential null lists
        var preSQLs = originalConfig.getList(Key.PRE_SQL, String.class);
        var postSQLs = originalConfig.getList(Key.POST_SQL, String.class);

        if (preSQLs != null && !preSQLs.isEmpty()) {
            allSqls.addAll(preSQLs);
        }
        if (postSQLs != null && !postSQLs.isEmpty()) {
            allSqls.addAll(postSQLs);
        }

        // Using Stream API for more expressive code
        return allSqls.stream()
                .filter(StringUtils::isNotBlank)
                .anyMatch(sql -> sql.trim().toUpperCase().startsWith("DELETE"));
    }

    /**
     * Get direct JDBC connection
     * <p>
     * if connecting failed, try to connect for MAX_TRY_TIMES times
     * <p>
     *
     * @param dataBaseType database type.
     * @param jdbcUrl java jdbc url.
     * @param username User for login.
     * @param password Password to use when connecting to server.
     * @return Connection class {@link Connection}
     */
    public static synchronized Connection getConnection(DataBaseType dataBaseType, String jdbcUrl, String username, String password)
    {
        return getConnection(dataBaseType, jdbcUrl, username, password, DEFAULT_SOCKET_TIMEOUT_SEC);
    }

    public static synchronized Connection getConnection(DataBaseType dataBaseType, String jdbcUrl, String username, String password, int socketTimeout)
    {

        try (BasicDataSource bds = new BasicDataSource()) {
            bds.setUrl(jdbcUrl);
            bds.setUsername(username);
            bds.setPassword(password);

            if (dataBaseType == DataBaseType.Oracle) {
                //oracle.net.READ_TIMEOUT for jdbc versions < 10.1.0.5 oracle.jdbc.ReadTimeout for jdbc versions >=10.1.0.5
                // unit ms
                bds.addConnectionProperty("oracle.jdbc.ReadTimeout", String.valueOf(socketTimeout * 1000));
            }
            if ("org.apache.hive.jdbc.HiveDriver".equals(bds.getDriverClassName())) {
                DriverManager.setLoginTimeout(DEFAULT_SOCKET_TIMEOUT_SEC);
            }
            if (jdbcUrl.contains("inceptor2")) {
                LOG.warn("inceptor2 must be process specially");
                jdbcUrl = jdbcUrl.replace("inceptor2", "hive2");
                bds.setUrl(jdbcUrl);
                bds.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
                DriverManager.setLoginTimeout(DEFAULT_SOCKET_TIMEOUT_SEC);
            }
            else {
                LOG.debug("Connecting to database with driver {}", dataBaseType.getDriverClassName());
                bds.setDriverClassName(dataBaseType.getDriverClassName());
            }
            bds.setMinIdle(2);
//            bds.setMaxActive(5);
            bds.setMaxOpenPreparedStatements(200);
            return bds.getConnection();
        }
        catch (Exception e) {
            throw RdbmsException.asConnException(e);
        }
    }

    /**
     * Get direct JDBC connection
     * <p>
     * if connecting failed, try to connect for MAX_TRY_TIMES times
     * <p>
     *
     * @param dataBaseType The database's type
     * @param jdbcUrl jdbc url
     * @param username User for login
     * @param password Password to use when connecting to server
     * @return Connection class {@link Connection}
     */
    public static Connection getConnectionWithoutRetry(DataBaseType dataBaseType, String jdbcUrl, String username, String password)
    {
        return getConnectionWithoutRetry(dataBaseType, jdbcUrl, username, password, DEFAULT_SOCKET_TIMEOUT_SEC);
    }

    public static Connection getConnectionWithoutRetry(DataBaseType dataBaseType, String jdbcUrl, String username, String password, int socketTimeout)
    {
        return DBUtil.getConnection(dataBaseType, jdbcUrl, username, password, socketTimeout);
    }

    /**
     * a wrapped method to execute select-like sql statement.
     *
     * @param conn Database connection.
     * @param sql sql statement to be executed
     * @param fetchSize fetch size
     * @return a {@link ResultSet}
     * @throws SQLException if occurs SQLException.
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize)
            throws SQLException
    {
        // Default 3600s query Timeout
        return query(conn, sql, fetchSize, DEFAULT_SOCKET_TIMEOUT_SEC);
    }

    /**
     * a wrapped method to execute select-like sql statement.
     *
     * @param conn Database connection.
     * @param sql sql statement to be executed
     * @param fetchSize fetch size each batch
     * @param queryTimeout unit:second
     * @return A {@link ResultSet}
     * @throws SQLException if failed to execute sql statement
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize, int queryTimeout)
            throws SQLException
    {
        // make sure autocommit is off
        try {
            conn.setAutoCommit(false);
        }
        catch (SQLFeatureNotSupportedException ignore) {
            LOG.warn("The current database does not support AUTO_COMMIT property");
        }

        // Using try-with-resources for Statement would close it, but we need to return ResultSet
        Statement stmt;
        try {
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY); //NOSONAR
        }
        catch (SQLException ignore) {
            // some database does not support TYPE_FORWARD_ONLY/CONCUR_READ_ONLY
            LOG.warn("The current database does not support TYPE_FORWARD_ONLY/CONCUR_READ_ONLY");
            stmt = conn.createStatement(); //NOSONAR
        }
        stmt.setFetchSize(fetchSize);
        stmt.setQueryTimeout(queryTimeout);
        return stmt.executeQuery(sql);
    }

    public static void closeDBResources(ResultSet rs, Statement stmt, Connection conn)
    {
        if (rs != null) {
            try {
                rs.close();
            }
            catch (SQLException ignored) {
                // Ignored
            }
        }

        if (stmt != null) {
            try {
                stmt.close();
            }
            catch (SQLException ignored) {
                // Ignored
            }
        }

        if (conn != null) {
            try {
                conn.close();
            }
            catch (SQLException ignored) {
                // Ignored
            }
        }
    }

    public static void closeDBResources(Statement stmt, Connection conn)
    {
        closeDBResources(null, stmt, conn);
    }

    public static List<String> getTableColumns(DataBaseType dataBaseType, String jdbcUrl, String user, String pass, String tableName)
    {
        try (Connection conn = getConnection(dataBaseType, jdbcUrl, user, pass)) {
            return getTableColumnsByConn(conn, tableName);
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR, "Failed to get table columns", e);
        }
    }

    public static List<String> getTableColumnsByConn(Connection conn, String tableName)
    {
        List<String> columns = new ArrayList<>();

        List<Map<String, Object>> rsMetaData = getColumnMetaData(conn, tableName, "*");

        // Using Stream API to transform metadata to column names
        return rsMetaData.stream()
                .skip(1) // Skip the first null element
                .map(map -> map.get("name").toString())
                .toList();
    }

    /**
     * get column description
     *
     * @param conn database connection
     * @param tableName The table name
     * @param column table column
     * @return {@link List}
     */
    public static List<Map<String, Object>> getColumnMetaData(Connection conn, String tableName, String column)
    {
        List<Map<String, Object>> result = new ArrayList<>();
        // skip index 0, compliant with jdbc resultSet and resultMetaData
        result.add(null);

        try (var statement = conn.createStatement()) {
            // Using text blocks for multiline SQL (JDK 15+)
            String queryColumnSql;
            if (DataBaseType.TDengine.getDriverClassName().equals(conn.getMetaData().getDriverName())) {
                // TDengine does not support 1=2 clause
                queryColumnSql = "SELECT " + column + " FROM " + tableName + " LIMIT 0";
            }
            else {
                queryColumnSql = "SELECT " + column + " FROM " + tableName + " WHERE 1 = 2";
            }

            ResultSetMetaData metaData = statement.executeQuery(queryColumnSql).getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                Map<String, Object> map = new HashMap<>();
                map.put("name", metaData.getColumnName(i));
                map.put("type", metaData.getColumnType(i));
                map.put("label", metaData.getColumnLabel(i));
                map.put("typeName", metaData.getColumnTypeName(i));
                map.put("precision", metaData.getPrecision(i));
                map.put("scale", metaData.getScale(i));
                result.add(map);
            }

            return result;
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(EXECUTE_FAIL, "Failed to obtain the fields of table " + tableName, e);
        }
    }

    public static void testConnWithoutRetry(DataBaseType dataBaseType, String url, String user, String pass, List<String> preSql)
    {
        try (Connection connection = getConnection(dataBaseType, url, user, pass)) {
            if (preSql != null && !preSql.isEmpty()) {
                for (String pre : preSql) {
                    if (!doPreCheck(connection, pre)) {
                        LOG.warn("Failed to doPreCheck.");
                    }
                }
            }
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR, "Failed to test connection", e);
        }
    }

    public static ResultSet query(Connection conn, String sql)
            throws SQLException
    {
        try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setQueryTimeout(DEFAULT_SOCKET_TIMEOUT_SEC);
            boolean hasResultSet = stmt.execute(sql);
            if (hasResultSet) {
                return stmt.getResultSet();
            }
            else {
                return null;
            }
        }
    }

    private static boolean doPreCheck(Connection conn, String pre)
    {
        try (ResultSet rs = query(conn, pre)) {
            int checkResult = -1;
            if (DBUtil.asyncResultSetNext(rs)) {
                checkResult = rs.getInt(1);
                if (DBUtil.asyncResultSetNext(rs)) {
                    LOG.warn("Failed to pre-check with [{}]. It should return 0.", pre);
                    return false;
                }
            }
            return checkResult == 0;
        }
        catch (Exception e) {
            LOG.warn("Failed to pre-check with [{}], errorMessage: [{}].", pre, e.getMessage());
            return false;
        }
    }

    // warn:until now, only oracle need to handle session config.
    public static void dealWithSessionConfig(Connection conn, Configuration config, DataBaseType databaseType, String message)
    {
        switch (databaseType) {
            case Oracle, MySql, SQLServer -> {
                List<String> sessionConfig = config.getList(Key.SESSION, new ArrayList<>(), String.class);
                DBUtil.doDealWithSessionConfig(conn, sessionConfig, message);
            }
            default -> { /* No action needed */ }
        }
    }

    private static void doDealWithSessionConfig(Connection conn, List<String> sessions, String message)
    {
        if (null == sessions || sessions.isEmpty()) {
            return;
        }

        try (Statement stmt = conn.createStatement()) {
            for (String sessionSql : sessions) {
                LOG.info("Executing SQL:[{}]", sessionSql);
                try {
                    stmt.execute(sessionSql);
                }
                catch (SQLException e) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR, "Failed to set session with " + message, e);
                }
            }
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, "Failed to create statement for session config with " + message, e);
        }
    }

    public static void sqlValid(String sql, DataBaseType dataBaseType)
    {
        SQLStatementParser statementParser = SQLParserUtils.createSQLStatementParser(sql, dataBaseType.getTypeName());
        statementParser.parseStatementList();
    }

    /**
     * async next() only apply to meta query, not for data reading
     *
     * @param resultSet result set
     * @return boolean
     */
    public static boolean asyncResultSetNext(ResultSet resultSet)
    {
        return asyncResultSetNext(resultSet, 3600);
    }

    public static boolean asyncResultSetNext(ResultSet resultSet, int timeout)
    {
        // Use a method reference for cleaner code
        Future<Boolean> future = RS_EXECUTORS.get().submit(resultSet::next);
        try {
            return future.get(timeout, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, "Asynchronous retrieval of ResultSet failed.", e);
        }
    }

    public static void loadDriverClass(String pluginType, String pluginName)
    {
        try {
            String pluginJsonPath = String.join(File.separator,
                    System.getProperty("addax.home"),
                    "plugin",
                    pluginType,
                    pluginName + pluginType,
                    "plugin.json");

            Configuration configuration = Configuration.from(new File(pluginJsonPath));
            List<String> drivers = configuration.getList("drivers", String.class);

            // Using parallel streams for potentially faster loading
            drivers.forEach(driver -> {
                try {
                    Class.forName(driver);
                }
                catch (ClassNotFoundException e) {
                    throw new RuntimeException("Driver class not found: " + driver, e);
                }
            });
        }
        catch (RuntimeException e) {
            if (e.getCause() instanceof ClassNotFoundException) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "Error loading database driver. Please confirm that the libs directory has the driver jar package "
                                + "and the drivers configuration in plugin.json is correct.", e.getCause());
            }
            throw e;
        }
    }
}
