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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

/**
 * Utility class for database operations, including connection management, SQL validation,
 * privilege checks, and result set handling.
 */
public final class DBUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(DBUtil.class);
    private static final int DEFAULT_SOCKET_TIMEOUT_SEC = 20_000;

    private static final Map<String, BasicDataSource> DS_CACHE = new java.util.concurrent.ConcurrentHashMap<>();
    private static volatile boolean shutdownHookRegistered = false;

    private DBUtil()
    {
        // Private constructor to prevent instantiation
    }

    /**
     * Validates JDBC URL connectivity with retry mechanism.
     *
     * @param dataBaseType The database type
     * @param jdbcUrl The JDBC URL to validate
     * @param username Database username
     * @param password Database password 
     * @param preSql List of pre-SQL statements to execute during validation
     * @throws AddaxException if connection validation fails after retries
     */
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

    /**
     * Validates JDBC URL connectivity without retry mechanism.
     *
     * @param dataBaseType The database type
     * @param jdbcUrl The JDBC URL to validate
     * @param username Database username
     * @param password Database password
     * @param preSql List of pre-SQL statements to execute during validation
     * @throws AddaxException if connection validation fails
     */
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

    /**
     * Checks if the specified user has INSERT privileges on the given tables.
     *
     * @param dataBaseType The database type
     * @param jdbcURL The JDBC URL for connection
     * @param userName Database username
     * @param password Database password
     * @param tableList List of table names to check privileges for
     * @return true if user has INSERT privilege on all tables, false otherwise
     */
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

    /**
     * Checks if the specified user has DELETE privileges on the given tables.
     *
     * @param dataBaseType The database type
     * @param jdbcURL The JDBC URL for connection
     * @param userName Database username
     * @param password Database password
     * @param tableList List of table names to check privileges for
     * @return true if user has DELETE privilege on all tables, false otherwise
     */
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

    /**
     * Determines if DELETE privilege check is needed by examining pre/post SQL statements.
     *
     * @param originalConfig The configuration containing pre/post SQL statements
     * @return true if any SQL statement contains DELETE operations, false otherwise
     */
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
     * Get direct JDBC connection with default socket timeout.
     * <p>
     * If connecting failed, try to connect for MAX_TRY_TIMES times
     * </p>
     *
     * @param dataBaseType Database type
     * @param jdbcUrl JDBC URL for connection
     * @param username Username for login
     * @param password Password to use when connecting to server
     * @return Connection instance
     * @throws RdbmsException if connection fails
     */
    public static synchronized Connection getConnection(DataBaseType dataBaseType, String jdbcUrl, String username, String password)
    {
        return getConnection(dataBaseType, jdbcUrl, username, password, DEFAULT_SOCKET_TIMEOUT_SEC);
    }

    /**
     * Get direct JDBC connection with specified socket timeout.
     *
     * @param dataBaseType Database type
     * @param jdbcUrl JDBC URL for connection
     * @param username Username for login
     * @param password Password to use when connecting to server
     * @param socketTimeout Socket timeout in seconds
     * @return Connection instance
     * @throws RdbmsException if connection fails
     */
    public static synchronized Connection getConnection(DataBaseType dataBaseType, String jdbcUrl, String username, String password, int socketTimeout)
    {
        try {
            if (jdbcUrl == null) {
                throw new IllegalArgumentException("jdbcUrl must not be null");
            }
            // Use shared cached datasource
            BasicDataSource ds = getOrCreateDataSource(dataBaseType, jdbcUrl, username, password, socketTimeout);
            try {
                DriverManager.setLoginTimeout(socketTimeout);
            } catch (Exception ignore) {}
            return ds.getConnection();
        }
        catch (Exception e) {
            throw RdbmsException.asConnException(e, jdbcUrl);
        }
    }

    /**
     * Get direct JDBC connection without retry mechanism.
     * <p>
     * If connecting failed, fail immediately without retries
     * </p>
     *
     * @param dataBaseType The database type
     * @param jdbcUrl JDBC URL for connection
     * @param username Username for login
     * @param password Password to use when connecting to server
     * @return Connection instance
     * @throws RdbmsException if connection fails
     */
    public static Connection getConnectionWithoutRetry(DataBaseType dataBaseType, String jdbcUrl, String username, String password)
    {
        return getConnectionWithoutRetry(dataBaseType, jdbcUrl, username, password, DEFAULT_SOCKET_TIMEOUT_SEC);
    }

    /**
     * Get connection without retry mechanism with specified socket timeout.
     *
     * @param dataBaseType The database type
     * @param jdbcUrl JDBC URL for connection
     * @param username Username for login
     * @param password Password to use when connecting to server
     * @param socketTimeout Socket timeout in seconds
     * @return Connection instance
     * @throws RdbmsException if connection fails
     */
    public static Connection getConnectionWithoutRetry(DataBaseType dataBaseType, String jdbcUrl, String username, String password, int socketTimeout)
    {
        return DBUtil.getConnection(dataBaseType, jdbcUrl, username, password, socketTimeout);
    }

    /**
     * A wrapper method to execute SELECT-like SQL statements with default query timeout.
     *
     * @param conn Database connection
     * @param sql SQL statement to be executed
     * @param fetchSize Fetch size for each batch
     * @return A {@link ResultSet} containing query results
     * @throws SQLException if SQL execution fails
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize)
            throws SQLException
    {
        // Default 3600s query Timeout
        return query(conn, sql, fetchSize, DEFAULT_SOCKET_TIMEOUT_SEC);
    }

    /**
     * A wrapper method to execute SELECT-like SQL statements with configurable query timeout.
     *
     * @param conn Database connection
     * @param sql SQL statement to be executed
     * @param fetchSize Fetch size for each batch
     * @param queryTimeout Query timeout in seconds
     * @return A {@link ResultSet} containing query results
     * @throws SQLException if SQL execution fails
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize, int queryTimeout)
            throws SQLException
    {
        // Make sure autocommit is off
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
            // Some databases do not support TYPE_FORWARD_ONLY/CONCUR_READ_ONLY
            LOG.warn("The current database does not support TYPE_FORWARD_ONLY/CONCUR_READ_ONLY");
            stmt = conn.createStatement(); //NOSONAR
        }
        stmt.setFetchSize(fetchSize);
        stmt.setQueryTimeout(queryTimeout);
        return stmt.executeQuery(sql);
    }

    /**
     * Safely close database resources (ResultSet, Statement, Connection).
     * Handles null values and suppresses SQLExceptions during cleanup.
     *
     * @param rs ResultSet to close (can be null)
     * @param stmt Statement to close (can be null)
     * @param conn Connection to close (can be null)
     */
    public static void closeDBResources(ResultSet rs, Statement stmt, Connection conn)
    {
        Statement stmtToClose = stmt;
        if (rs != null) {
            try {
                // Try to capture the statement from the ResultSet if no explicit statement is provided
                if (stmtToClose == null) {
                    try {
                        stmtToClose = rs.getStatement();
                    }
                    catch (SQLException ignored) {
                        // ignore, we may not be able to get the statement
                    }
                }
                rs.close();
            }
            catch (SQLException ignored) {
                // Exception ignored during cleanup
            }
        }

        if (stmtToClose != null) {
            try {
                stmtToClose.close();
            }
            catch (SQLException ignored) {
                // Exception ignored during cleanup
            }
        }

        if (conn != null) {
            try {
                conn.close();
            }
            catch (SQLException ignored) {
                // Exception ignored during cleanup
            }
        }
    }

    /**
     * Convenience method to close Statement and Connection resources.
     *
     * @param stmt Statement to close (can be null)
     * @param conn Connection to close (can be null)
     */
    public static void closeDBResources(Statement stmt, Connection conn)
    {
        closeDBResources(null, stmt, conn);
    }

    /**
     * Retrieves all column names for a specified table.
     *
     * @param dataBaseType Database type
     * @param jdbcUrl JDBC URL for connection
     * @param user Database username
     * @param pass Database password
     * @param tableName Name of the table to query
     * @return List of column names in the table
     * @throws AddaxException if connection or query fails
     */
    public static List<String> getTableColumns(DataBaseType dataBaseType, String jdbcUrl, String user, String pass, String tableName)
    {
        try (Connection conn = getConnection(dataBaseType, jdbcUrl, user, pass)) {
            return getTableColumnsByConn(conn, tableName, dataBaseType);
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR, "Failed to get table columns", e);
        }
    }

    /**
     * Retrieves column names for a table using an existing connection.
     *
     * @param conn Database connection
     * @param tableName Name of the table to query
     * @return List of column names in the table
     */
    public static List<String> getTableColumnsByConn(Connection conn, String tableName, DataBaseType dataBaseType)
    {
        List<Map<String, Object>> rsMetaData = getColumnMetaData(conn, tableName, "*");

        // Using Stream API to transform metadata to column names
        return rsMetaData.stream()
                .skip(1) // Skip the first null element
                .map(map ->  dataBaseType.quoteColumnName(map.get("name").toString(), true))
                .toList();
    }

    /**
     * Retrieves detailed column metadata for a table.
     *
     * @param conn Database connection
     * @param tableName The table name
     * @param column The column specification (use "*" for all columns)
     * @return List of maps containing column metadata (name, type, label, typeName, precision, scale)
     * @throws AddaxException if metadata retrieval fails
     */
    public static List<Map<String, Object>> getColumnMetaData(Connection conn, String tableName, String column)
    {
        List<Map<String, Object>> result = new ArrayList<>();
        // Skip index 0, compliant with JDBC ResultSet and ResultSetMetaData
        result.add(null);

        try (var statement = conn.createStatement()) {
            // Build query SQL based on database type
            String queryColumnSql;
            if (DataBaseType.TDengine.getDriverClassName().equals(conn.getMetaData().getDriverName())) {
                // TDengine does not support "1=2" clause
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

    /**
     * Tests database connection and executes pre-SQL statements without retry mechanism.
     *
     * @param dataBaseType Database type
     * @param url JDBC URL
     * @param user Database username
     * @param pass Database password
     * @param preSql List of pre-SQL statements to execute
     * @throws AddaxException if connection test fails
     */
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

    /**
     * Executes a SQL query and returns the ResultSet.
     *
     * @param conn Database connection
     * @param sql SQL statement to execute
     * @return ResultSet if query returns results, null otherwise
     * @throws SQLException if SQL execution fails
     */
    public static ResultSet query(Connection conn, String sql)
            throws SQLException
    {
        // Create the statement and return its ResultSet without closing the statement here.
        // Callers are expected to close the ResultSet and the Statement (or use closeDBResources)
        // to avoid closing the Statement before the ResultSet is consumed.
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY); //NOSONAR
        stmt.setQueryTimeout(DEFAULT_SOCKET_TIMEOUT_SEC);
        boolean hasResultSet = stmt.execute(sql);
        if (hasResultSet) {
            return stmt.getResultSet();
        }
        else {
            // no ResultSet produced, close the statement immediately
            try {
                stmt.close();
            }
            catch (SQLException ignored) {
                // ignore
            }
            return null;
        }
    }

    /**
     * Executes a pre-check SQL statement and validates the result.
     *
     * @param conn Database connection
     * @param pre Pre-check SQL statement
     * @return true if pre-check passes (result is 0), false otherwise
     */
    private static boolean doPreCheck(Connection conn, String pre)
    {
        ResultSet rs = null;
        try {
            rs = query(conn, pre);
            if (rs == null) {
                LOG.warn("Pre-check SQL [{}] returned no ResultSet.", pre);
                return false;
            }
            int checkResult = -1;
            if (DBUtil.asyncResultSetNext(rs)) {
                Object obj = rs.getObject(1);
                if (obj instanceof Number) {
                    checkResult = ((Number) obj).intValue();
                }
                else if (obj != null) {
                    try {
                        checkResult = Integer.parseInt(obj.toString());
                    }
                    catch (NumberFormatException ignored) {
                        // leave checkResult as -1
                    }
                }
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
        finally {
            DBUtil.closeDBResources(rs, null, null);
        }
    }

    /**
     * Handles session configuration for different database types.
     * Currently, supports Oracle, MySQL, and SQLServer session configurations.
     *
     * @param conn Database connection
     * @param config Configuration containing session settings
     * @param databaseType Database type
     * @param message Context message for logging
     */
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

    /**
     * Executes a list of session configuration SQL statements.
     *
     * @param conn Database connection
     * @param sessions List of session configuration SQL statements
     * @param message Context message for logging
     */
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

    /**
     * Validates SQL syntax using Druid SQL parser.
     *
     * @param sql SQL statement to validate
     * @param dataBaseType Database type for parser selection
     */
    public static void sqlValid(String sql, DataBaseType dataBaseType)
    {
        SQLStatementParser statementParser = SQLParserUtils.createSQLStatementParser(sql, dataBaseType.getTypeName());
        statementParser.parseStatementList();
    }

    /**
     * Asynchronously advances ResultSet to next row with default timeout.
     * This method is designed for metadata queries, not for data reading.
     *
     * @param resultSet ResultSet to advance
     * @return true if there is a next row, false otherwise
     */
    public static boolean asyncResultSetNext(ResultSet resultSet)
    {
        try {
            return resultSet.next();
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, "Failed to advance ResultSet.", e);
        }
    }

    /**
     * Loads database driver classes from plugin configuration.
     *
     * @param pluginType Type of plugin ("reader" or "writer")
     * @param pluginName Name of the plugin
     */
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

            // Load drivers sequentially to avoid potential class loading conflicts
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

    private static String dsKey(String driver, String url, String user) {
        return driver + "|" + url + "|" + (user == null ? "" : user);
    }

    private static BasicDataSource getOrCreateDataSource(DataBaseType dataBaseType, String jdbcUrl, String username, String password, int socketTimeout) {
        String driverClassName;
        String effectiveUrl = jdbcUrl;
        if (effectiveUrl != null && effectiveUrl.contains("inceptor2")) {
            LOG.warn("Detected 'inceptor2' in jdbcUrl; replacing with 'hive2' and using HiveDriver.");
            effectiveUrl = effectiveUrl.replace("inceptor2", "hive2");
            driverClassName = "org.apache.hive.jdbc.HiveDriver";
        } else if (effectiveUrl != null && effectiveUrl.startsWith("jdbc:hive2")) {
            driverClassName = "org.apache.hive.jdbc.HiveDriver";
        } else {
            driverClassName = dataBaseType.getDriverClassName();
        }
        String key = dsKey(driverClassName, effectiveUrl, username);
        BasicDataSource ds = DS_CACHE.get(key);
        if (ds != null) {
            return ds;
        }
        ds = new BasicDataSource();
        ds.setDriverClassName(driverClassName);
        ds.setUrl(effectiveUrl);
        ds.setUsername(username);
        ds.setPassword(password);
        // Pool config (tuned conservatively)
        ds.setMinIdle(2);
        ds.setMaxTotal(8);
        ds.setMaxOpenPreparedStatements(200);
        ds.setValidationQueryTimeout(Duration.ofSeconds(socketTimeout));
        // Vendor-specific properties
        if (dataBaseType == DataBaseType.Oracle) {
            ds.addConnectionProperty("oracle.jdbc.ReadTimeout", String.valueOf(socketTimeout * 1000L));
        }
        // Register shutdown hook once
        if (!shutdownHookRegistered) {
            synchronized (DBUtil.class) {
                if (!shutdownHookRegistered) {
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        DS_CACHE.values().forEach(d -> {
                            try { d.close(); } catch (Exception ignore) {}
                        });
                    }, "addax-ds-shutdown"));
                    shutdownHookRegistered = true;
                }
            }
        }
        BasicDataSource prev = DS_CACHE.putIfAbsent(key, ds);
        return prev != null ? prev : ds;
    }
}
