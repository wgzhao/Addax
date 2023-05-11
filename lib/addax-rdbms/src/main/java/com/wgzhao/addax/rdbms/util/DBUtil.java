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
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.common.util.RetryUtil;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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

public final class DBUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(DBUtil.class);
    private static final int DEFAULT_SOCKET_TIMEOUT_SEC = 20_000;

    private static final ThreadLocal<ExecutorService> rsExecutors = ThreadLocal.withInitial(() -> Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
            .setNameFormat("rsExecutors-%d")
            .setDaemon(true)
            .build()));

    private DBUtil()
    {
    }

    public static String chooseJdbcUrl(DataBaseType dataBaseType, List<String> jdbcUrls, String username, String password, List<String> preSql)
    {
        if (null == jdbcUrls || jdbcUrls.isEmpty()) {
            throw AddaxException.asAddaxException(DBUtilErrorCode.CONF_ERROR,
                    String.format("The configure item jdbcUrl [%s] cannot be empty.",
                            StringUtils.join(jdbcUrls, ",")));
        }
        try {
            return RetryUtil.executeWithRetry(() -> {
                boolean connOK;
                for (String url : jdbcUrls) {
                    url = url.trim();
                    if (StringUtils.isNotBlank(url)) {
                        if (null == preSql || preSql.isEmpty()) {
                            connOK = testConnWithoutRetry(dataBaseType, url, username, password);
                        }
                        else {
                            connOK = testConnWithoutRetry(dataBaseType, url, username, password, preSql);
                        }

                        if (connOK) {
                            return url;
                        }
                    }
                }
                throw new Exception("Unable to connect the database.");
            }, 3, 1000L, true);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(DBUtilErrorCode.CONN_DB_ERROR,
                    String.format("Failed to connect the database, no connectable jdbcUrl can be found in [%s].",
                            StringUtils.join(jdbcUrls, ",")), e);
        }
    }

    public static String chooseJdbcUrlWithoutRetry(DataBaseType dataBaseType, List<String> jdbcUrls, String username,
            String password, List<String> preSql)
    {
        if (null == jdbcUrls || jdbcUrls.isEmpty()) {
            throw AddaxException.asAddaxException(
                    DBUtilErrorCode.CONF_ERROR,
                    String.format("The configure item jdbcUrl [%s] cannot be empty.",
                            StringUtils.join(jdbcUrls, ",")));
        }

        boolean connOK;
        for (String url : jdbcUrls) {
            url = url.trim();
            if (StringUtils.isNotBlank(url)) {
                if (null != preSql && !preSql.isEmpty()) {
                    connOK = testConnWithoutRetry(dataBaseType, url, username, password, preSql);
                }
                else {
                    try {
                        connOK = testConnWithoutRetry(dataBaseType, url, username, password);
                    }
                    catch (Exception e) {
                        throw AddaxException.asAddaxException(
                                DBUtilErrorCode.CONN_DB_ERROR,
                                String.format("Failed to connect the database, no connectable jdbcUrl can be found in [%s].",
                                        StringUtils.join(jdbcUrls, ",")), e);
                    }
                }
                if (connOK) {
                    return url;
                }
            }
        }
        throw AddaxException.asAddaxException(
                DBUtilErrorCode.CONN_DB_ERROR,
                String.format("Failed to connect the database, no connectable jdbcUrl can be found in [%s].",
                        StringUtils.join(jdbcUrls, ",")));
    }

    public static boolean checkInsertPrivilege(DataBaseType dataBaseType, String jdbcURL, String userName, String password, List<String> tableList)
    {
        Connection connection = connect(dataBaseType, jdbcURL, userName, password);
        String insertTemplate = "INSERT INTO %s (SELECT * FROM %s WHERE 1 = 2)";

        boolean hasInsertPrivilege = true;
        Statement insertStmt = null;
        for (String tableName : tableList) {
            String checkInsertPrivilegeSql = String.format(insertTemplate, tableName, tableName);
            try {
                insertStmt = connection.createStatement();
                insertStmt.execute(checkInsertPrivilegeSql);
            }
            catch (Exception e) {
                hasInsertPrivilege = false;
                LOG.warn("Failed to insert into table [{}] with user [{}]: {}.", userName, tableName, e.getMessage());
            }
        }

        closeDBResources(insertStmt, connection);
        return hasInsertPrivilege;
    }

    public static boolean checkDeletePrivilege(DataBaseType dataBaseType, String jdbcURL, String userName, String password, List<String> tableList)
    {
        Connection connection = connect(dataBaseType, jdbcURL, userName, password);
        String deleteTemplate = "DELETE FROM %s WHERE 1 = 2";

        boolean hasInsertPrivilege = true;
        Statement deleteStmt = null;
        for (String tableName : tableList) {
            String checkDeletePrivilegeSQL = String.format(deleteTemplate, tableName);
            try {
                deleteStmt = connection.createStatement();
                deleteStmt.execute(checkDeletePrivilegeSQL);
            }
            catch (Exception e) {
                hasInsertPrivilege = false;
                LOG.warn("Failed to delete from table [{}] with user [{}]: {}.", userName, tableName, e.getMessage());
            }
        }

        closeDBResources(deleteStmt, connection);
        return hasInsertPrivilege;
    }

    public static boolean needCheckDeletePrivilege(Configuration originalConfig)
    {
        List<String> allSqls = new ArrayList<>();
        List<String> preSQLs = originalConfig.getList(Key.PRE_SQL, String.class);
        List<String> postSQLs = originalConfig.getList(Key.POST_SQL, String.class);
        if (preSQLs != null && !preSQLs.isEmpty()) {
            allSqls.addAll(preSQLs);
        }
        if (postSQLs != null && !postSQLs.isEmpty()) {
            allSqls.addAll(postSQLs);
        }
        for (String sql : allSqls) {
            if (StringUtils.isNotBlank(sql) && sql.trim().toUpperCase().startsWith("DELETE")) {
                return true;
            }
        }
        return false;
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
    public static Connection getConnection(DataBaseType dataBaseType, String jdbcUrl, String username, String password)
    {

        return getConnection(dataBaseType, jdbcUrl, username, password, DEFAULT_SOCKET_TIMEOUT_SEC);
    }

    public static Connection getConnection(DataBaseType dataBaseType, String jdbcUrl, String username, String password, int socketTimeout)
    {

        try {
            return RetryUtil.executeWithRetry(() -> DBUtil.connect(dataBaseType, jdbcUrl, username,
                    password, socketTimeout), 3, 1000L, true);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(DBUtilErrorCode.CONN_DB_ERROR,
                    String.format("Failed to connect database with %s.", jdbcUrl), e);
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
        return DBUtil.connect(dataBaseType, jdbcUrl, username, password, socketTimeout);
    }

    private static synchronized Connection connect(DataBaseType dataBaseType, String url, String user, String pass)
    {
        return connect(dataBaseType, url, user, pass, DEFAULT_SOCKET_TIMEOUT_SEC);
    }

    private static synchronized Connection connect(DataBaseType dataBaseType, String url, String user, String pass, int socketTimeout)
    {
        BasicDataSource bds = new BasicDataSource();
        bds.setUrl(url);
        bds.setUsername(user);
        bds.setPassword(pass);

        if (dataBaseType == DataBaseType.Oracle) {
            //oracle.net.READ_TIMEOUT for jdbc versions < 10.1.0.5 oracle.jdbc.ReadTimeout for jdbc versions >=10.1.0.5
            // unit ms
            bds.addConnectionProperty("oracle.jdbc.ReadTimeout", String.valueOf(socketTimeout * 1000));
        }
        if (url.contains("inceptor2")) {
            LOG.warn("inceptor2 must be process specially");
            url = url.replace("inceptor2", "hive2");
            bds.setUrl(url);
            bds.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
        }
        else {
            LOG.debug("Connecting to database with driver {}", dataBaseType.getDriverClassName());
            bds.setDriverClassName(dataBaseType.getDriverClassName());
        }
        try {
            bds.setMinIdle(2);
            bds.setMaxIdle(5);
            bds.setMaxOpenPreparedStatements(200);
            return bds.getConnection();
        }
        catch (Exception e) {
            throw RdbmsException.asConnException(e);
        }
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param conn Database connection .
     * @param sql sql statement to be executed
     * @param fetchSize fetch size
     * @return a {@link ResultSet}
     * @throws SQLException if occurs SQLException.
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize)
            throws SQLException
    {
        // 默认3600 s 的query Timeout
        return query(conn, sql, fetchSize, DEFAULT_SOCKET_TIMEOUT_SEC);
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param conn Database connection .
     * @param sql sql statement to be executed
     * @param fetchSize fetch size each batch
     * @param queryTimeout unit:second
     * @return A {@link ResultSet}
     * @throws SQLException if failed to execute sql statement
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize, int queryTimeout)
            throws SQLException
    {

        Statement stmt;
        try {
            // make sure autocommit is off
            conn.setAutoCommit(false);
        }
        catch (SQLFeatureNotSupportedException ignore) {
            LOG.warn("The current database does not support AUTO_COMMIT property");
        }
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
        if (null != rs) {
            try {
                rs.close();
            }
            catch (SQLException ignored) {
                //
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            }
            catch (SQLException ignored) {
                //
            }
        }

        if (null != conn) {
            try {
                conn.close();
            }
            catch (SQLException ignored) {
                //
            }
        }
    }

    public static void closeDBResources(Statement stmt, Connection conn)
    {
        closeDBResources(null, stmt, conn);
    }

    public static List<String> getTableColumns(DataBaseType dataBaseType, String jdbcUrl, String user, String pass, String tableName)
    {
        Connection conn = getConnection(dataBaseType, jdbcUrl, user, pass);
        return getTableColumnsByConn(conn, tableName);
    }

    public static List<String> getTableColumnsByConn(Connection conn, String tableName)
    {
        List<String> columns = new ArrayList<>();

        List<Map<String, Object>> rsMetaData = getColumnMetaData(conn, tableName, "*");
        for (int i = 1, len = rsMetaData.size(); i < len; i++) {
            columns.add(rsMetaData.get(i).get("name").toString());
        }
        return columns;
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
        try {
            Statement statement = conn.createStatement();
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
            statement.close();
            return result;
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(DBUtilErrorCode.GET_COLUMN_INFO_FAILED,
                    String.format("Failed to obtain the fields of table [%s].", tableName), e);
        }
    }

    public static boolean testConnWithoutRetry(DataBaseType dataBaseType, String url, String user, String pass)
    {
        try (Connection ignored = connect(dataBaseType, url, user, pass)) {
            return true;
        }
        catch (Exception e) {
            LOG.error("Failed to connection with [{}]: {}.", url, e.getMessage());
        }
        return false;
    }

    public static boolean testConnWithoutRetry(DataBaseType dataBaseType, String url, String user, String pass, List<String> preSql)
    {
        try (Connection connection = connect(dataBaseType, url, user, pass)) {
            for (String pre : preSql) {
                if (!doPreCheck(connection, pre)) {
                    LOG.warn("Failed to doPreCheck.");
                    return false;
                }
            }
            return true;
        }
        catch (Exception e) {
            LOG.warn("Failed to connection with [{}]: {}.", url, e.getMessage());
        }
        return false;
    }

    public static ResultSet query(Connection conn, String sql)
            throws SQLException
    {
        try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setQueryTimeout(DEFAULT_SOCKET_TIMEOUT_SEC);
            return stmt.executeQuery(sql);
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
            if (0 == checkResult) {
                return true;
            }
            LOG.warn("Failed to pre-check with [{}]. It should return 0.", pre);
        }
        catch (Exception e) {
            LOG.warn("Failed to pre-check with [{}], errorMessage: [{}].", pre, e.getMessage());
        }
        return false;
    }

    // warn:until now, only oracle need to handle session config.
    public static void dealWithSessionConfig(Connection conn, Configuration config, DataBaseType databaseType, String message)
    {
        List<String> sessionConfig;
        switch (databaseType) {
            case Oracle:
            case MySql:
            case SQLServer:
                sessionConfig = config.getList(Key.SESSION, new ArrayList<>(), String.class);
                DBUtil.doDealWithSessionConfig(conn, sessionConfig, message);
                break;
            default:
                break;
        }
    }

    private static void doDealWithSessionConfig(Connection conn, List<String> sessions, String message)
    {
        if (null == sessions || sessions.isEmpty()) {
            return;
        }

        Statement stmt;
        try {
            stmt = conn.createStatement();
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(DBUtilErrorCode.SET_SESSION_ERROR,
                    String.format("Failed to set session with [%s]", message), e);
        }

        for (String sessionSql : sessions) {
            LOG.info("Executing SQL:[{}]", sessionSql);
            try {
                stmt.execute(sessionSql);
            }
            catch (SQLException e) {
                throw AddaxException.asAddaxException(DBUtilErrorCode.SET_SESSION_ERROR,
                        String.format("Failed to set session with [%s].", message), e);
            }
        }
        DBUtil.closeDBResources(stmt, null);
    }

    public static void sqlValid(String sql, DataBaseType dataBaseType)
    {
        SQLStatementParser statementParser = SQLParserUtils.createSQLStatementParser(sql, dataBaseType.getTypeName());
        statementParser.parseStatementList();
    }

    /**
     * 异步获取resultSet的next(),注意，千万不能应用在数据的读取中。只能用在meta的获取
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
        Future<Boolean> future = rsExecutors.get().submit(resultSet::next);
        try {
            return future.get(timeout, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(DBUtilErrorCode.RS_ASYNC_ERROR, "Asynchronous retrieval of ResultSet failed.", e);
        }
    }

    public static void loadDriverClass(String pluginType, String pluginName)
    {
        try {
            String pluginJsonPath = StringUtils.join(
                    new String[] {
                            System.getProperty("addax.home"),
                            "plugin",
                            pluginType,
                            String.format("%s%s", pluginName, pluginType),
                            "plugin.json"}, File.separator);
            Configuration configuration = Configuration.from(new File(pluginJsonPath));
            List<String> drivers = configuration.getList("drivers", String.class);
            for (String driver : drivers) {
                Class.forName(driver);
            }
        }
        catch (ClassNotFoundException e) {
            throw AddaxException.asAddaxException(DBUtilErrorCode.CONF_ERROR,
                    "Error loading database driver. Please confirm that the libs directory has the driver jar package "
                            + "and the drivers configuration in plugin.json is correct.", e);
        }
    }
}
