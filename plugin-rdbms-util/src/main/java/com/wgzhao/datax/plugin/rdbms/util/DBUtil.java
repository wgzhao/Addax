package com.wgzhao.datax.plugin.rdbms.util;

import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.util.Configuration;
import com.wgzhao.datax.common.util.RetryUtil;
import com.wgzhao.datax.plugin.rdbms.reader.Key;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class DBUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(DBUtil.class);

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
            throw DataXException.asDataXException(
                    DBUtilErrorCode.CONF_ERROR,
                    String.format("您的jdbcUrl的配置信息有错, 因为jdbcUrl[%s]不能为空. 请检查您的配置并作出修改.",
                            StringUtils.join(jdbcUrls, ",")));
        }
        try {
            return RetryUtil.executeWithRetry(() -> {
                boolean connOK;
                for (String url : jdbcUrls) {
                    if (StringUtils.isNotBlank(url)) {
                        url = url.trim();
                        if (null != preSql && !preSql.isEmpty()) {
                            connOK = testConnWithoutRetry(dataBaseType, url, username, password, preSql);
                        }
                        else {
                            connOK = testConnWithoutRetry(dataBaseType, url, username, password);
                        }
                        if (connOK) {
                            return url;
                        }
                    }
                }
                throw new Exception("DataX无法连接对应的数据库，可能原因是：1) 配置的ip/port/database/jdbc错误，无法连接。" +
                        "2) 配置的username/password错误，鉴权失败。请和DBA确认该数据库的连接信息是否正确。");
            }, 3, 1000L, true);
        }
        catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.CONN_DB_ERROR,
                    String.format("数据库连接失败. 因为根据您配置的连接信息,无法从:%s 中找到可连接的jdbcUrl. 请检查您的配置并作出修改.",
                            StringUtils.join(jdbcUrls, ",")), e);
        }
    }

    public static String chooseJdbcUrlWithoutRetry(DataBaseType dataBaseType,
            List<String> jdbcUrls, String username,
            String password, List<String> preSql)
    {
        if (null == jdbcUrls || jdbcUrls.isEmpty()) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.CONF_ERROR,
                    String.format("您的jdbcUrl的配置信息有错, 因为jdbcUrl[%s]不能为空. 请检查您的配置并作出修改.",
                            StringUtils.join(jdbcUrls, ",")));
        }

        boolean connOK;
        for (String url : jdbcUrls) {
            if (StringUtils.isNotBlank(url)) {
                url = url.trim();
                if (null != preSql && !preSql.isEmpty()) {
                    connOK = testConnWithoutRetry(dataBaseType,
                            url, username, password, preSql);
                }
                else {
                    try {
                        connOK = testConnWithoutRetry(dataBaseType,
                                url, username, password);
                    }
                    catch (Exception e) {
                        throw DataXException.asDataXException(
                                DBUtilErrorCode.CONN_DB_ERROR,
                                String.format("数据库连接失败. 因为根据您配置的连接信息,无法从:%s 中找到可连接的jdbcUrl. 请检查您的配置并作出修改.",
                                        StringUtils.join(jdbcUrls, ",")), e);
                    }
                }
                if (connOK) {
                    return url;
                }
            }
        }
        throw DataXException.asDataXException(
                DBUtilErrorCode.CONN_DB_ERROR,
                String.format("数据库连接失败. 因为根据您配置的连接信息,无法从:%s 中找到可连接的jdbcUrl. 请检查您的配置并作出修改.",
                        StringUtils.join(jdbcUrls, ",")));
    }

    public static boolean checkInsertPrivilege(DataBaseType dataBaseType, String jdbcURL, String userName, String password, List<String> tableList)
    {
        Connection connection = connect(dataBaseType, jdbcURL, userName, password);
        String insertTemplate = "insert into %s(select * from %s where 1 = 2)";

        boolean hasInsertPrivilege = true;
        Statement insertStmt;
        for (String tableName : tableList) {
            String checkInsertPrivilegeSql = String.format(insertTemplate, tableName, tableName);
            try {
                insertStmt = connection.createStatement();
                executeSqlWithoutResultSet(insertStmt, checkInsertPrivilegeSql);
            }
            catch (Exception e) {
                if (DataBaseType.Oracle == dataBaseType) {
                    if (e.getMessage() != null && e.getMessage().contains("insufficient privileges")) {
                        hasInsertPrivilege = false;
                        LOG.warn("User [{}] has no 'insert' privilege on table[{}], errorMessage:[{}]", userName, tableName, e.getMessage());
                    }
                }
                else {
                    hasInsertPrivilege = false;
                    LOG.warn("User [{}] has no 'insert' privilege on table[{}], errorMessage:[{}]", userName, tableName, e.getMessage());
                }
            }
        }
        try {
            connection.close();
        }
        catch (SQLException e) {
            LOG.warn("connection close failed, {}", e.getMessage());
        }
        return hasInsertPrivilege;
    }

    public static boolean checkDeletePrivilege(DataBaseType dataBaseType, String jdbcURL, String userName, String password, List<String> tableList)
    {
        Connection connection = connect(dataBaseType, jdbcURL, userName, password);
        String deleteTemplate = "delete from %s WHERE 1 = 2";

        boolean hasInsertPrivilege = true;
        Statement deleteStmt;
        for (String tableName : tableList) {
            String checkDeletePrivilegeSQL = String.format(deleteTemplate, tableName);
            try {
                deleteStmt = connection.createStatement();
                executeSqlWithoutResultSet(deleteStmt, checkDeletePrivilegeSQL);
            }
            catch (Exception e) {
                hasInsertPrivilege = false;
                LOG.warn("User [{}] has no 'delete' privilege on table[{}], errorMessage:[{}]", userName, tableName, e.getMessage());
            }
        }
        try {
            connection.close();
        }
        catch (SQLException e) {
            LOG.warn("connection close failed, {}", e.getMessage());
        }
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
     * NOTE: In DataX, we don't need connection pool in fact
     *
     * @param dataBaseType database type.
     * @param jdbcUrl java jdbc url.
     * @param username User for login.
     * @param password Password to use when connecting to server.
     * @return Connection class {@link Connection}
     */
    public static Connection getConnection(DataBaseType dataBaseType,
            String jdbcUrl, String username, String password)
    {

        return getConnection(dataBaseType, jdbcUrl, username, password, String.valueOf(Constant.SOCKET_TIMEOUT_INSECOND * 1000));
    }

    public static Connection getConnection(DataBaseType dataBaseType,
            String jdbcUrl, String username, String password, String socketTimeout)
    {

        try {
            return RetryUtil.executeWithRetry(() -> DBUtil.connect(dataBaseType, jdbcUrl, username,
                    password, socketTimeout), 3, 1000L, true);
        }
        catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.CONN_DB_ERROR,
                    String.format("数据库连接失败. 因为根据您配置的连接信息:%s获取数据库连接失败. 请检查您的配置并作出修改.", jdbcUrl), e);
        }
    }

    /**
     * Get direct JDBC connection
     * <p>
     * if connecting failed, try to connect for MAX_TRY_TIMES times
     * <p>
     * NOTE: In DataX, we don't need connection pool in fact
     *
     * @param dataBaseType  The database's type
     * @param jdbcUrl jdbc url
     * @param username User for login
     * @param password Password to use when connecting to server
     * @return Connection class {@link Connection}
     */
    public static Connection getConnectionWithoutRetry(DataBaseType dataBaseType,
            String jdbcUrl, String username, String password)
    {
        return getConnectionWithoutRetry(dataBaseType, jdbcUrl, username,
                password, String.valueOf(Constant.SOCKET_TIMEOUT_INSECOND * 1000));
    }

    public static Connection getConnectionWithoutRetry(DataBaseType dataBaseType,
            String jdbcUrl, String username, String password, String socketTimeout)
    {
        return DBUtil.connect(dataBaseType, jdbcUrl, username,
                password, socketTimeout);
    }

    private static synchronized Connection connect(DataBaseType dataBaseType,
            String url, String user, String pass)
    {
        return connect(dataBaseType, url, user, pass, String.valueOf(Constant.SOCKET_TIMEOUT_INSECOND * 1000));
    }

    private static synchronized Connection connect(DataBaseType dataBaseType,
            String url, String user, String pass, String socketTimeout)
    {

        Properties prop = new Properties();
        prop.put("user", user);
        prop.put("password", pass);

        if (dataBaseType == DataBaseType.Oracle) {
            //oracle.net.READ_TIMEOUT for jdbc versions < 10.1.0.5 oracle.jdbc.ReadTimeout for jdbc versions >=10.1.0.5
            // unit ms
            prop.put("oracle.jdbc.ReadTimeout", socketTimeout);
        }

        return connect(dataBaseType, url, prop);
    }

    private static synchronized Connection connect(DataBaseType dataBaseType,
            String url, Properties prop)
    {
        try {
            if (url.contains("inceptor2")) {
                LOG.warn("inteptor2 must be process specially");
                url = url.replace("inceptor2", "hive2");
                Class.forName("org.apache.hive.jdbc.HiveDriver");
                return DriverManager.getConnection(url, prop.getProperty("user"), prop.getProperty("password"));
            }
            else {
                Class.forName(dataBaseType.getDriverClassName());
                DriverManager.setLoginTimeout(Constant.TIMEOUT_SECONDS);
                return DriverManager.getConnection(url, prop);
            }
        }
        catch (Exception e) {
            throw RdbmsException.asConnException(dataBaseType, e, prop.getProperty("user"), null);
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
        return query(conn, sql, fetchSize, Constant.SOCKET_TIMEOUT_INSECOND);
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param conn Database connection .
     * @param sql sql statement to be executed
     * @param fetchSize fetch size each batch
     * @param queryTimeout unit:second
     * @return A {@link ResultSet}
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize, int queryTimeout)
            throws SQLException
    {
        // make sure autocommit is off
        conn.setAutoCommit(false);

        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY); //NOSONAR
        stmt.setFetchSize(fetchSize);
        stmt.setQueryTimeout(queryTimeout);
        return query(stmt, sql);
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param stmt {@link Statement}
     * @param sql sql statement to be executed
     * @return a {@link ResultSet}
     * @throws SQLException if occurs SQLException.
     */
    public static ResultSet query(Statement stmt, String sql)
            throws SQLException
    {
        return stmt.executeQuery(sql);
    }

    public static void executeSqlWithoutResultSet(Statement stmt, String sql)
            throws SQLException
    {
        stmt.execute(sql);
    }

    /**
     * Close {@link ResultSet}, {@link Statement} referenced by this
     * {@link ResultSet}
     *
     * @param rs {@link ResultSet} to be closed
     */
    public static void closeResultSet(ResultSet rs)
    {
        try {
            if (null != rs) {
                Statement stmt = rs.getStatement();
                if (null != stmt) {
                    stmt.close();
                }
                rs.close();
            }
        }
        catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void closeDBResources(ResultSet rs, Statement stmt,
            Connection conn)
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

    public static List<String> getTableColumns(DataBaseType dataBaseType,
            String jdbcUrl, String user, String pass, String tableName)
    {
        Connection conn = getConnection(dataBaseType, jdbcUrl, user, pass);
        return getTableColumnsByConn(dataBaseType, conn, tableName);
    }

    public static List<String> getTableColumnsByConn(DataBaseType dataBaseType, Connection conn, String tableName)
    {
        List<String> columns = new ArrayList<>();
        Statement statement = null;
        ResultSet rs = null;
        String queryColumnSql = null;
        try {
            statement = conn.createStatement();
            queryColumnSql = String.format("select * from %s where 1=2",
                    tableName);
            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
                columns.add(rsMetaData.getColumnName(i + 1));
            }
        }
        catch (SQLException e) {
            throw RdbmsException.asQueryException(dataBaseType, e, queryColumnSql, tableName, null);
        }
        finally {
            DBUtil.closeDBResources(rs, statement, conn);
        }

        return columns;
    }

    /**
     * get column description
     *
     * @param conn database connection
     * @param tableName  The table name
     * @param column table column
     * @return Left:ColumnName Middle:ColumnType Right:ColumnTypeName
     */
    public static Triple<List<String>, List<Integer>, List<String>> getColumnMetaData(
            Connection conn, String tableName, String column)
    {
        Statement statement = null;
        ResultSet rs = null;

        Triple<List<String>, List<Integer>, List<String>> columnMetaData = new ImmutableTriple<>(
                new ArrayList<>(), new ArrayList<>(),
                new ArrayList<>());
        try {
            statement = conn.createStatement();
            String queryColumnSql = "select " + column + " from " + tableName
                    + " where 1=2";

            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {

                columnMetaData.getLeft().add(rsMetaData.getColumnName(i + 1));
                columnMetaData.getMiddle().add(rsMetaData.getColumnType(i + 1));
                columnMetaData.getRight().add(
                        rsMetaData.getColumnTypeName(i + 1));
            }
            return columnMetaData;
        }
        catch (SQLException e) {
            throw DataXException
                    .asDataXException(DBUtilErrorCode.GET_COLUMN_INFO_FAILED,
                            String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.", tableName), e);
        }
        finally {
            DBUtil.closeDBResources(rs, statement, null);
        }
    }

    public static boolean testConnWithoutRetry(DataBaseType dataBaseType,
            String url, String user, String pass)
    {
        Connection connection = null;
        try {
            connection = connect(dataBaseType, url, user, pass);
            return true;
        }
        catch (Exception e) {
            LOG.error("test connection of [{}] failed, for {}.", url,
                    e.getMessage());
        }
        finally {
            DBUtil.closeDBResources(null, connection);
        }
        return false;
    }

    public static boolean testConnWithoutRetry(DataBaseType dataBaseType,
            String url, String user, String pass, List<String> preSql)
    {
        Connection connection = null;
        try {
            connection = connect(dataBaseType, url, user, pass);
            for (String pre : preSql) {
                if (!doPreCheck(connection, pre)) {
                    LOG.warn("doPreCheck failed.");
                    return false;
                }
            }
            return true;
        }
        catch (Exception e) {
            LOG.warn("test connection of [{}] failed, for {}.", url,
                    e.getMessage());
        }
        finally {
            DBUtil.closeDBResources(null, connection);
        }

        return false;
    }

    public static ResultSet query(Connection conn, String sql)
            throws SQLException
    {
        try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY)) {
            //默认3600 seconds
            stmt.setQueryTimeout(Constant.SOCKET_TIMEOUT_INSECOND);
            return query(stmt, sql);
        }
    }

    private static boolean doPreCheck(Connection conn, String pre)
    {
        ResultSet rs = null;
        try {
            rs = query(conn, pre);

            int checkResult = -1;
            if (DBUtil.asyncResultSetNext(rs)) {
                checkResult = rs.getInt(1);
                if (DBUtil.asyncResultSetNext(rs)) {
                    LOG.warn(
                            "pre check failed. It should return one result:0, pre:[{}].",
                            pre);
                    return false;
                }
            }

            if (0 == checkResult) {
                return true;
            }

            LOG.warn(
                    "pre check failed. It should return one result:0, pre:[{}].",
                    pre);
        }
        catch (Exception e) {
            LOG.warn("pre check failed. pre:[{}], errorMessage:[{}].", pre,
                    e.getMessage());
        }
        finally {
            DBUtil.closeResultSet(rs);
        }
        return false;
    }

    // warn:until now, only oracle need to handle session config.
    public static void dealWithSessionConfig(Connection conn,
            Configuration config, DataBaseType databaseType, String message)
    {
        List<String> sessionConfig;
        switch (databaseType) {
            case Oracle:
            case MySql:
                sessionConfig = config.getList(Key.SESSION,
                        new ArrayList<>(), String.class);
                DBUtil.doDealWithSessionConfig(conn, sessionConfig, message);
                break;
            default:
                break;
        }
    }

    private static void doDealWithSessionConfig(Connection conn,
            List<String> sessions, String message)
    {
        if (null == sessions || sessions.isEmpty()) {
            return;
        }

        Statement stmt;
        try {
            stmt = conn.createStatement();
        }
        catch (SQLException e) {
            throw DataXException
                    .asDataXException(DBUtilErrorCode.SET_SESSION_ERROR, String
                                    .format("session配置有误. 因为根据您的配置执行 session 设置失败. 上下文信息是:[%s]. 请检查您的配置并作出修改.", message),
                            e);
        }

        for (String sessionSql : sessions) {
            LOG.info("execute sql:[{}]", sessionSql);
            try {
                DBUtil.executeSqlWithoutResultSet(stmt, sessionSql);
            }
            catch (SQLException e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.SET_SESSION_ERROR, String.format(
                                "session配置有误. 因为根据您的配置执行 session 设置失败. 上下文信息是:[%s]. 请检查您的配置并作出修改.", message), e);
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
     * @param resultSet resut set
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
            throw DataXException.asDataXException(
                    DBUtilErrorCode.RS_ASYNC_ERROR, "异步获取ResultSet失败", e);
        }
    }

    public static void loadDriverClass(String pluginType, String pluginName)
    {
        try {
            String pluginJsonPath = StringUtils.join(
                    new String[] {System.getProperty("datax.home"), "plugin",
                            pluginType,
                            String.format("%s%s", pluginName, pluginType),
                            "plugin.json"}, File.separator);
            Configuration configuration = Configuration.from(new File(
                    pluginJsonPath));
            List<String> drivers = configuration.getList("drivers",
                    String.class);
            for (String driver : drivers) {
                Class.forName(driver);
            }
        }
        catch (ClassNotFoundException e) {
            throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                    "数据库驱动加载错误, 请确认libs目录有驱动jar包且plugin.json中drivers配置驱动类正确!",
                    e);
        }
    }
}
