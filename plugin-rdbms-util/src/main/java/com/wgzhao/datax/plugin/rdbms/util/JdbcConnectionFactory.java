package com.wgzhao.datax.plugin.rdbms.util;

import java.sql.Connection;

/**
 * Date: 15/3/16 下午3:12
 */
public class JdbcConnectionFactory
        implements ConnectionFactory
{

    private final DataBaseType dataBaseType;

    private final String jdbcUrl;

    private final String userName;

    private final String password;

    public JdbcConnectionFactory(DataBaseType dataBaseType, String jdbcUrl, String userName, String password)
    {
        this.dataBaseType = dataBaseType;
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public Connection getConnecttion()
    {
        return DBUtil.getConnection(dataBaseType, jdbcUrl, userName, password);
    }

    @Override
    public Connection getConnecttionWithoutRetry()
    {
        return DBUtil.getConnectionWithoutRetry(dataBaseType, jdbcUrl, userName, password);
    }

    @Override
    public String getConnectionInfo()
    {
        return "jdbcUrl:" + jdbcUrl;
    }
}
