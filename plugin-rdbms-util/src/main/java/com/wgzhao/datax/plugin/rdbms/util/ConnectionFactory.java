package com.wgzhao.datax.plugin.rdbms.util;

import java.sql.Connection;

/**
 * Date: 15/3/16 下午2:17
 */
public interface ConnectionFactory
{

    Connection getConnecttion();

    Connection getConnecttionWithoutRetry();

    String getConnectionInfo();
}
