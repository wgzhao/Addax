package com.wgzhao.datax.plugin.rdbms.reader.util;

import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.util.Configuration;
import com.wgzhao.datax.plugin.rdbms.reader.Key;
import com.wgzhao.datax.plugin.rdbms.util.DBUtil;
import com.wgzhao.datax.plugin.rdbms.util.DataBaseType;
import com.wgzhao.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.druid.sql.parser.ParserException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by judy.lt on 2015/6/4.
 */
public class PreCheckTask
        implements Callable<Boolean>
{
    private final String userName;
    private final String password;
    private final String splitPkId;
    private final Configuration connection;
    private final DataBaseType dataBaseType;

    public PreCheckTask(String userName,
            String password,
            Configuration connection,
            DataBaseType dataBaseType,
            String splitPkId)
    {
        this.connection = connection;
        this.userName = userName;
        this.password = password;
        this.dataBaseType = dataBaseType;
        this.splitPkId = splitPkId;
    }

    @Override
    public Boolean call()
            throws DataXException
    {
        String jdbcUrl = this.connection.getString(Key.JDBC_URL);
        List<Object> querySqls = this.connection.getList(Key.QUERY_SQL, Object.class);
        List<Object> splitPkSqls = this.connection.getList(Key.SPLIT_PK_SQL, Object.class);
        List<Object> tables = this.connection.getList(Key.TABLE, Object.class);
        Connection conn = DBUtil.getConnectionWithoutRetry(this.dataBaseType, jdbcUrl,
                this.userName, password);
        int fetchSize = 1;
        if (DataBaseType.MySql == dataBaseType) {
            fetchSize = Integer.MIN_VALUE;
        }
        try {
            for (int i = 0; i < querySqls.size(); i++) {

                String splitPkSql = null;
                String querySql = querySqls.get(i).toString();

                String table = null;
                if (tables != null && !tables.isEmpty()) {
                    table = tables.get(i).toString();
                }

                /*verify query*/
                ResultSet rs = null;
                try {
                    DBUtil.sqlValid(querySql, dataBaseType);
                    if (i == 0) {
                        rs = DBUtil.query(conn, querySql, fetchSize);
                    }
                }
                catch (ParserException e) {
                    throw RdbmsException.asSqlParserException(this.dataBaseType, e, querySql);
                }
                catch (Exception e) {
                    throw RdbmsException.asQueryException(this.dataBaseType, e, querySql, table, userName);
                }
                finally {
                    DBUtil.closeDBResources(rs, null, null);
                }
                /*verify splitPK*/
                try {
                    if (splitPkSqls != null && !splitPkSqls.isEmpty()) {
                        splitPkSql = splitPkSqls.get(i).toString();
                        DBUtil.sqlValid(splitPkSql, dataBaseType);
                        if (i == 0) {
                            SingleTableSplitUtil.precheckSplitPk(conn, splitPkSql, fetchSize, table, userName);
                        }
                    }
                }
                catch (ParserException e) {
                    throw RdbmsException.asSqlParserException(this.dataBaseType, e, splitPkSql);
                }
                catch (DataXException e) {
                    throw e;
                }
                catch (Exception e) {
                    throw RdbmsException.asSplitPKException(this.dataBaseType, e, splitPkSql, this.splitPkId.trim());
                }
            }
        }
        finally {
            DBUtil.closeDBResources(null, conn);
        }
        return true;
    }
}
