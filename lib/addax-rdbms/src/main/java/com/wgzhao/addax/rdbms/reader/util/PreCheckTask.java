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

package com.wgzhao.addax.rdbms.reader.util;

import com.alibaba.druid.sql.parser.ParserException;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.util.RdbmsException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.Callable;

public class PreCheckTask
        implements Callable<Boolean>
{
    private final String userName;
    private final String password;
    private final String splitPkId;
    private final Configuration connection;
    private final DataBaseType dataBaseType;

    public PreCheckTask(String userName, String password, Configuration connection, DataBaseType dataBaseType, String splitPkId)
    {
        this.connection = connection;
        this.userName = userName;
        this.password = password;
        this.dataBaseType = dataBaseType;
        this.splitPkId = splitPkId;
    }

    @Override
    public Boolean call()
            throws AddaxException
    {
        String jdbcUrl = this.connection.getString(Key.JDBC_URL);
        List<Object> querySqls = this.connection.getList(Key.QUERY_SQL, Object.class);
        List<Object> splitPkSqls = this.connection.getList(Key.SPLIT_PK_SQL, Object.class);
        Connection conn = DBUtil.getConnectionWithoutRetry(this.dataBaseType, jdbcUrl, this.userName, password);
        int fetchSize = 1;
        if (DataBaseType.MySql == dataBaseType) {
            fetchSize = Integer.MIN_VALUE;
        }
        try {
            for (int i = 0; i < querySqls.size(); i++) {

                String splitPkSql = null;
                String querySql = querySqls.get(i).toString();

                ResultSet rs = null;
                try {
                    DBUtil.sqlValid(querySql, dataBaseType);
                    if (i == 0) {
                        rs = DBUtil.query(conn, querySql, fetchSize);
                    }
                }
                catch (ParserException e) {
                    throw RdbmsException.asSqlParserException(e, querySql);
                }
                catch (Exception e) {
                    throw RdbmsException.asQueryException(e, querySql);
                }
                finally {
                    DBUtil.closeDBResources(rs, null, null);
                }
                try {
                    if (splitPkSqls != null && !splitPkSqls.isEmpty()) {
                        splitPkSql = splitPkSqls.get(i).toString();
                        DBUtil.sqlValid(splitPkSql, dataBaseType);
                    }
                }
                catch (ParserException e) {
                    throw RdbmsException.asSqlParserException(e, splitPkSql);
                }
                catch (AddaxException e) {
                    throw e;
                }
                catch (Exception e) {
                    throw RdbmsException.asSplitPKException(e, splitPkSql, splitPkId.trim());
                }
            }
        }
        finally {
            DBUtil.closeDBResources(null, conn);
        }
        return true;
    }
}
