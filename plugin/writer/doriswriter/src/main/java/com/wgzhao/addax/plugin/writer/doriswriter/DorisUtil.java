/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.plugin.writer.doriswriter;

import com.alibaba.druid.sql.parser.ParserException;
import com.google.common.base.Strings;
import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.util.RdbmsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * jdbc util
 */
public class DorisUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(DorisUtil.class);

    private DorisUtil() {}

    public static List<String> renderPreOrPostSqls(List<String> preOrPostSqls, String tableName)
    {
        if (null == preOrPostSqls) {
            return Collections.emptyList();
        }
        List<String> renderedSqls = new ArrayList<>();
        for (String sql : preOrPostSqls) {
            if (!Strings.isNullOrEmpty(sql)) {
                renderedSqls.add(sql.replace(Constant.TABLE_NAME_PLACEHOLDER, tableName));
            }
        }
        return renderedSqls;
    }

    public static void executeSqls(Connection conn, List<String> sqls)
    {
        try {
            for (String sql : sqls) {
                LOG.info("Executing sql:[{}].", sql);
                DBUtil.query(conn, sql);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void preCheckPrePareSQL(DorisKey options)
    {
        String table = options.getTable();
        List<String> preSqls = options.getPreSqlList();
        List<String> renderedPreSqls = DorisUtil.renderPreOrPostSqls(preSqls, table);
        if (!renderedPreSqls.isEmpty()) {
            LOG.info("Begin to preCheck preSqls:[{}].", String.join(";", renderedPreSqls));
            for (String sql : renderedPreSqls) {
                try {
                    DBUtil.sqlValid(sql, DataBaseType.MySql);
                }
                catch (ParserException e) {
                    throw RdbmsException.asPreSQLParserException(e, sql);
                }
            }
        }
    }

    public static void preCheckPostSQL(DorisKey options)
    {
        String table = options.getTable();
        List<String> postSqls = options.getPostSqlList();
        List<String> renderedPostSqls = DorisUtil.renderPreOrPostSqls(postSqls, table);
        if (!renderedPostSqls.isEmpty()) {
            LOG.info("Begin to preCheck postSqls:[{}].", String.join(";", renderedPostSqls));
            for (String sql : renderedPostSqls) {
                try {
                    DBUtil.sqlValid(sql, DataBaseType.MySql);
                }
                catch (ParserException e) {
                    throw RdbmsException.asPostSQLParserException(e, sql);
                }
            }
        }
    }
}
