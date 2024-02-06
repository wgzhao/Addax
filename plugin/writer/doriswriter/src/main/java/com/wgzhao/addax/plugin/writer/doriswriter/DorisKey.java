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

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DBUtilErrorCode;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.util.RdbmsException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.wgzhao.addax.common.base.Constant.DEFAULT_BATCH_SIZE;

public class DorisKey
        extends Key
{

    private static final int MAX_RETRIES = 3;
    private static final long DEFAULT_FLUSH_INTERVAL = 3_000;

    private static final String LOAD_PROPS_FORMAT = "format";

    public enum StreamLoadFormat
    {
        CSV, JSON;
    }

    private static final String FLUSH_INTERVAL = "flushInterval";
    private static final String LOAD_URL = "loadUrl";
    private static final String FLUSH_QUEUE_LENGTH = "flushQueueLength";
    private static final String LOAD_PROPS = "loadProps";

    private static final String DEFAULT_LABEL_PREFIX = "addax_doris_writer_";


    private final Configuration options;

    private List<String> infoSchemaColumns;

    private final String database;
    private final String jdbcUrl;
    private final String table;

    public DorisKey(Configuration options)
    {
        this.options = options;
        Configuration conn = Configuration.from(options.getList(CONNECTION).get(0).toString());
        this.database = conn.getNecessaryValue(DATABASE, DBUtilErrorCode.REQUIRED_VALUE);
        this.jdbcUrl = conn.getNecessaryValue(JDBC_URL, DBUtilErrorCode.REQUIRED_VALUE);
        this.table = conn.getList(TABLE, String.class).get(0);

        infoSchemaColumns = options.getList(COLUMN, String.class);
//        this.userSetColumns = options.getList(COLUMN, String.class).stream().map(str -> str.replace("`", "")).collect(Collectors.toList());
        if (1 == infoSchemaColumns.size() && "*".trim().equals(infoSchemaColumns.get(0))) {
            // get columns from database
            this.infoSchemaColumns = getDorisTableColumns();
        }
    }

    public List<String> getDorisTableColumns()
    {
        String currentSql = String.format("SELECT COLUMN_NAME FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s' ORDER BY `ORDINAL_POSITION` ASC;",
                database, table);
        List<String> columns = new ArrayList<>();
        ResultSet rs = null;
        try (Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, options.getString(USERNAME), options.getString(PASSWORD))) {
            Statement stmt = conn.createStatement();
            rs = stmt.executeQuery(currentSql);
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                columns.add(colName);
            }
            return columns;
        }
        catch (Exception e) {
            throw RdbmsException.asQueryException(e, currentSql);
        }
        finally {
            DBUtil.closeDBResources(rs, null, null);
        }
    }

    public void doPretreatment()
    {
//        validateRequired();
        validateStreamLoadUrl();
    }

    public String getJdbcUrl()
    {
        return this.jdbcUrl;
    }

    public String getDatabase()
    {
        return this.database;
    }

    public String getTable()
    {
        return this.table;
    }

    public String getUsername()
    {
        return options.getString(USERNAME);
    }

    public String getPassword()
    {
        return options.getString(PASSWORD);
    }

    public String getLabelPrefix()
    {
        return DEFAULT_LABEL_PREFIX;
    }

    public List<String> getLoadUrlList()
    {
        return options.getList(LOAD_URL, String.class);
    }

    public List<String> getColumns()
    {
        return this.infoSchemaColumns;
    }

    public List<String> getPreSqlList()
    {
        return options.getList(PRE_SQL, String.class);
    }

    public List<String> getPostSqlList()
    {
        return options.getList(POST_SQL, String.class);
    }

    public Map<String, Object> getLoadProps()
    {
        return options.getMap(LOAD_PROPS);
    }

    public int getMaxRetries()
    {
        return MAX_RETRIES;
    }

    public long getBatchSize()
    {
        Long size = options.getLong(BATCH_SIZE);
        return null == size ? DEFAULT_BATCH_SIZE : size;
    }

    public long getFlushInterval()
    {
        Long interval = options.getLong(FLUSH_INTERVAL);
        return null == interval ? DEFAULT_FLUSH_INTERVAL : interval;
    }

    public int getFlushQueueLength()
    {
        Integer len = options.getInt(FLUSH_QUEUE_LENGTH);
        return null == len ? 1 : len;
    }

    public StreamLoadFormat getStreamLoadFormat()
    {
        Map<String, Object> loadProps = getLoadProps();
        if (null == loadProps) {
            return StreamLoadFormat.CSV;
        }
        if (loadProps.containsKey(LOAD_PROPS_FORMAT)
                && StreamLoadFormat.JSON.name().equalsIgnoreCase(String.valueOf(loadProps.get(LOAD_PROPS_FORMAT)))) {
            return StreamLoadFormat.JSON;
        }
        return StreamLoadFormat.CSV;
    }

    private void validateStreamLoadUrl()
    {
        List<String> urlList = getLoadUrlList();
        for (String host : urlList) {
            if (host.split(":").length < 2) {
                throw AddaxException.asAddaxException(DBUtilErrorCode.CONF_ERROR,
                        "The format of loadUrl is not correct, please enter:[`fe_ip:fe_http_ip;fe_ip:fe_http_ip`].");
            }
        }
    }
}
