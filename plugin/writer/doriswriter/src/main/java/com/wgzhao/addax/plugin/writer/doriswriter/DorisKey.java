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

import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.util.RdbmsException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.core.base.Constant.DEFAULT_BATCH_SIZE;
import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/** Doris Key configuration keys. */
public class DorisKey
        extends Key
{

    private static final int MAX_RETRIES = 3;
    private static final long DEFAULT_FLUSH_INTERVAL = 3_000;

    private static final String LOAD_PROPS_FORMAT = "format";

    /** Stream Load Format configuration keys. */
    public enum StreamLoadFormat
    {
        CSV, JSON
    }

    private static final String FLUSH_INTERVAL = "flushInterval";
    private static final String LOAD_URL = "loadUrl";
    private static final String FLUSH_QUEUE_LENGTH = "flushQueueLength";
    private static final String LOAD_PROPS = "loadProps";
    private static final String COLUMN_SEPARATOR = "column_separator";
    private static final String LINE_SEPARATOR = "line_delimiter";
    private static final String DEFAULT_LABEL_PREFIX = "addax_doris_writer_";

    private final Configuration loadProps;
    private final Configuration options;
    private final StreamLoadFormat streamLoadFormat;

    private List<String> infoSchemaColumns;

    private final String database;
    private final String jdbcUrl;
    private final String table;

    /** Doriskey. */
    public DorisKey(Configuration options)
    {
        this.options = options;
        Configuration conn = options.getConfiguration(CONNECTION);
        this.database = conn.getNecessaryValue(DATABASE, REQUIRED_VALUE);
        this.jdbcUrl = conn.getNecessaryValue(JDBC_URL, REQUIRED_VALUE);
        this.table = conn.getList(TABLE, String.class).get(0);
        if (options.getString(LOAD_PROPS, null) == null) {
            this.loadProps = Configuration.newDefault();
        }
        else if (options.getString(LOAD_PROPS).startsWith("{")) {
            this.loadProps = options.getConfiguration(LOAD_PROPS);
        } else {
            throw AddaxException.asAddaxException(CONFIG_ERROR, "The format of loadProps should be a map");
        }
        this.streamLoadFormat = StreamLoadFormat.valueOf(loadProps.getString(LOAD_PROPS_FORMAT, "csv").toUpperCase());
        infoSchemaColumns = options.getList(COLUMN, String.class);

        if (1 == infoSchemaColumns.size() && "*".trim().equals(infoSchemaColumns.get(0))) {
            // get columns from database
            this.infoSchemaColumns = getDorisTableColumns();
        }
    }

    /** Returns the doristablecolumns. */
    public List<String> getDorisTableColumns()
    {
        String currentSql = "SELECT COLUMN_NAME FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA` = '" +
                database + "' AND `TABLE_NAME` = '" + table + "' ORDER BY `ORDINAL_POSITION` ASC;";
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

    /** Dopretreatment. */
    public void doPretreatment()
    {
        validateStreamLoadUrl();
    }

    /** Returns the jdbcurl. */
    public String getJdbcUrl()
    {
        return this.jdbcUrl;
    }

    /** Returns the database. */
    public String getDatabase()
    {
        return this.database;
    }

    /** Returns the table. */
    public String getTable()
    {
        return this.table;
    }

    /** Returns the username. */
    public String getUsername()
    {
        return options.getString(USERNAME);
    }

    /** Returns the password. */
    public String getPassword()
    {
        return options.getString(PASSWORD);
    }

    /** Returns the labelprefix. */
    public String getLabelPrefix()
    {
        return DEFAULT_LABEL_PREFIX;
    }

    /** Returns the loadurllist. */
    public List<String> getLoadUrlList()
    {
        return options.getList(LOAD_URL, String.class);
    }

    /** Returns the columns. */
    public List<String> getColumns()
    {
        return this.infoSchemaColumns;
    }

    /** Returns the presqllist. */
    public List<String> getPreSqlList()
    {
        return options.getList(PRE_SQL, String.class);
    }

    /** Returns the postsqllist. */
    public List<String> getPostSqlList()
    {
        return options.getList(POST_SQL, String.class);
    }

    /** Returns the maxretries. */
    public int getMaxRetries()
    {
        return MAX_RETRIES;
    }

    /** Returns the batchsize. */
    public long getBatchSize()
    {
        return options.getLong(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    }

    /** Returns the flushinterval. */
    public long getFlushInterval()
    {
        return options.getLong(FLUSH_INTERVAL, DEFAULT_FLUSH_INTERVAL);
    }

    /** Returns the flushqueuelength. */
    public int getFlushQueueLength()
    {
        return options.getInt(FLUSH_QUEUE_LENGTH, 1);
    }

    /** Returns the streamloadformat. */
    public StreamLoadFormat getStreamLoadFormat()
    {
        return streamLoadFormat;
    }

    /** Checks whether the jsonformat condition holds. */
    public boolean isJsonFormat()
    {
        return StreamLoadFormat.JSON.equals(streamLoadFormat);
    }

    /** Checks whether the csvformat condition holds. */
    public boolean isCsvFormat()
    {
        return StreamLoadFormat.CSV.equals(streamLoadFormat);
    }

    private void validateStreamLoadUrl()
    {
        List<String> urlList = getLoadUrlList();
        for (String host : urlList) {
            if (host.split(":").length < 2) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "The format of loadUrl is not correct, please enter:[`fe_ip:fe_http_ip;fe_ip:fe_http_ip`].");
            }
        }
    }

    /** Returns the linedelimiter. */
    public String getLineDelimiter()
    {
        return loadProps.getString(LINE_SEPARATOR, "\n");
    }

    /** Returns the columnseparator. */
    public String getColumnSeparator()
    {
        return loadProps.getString(COLUMN_SEPARATOR, "\t");
    }

    /** Loadprops2map. */
    public Map<String, String> loadProps2Map()
    {
        Map<String, String> result = new HashMap<>();
        loadProps.getKeys().forEach(key -> {
            result.put(key, loadProps.getString(key));
        });
        return result;
    }
}
