/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wgzhao.addax.plugin.writer.tdenginewriter;

import com.wgzhao.addax.core.exception.AddaxException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;

public class SchemaManager
{
    private static final Logger LOG = LoggerFactory.getLogger(SchemaManager.class);

    private final Connection conn;
    private TimestampPrecision precision;

    SchemaManager(Connection conn)
    {
        this.conn = conn;
    }

    public TimestampPrecision loadDatabasePrecision()
            throws AddaxException
    {
        if (this.precision != null) {
            return this.precision;
        }

        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select database()");
            String dbname = null;
            while (rs.next()) {
                dbname = rs.getString("database()");
            }
            if (dbname == null) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "Database not specified or available");
            }

            rs = stmt.executeQuery("show databases");
            while (rs.next()) {
                String name = rs.getString("name");
                if (!name.equalsIgnoreCase(dbname)) {
                    continue;
                }
                String precision = rs.getString("precision");
                switch (precision) {
                    case "ns":
                        this.precision = TimestampPrecision.NANOSEC;
                        break;
                    case "us":
                        this.precision = TimestampPrecision.MICROSEC;
                        break;
                    case "ms":
                    default:
                        this.precision = TimestampPrecision.MILLISEC;
                }
            }
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(EXECUTE_FAIL, e.getMessage());
        }
        return this.precision;
    }

    public Map<String, TableMeta> loadTableMeta(List<String> tables)
            throws AddaxException
    {
        Map<String, TableMeta> tableMetas = new HashMap();

        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("show stables");
            while (rs.next()) {
                TableMeta tableMeta = buildSupTableMeta(rs);
                if (!tables.contains(tableMeta.tbname)) {
                    continue;
                }
                tableMetas.put(tableMeta.tbname, tableMeta);
            }

            rs = stmt.executeQuery("show tables");
            while (rs.next()) {
                TableMeta tableMeta = buildSubTableMeta(rs);
                if (!tables.contains(tableMeta.tbname)) {
                    continue;
                }
                tableMetas.put(tableMeta.tbname, tableMeta);
            }

            for (String tbname : tables) {
                if (!tableMetas.containsKey(tbname)) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR, "table metadata of " + tbname + " is empty!");
                }
            }
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(EXECUTE_FAIL, e.getMessage());
        }
        return tableMetas;
    }

    public Map<String, List<ColumnMeta>> loadColumnMetas(List<String> tables)
            throws AddaxException
    {
        Map<String, List<ColumnMeta>> ret = new HashMap<>();

        for (String table : tables) {
            List<ColumnMeta> columnMetaList = new ArrayList<>();
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("describe " + table);
                for (int i = 0; rs.next(); i++) {
                    ColumnMeta columnMeta = buildColumnMeta(rs, i == 0);
                    columnMetaList.add(columnMeta);
                }
            }
            catch (SQLException e) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL, e.getMessage());
            }

            if (columnMetaList.isEmpty()) {
                LOG.error("column metadata of " + table + " is empty!");
                continue;
            }

            columnMetaList.stream().filter(colMeta -> colMeta.isTag).forEach(colMeta -> {
                String sql = "select " + colMeta.field + " from " + table;
                Object value = null;
                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery(sql);
                    for (int i = 0; rs.next(); i++) {
                        value = rs.getObject(colMeta.field);
                        if (i > 0) {
                            value = null;
                            break;
                        }
                    }
                }
                catch (SQLException e) {
                    LOG.error("failed to get tag value of {} from {}: {}", colMeta.field, table, e);
                }
                colMeta.value = value;
            });

            LOG.debug("load column metadata of " + table + ": " + Arrays.toString(columnMetaList.toArray()));
            ret.put(table, columnMetaList);
        }
        return ret;
    }

    private TableMeta buildSupTableMeta(ResultSet rs)
            throws SQLException
    {
        TableMeta tableMeta = new TableMeta();
        tableMeta.tableType = TableType.SUP_TABLE;
        tableMeta.tbname = rs.getString("name");
        tableMeta.columns = rs.getInt("columns");
        tableMeta.tags = rs.getInt("tags");
        tableMeta.tables = rs.getInt("tables");

        LOG.debug("load table metadata of " + tableMeta.tbname + ": " + tableMeta);
        return tableMeta;
    }

    private TableMeta buildSubTableMeta(ResultSet rs)
            throws SQLException
    {
        TableMeta tableMeta = new TableMeta();
        String stable_name = rs.getString("stable_name");
        tableMeta.tableType = StringUtils.isBlank(stable_name) ? TableType.NML_TABLE : TableType.SUB_TABLE;
        tableMeta.tbname = rs.getString("table_name");
        tableMeta.columns = rs.getInt("columns");
        tableMeta.stable_name = StringUtils.isBlank(stable_name) ? null : stable_name;

        LOG.debug("load table metadata of {}: {}", tableMeta.tbname, tableMeta);
        return tableMeta;
    }

    private ColumnMeta buildColumnMeta(ResultSet rs, boolean isPrimaryKey)
            throws SQLException
    {
        ColumnMeta columnMeta = new ColumnMeta();
        columnMeta.field = rs.getString("Field");
        columnMeta.type = rs.getString("Type");
        columnMeta.length = rs.getInt("Length");
        columnMeta.note = rs.getString("Note");
        columnMeta.isTag = columnMeta.note != null && columnMeta.note.equals("TAG");
        columnMeta.isPrimaryKey = isPrimaryKey;
        return columnMeta;
    }
}
