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

import com.taosdata.jdbc.SchemalessWriter;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.utils.Utils;
import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DefaultDataHandler
        implements DataHandler
{
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDataHandler.class);
    private static final String DEFAULT_USERNAME = "root";
    private static final String DEFAULT_PASSWORD = "taosdata";
    private static final boolean DEFAULT_IGNORE_TAGS_UNMATCHED = false;
    private String username;
    private String password;
    private String jdbcUrl;
    private int batchSize;
    private boolean ignoreTagsUnmatched;

    private List<String> tables;
    private List<String> columns;

    private Map<String, TableMeta> tableMetas;
    private SchemaManager schemaManager;

    public void setTableMetas(Map<String, TableMeta> tableMetas)
    {
        this.tableMetas = tableMetas;
    }

    public void setColumnMetas(Map<String, List<ColumnMeta>> columnMetas)
    {
        this.columnMetas = columnMetas;
    }

    public void setSchemaManager(SchemaManager schemaManager)
    {
        this.schemaManager = schemaManager;
    }

    private Map<String, List<ColumnMeta>> columnMetas;

    static {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public DefaultDataHandler(Configuration configuration)
    {
        this.username = configuration.getString(Key.USERNAME, DEFAULT_USERNAME);
        this.password = configuration.getString(Key.PASSWORD, DEFAULT_PASSWORD);
        this.jdbcUrl = configuration.getString(Key.JDBC_URL);
        this.batchSize = configuration.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
        this.tables = configuration.getList(Key.TABLE, String.class);
        this.columns = configuration.getList(Key.COLUMN, String.class);
        this.ignoreTagsUnmatched = configuration.getBool(TDKey.IGNORE_TAGS_UNMATCHED, DEFAULT_IGNORE_TAGS_UNMATCHED);
    }

    @Override
    public int handle(RecordReceiver lineReceiver, TaskPluginCollector collector)
    {
        int count = 0;
        int affectedRows = 0;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            LOG.info("connection[ jdbcUrl: " + jdbcUrl + ", username: " + username + "] established.");
            // prepare table_name -> table_meta
            this.schemaManager = new SchemaManager(conn);
            this.tableMetas = schemaManager.loadTableMeta(tables);
            // prepare table_name -> column_meta
            this.columnMetas = schemaManager.loadColumnMetas(tables);

            List<Record> recordBatch = new ArrayList<>();
            Record record;
            for (int i = 1; (record = lineReceiver.getFromReader()) != null; i++) {
                if (i % batchSize != 0) {
                    recordBatch.add(record);
                }
                else {
                    affectedRows = writeBatch(conn, recordBatch);
                    recordBatch.clear();
                }
                count++;
            }

            if (!recordBatch.isEmpty()) {
                affectedRows = writeBatch(conn, recordBatch);
                recordBatch.clear();
            }
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(TDengineWriterErrorCode.RUNTIME_EXCEPTION, e.getMessage());
        }

        if (affectedRows != count) {
            LOG.error("write record missing or incorrect happened, affectedRows: " + affectedRows + ", total: " + count);
        }

        return affectedRows;
    }

    /**
     * table: [ "stb1", "stb2", "tb1", "tb2", "t1" ]
     * stb1[ts,f1,f2] tags:[t1]
     * stb2[ts,f1,f2,f3] tags:[t1,t2]
     * 1. tables 表的的类型分成：stb(super table)/tb(sub table)/t(original table)
     * 2. 对于stb，自动建表/schemaless
     * 2.1: data中有tbname字段, 例如：data: [ts, f1, f2, f3, t1, t2, tbname] tbColumn: [ts, f1, f2, t1] =&gt; insert into tbname using stb1 tags
     * (t1) values(ts, f1, f2)
     * 2.2: data中没有tbname字段，例如：data: [ts, f1, f2, f3, t1, t2] tbColumn: [ts, f1, f2, t1] =&gt; schemaless: stb1,t1=t1 f1=f1,f2=f2 ts, 没有批量写
     * 3. 对于tb，拼sql，例如：data: [ts, f1, f2, f3, t1, t2] tbColumn: [ts, f1, f2, t1] =&gt; insert into tb(ts, f1, f2) values(ts, f1, f2)
     * 4. 对于t，拼sql，例如：data: [ts, f1, f2, f3, t1, t2] tbColumn: [ts, f1, f2, f3, t1, t2] insert into t(ts, f1, f2, f3, t1, t2) values(ts, f1, f2, f3, t1, t2)
     *
     * @param conn {@link Connection}
     * @param recordBatch list of {@link Record}
     * @return affectedRows
     */
    public int writeBatch(Connection conn, List<Record> recordBatch)
    {
        int affectedRows = 0;
        for (String table : tables) {
            TableMeta tableMeta = tableMetas.get(table);
            switch (tableMeta.tableType) {
                case SUP_TABLE: {
                    if (columns.contains("tbname")) {
                        affectedRows += writeBatchToSupTableBySQL(conn, table, recordBatch);
                    }
                    else {
                        affectedRows += writeBatchToSupTableBySchemaless(conn, table, recordBatch);
                    }
                }
                break;
                case SUB_TABLE:
                    affectedRows += writeBatchToSubTable(conn, table, recordBatch);
                    break;
                case NML_TABLE:
                default:
                    affectedRows += writeBatchToNormalTable(conn, table, recordBatch);
            }
        }
        return affectedRows;
    }

    /**
     * insert into record[idx(tbname)] using table tags(record[idx(t1)]) (ts, f1, f2, f3) values(record[idx(ts)], record[idx(f1)], )
     * record[idx(tbname)] using table tags(record[idx(t1)]) (ts, f1, f2, f3) values(record[idx(ts)], record[idx(f1)], )
     * record[idx(tbname)] using table tags(record[idx(t1)]) (ts, f1, f2, f3) values(record[idx(ts)], record[idx(f1)], )
     *
     * @param conn {@link Connection}
     * @param table {@link String}
     * @param recordBatch list of {@link Record}
     * @return {@link Integer}
     */
    private int writeBatchToSupTableBySQL(Connection conn, String table, List<Record> recordBatch)
    {
        List<ColumnMeta> columnMetas = this.columnMetas.get(table);

        StringBuilder sb = new StringBuilder("insert into");
        for (Record record : recordBatch) {
            sb.append(" ").append(record.getColumn(indexOf("tbname")).asString())
                    .append(" using ").append(table)
                    .append(" tags")
                    .append(columnMetas.stream().filter(colMeta -> columns.contains(colMeta.field)).filter(colMeta -> {
                        return colMeta.isTag;
                    }).map(colMeta -> {
                        return buildColumnValue(colMeta, record);
                    }).collect(Collectors.joining(",", "(", ")")))
                    .append(" ")
                    .append(columnMetas.stream().filter(colMeta -> columns.contains(colMeta.field)).filter(colMeta -> {
                        return !colMeta.isTag;
                    }).map(colMeta -> {
                        return colMeta.field;
                    }).collect(Collectors.joining(",", "(", ")")))
                    .append(" values")
                    .append(columnMetas.stream().filter(colMeta -> columns.contains(colMeta.field)).filter(colMeta -> {
                        return !colMeta.isTag;
                    }).map(colMeta -> {
                        return buildColumnValue(colMeta, record);
                    }).collect(Collectors.joining(",", "(", ")")));
        }
        String sql = sb.toString();

        return executeUpdate(conn, sql);
    }

    private int executeUpdate(Connection conn, String sql)
            throws AddaxException
    {
        int count;
        try (Statement stmt = conn.createStatement()) {
            LOG.debug(">>> " + sql);
            count = stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(TDengineWriterErrorCode.RUNTIME_EXCEPTION, e.getMessage());
        }
        return count;
    }

    private String buildColumnValue(ColumnMeta colMeta, Record record)
    {
        Column column = record.getColumn(indexOf(colMeta.field));
        TimestampPrecision timestampPrecision = schemaManager.loadDatabasePrecision();
        switch (column.getType()) {
            case DATE: {
                Date value = column.asDate();
                switch (timestampPrecision) {
                    case MILLISEC:
                        return "" + (value.getTime());
                    case MICROSEC:
                        return "" + (value.getTime() * 1000);
                    case NANOSEC:
                        return "" + (value.getTime() * 1000_000);
                    default:
                        return "'" + column.asString() + "'";
                }
            }
            case BYTES:
            case STRING:
                if (colMeta.type.equals("TIMESTAMP")) {
                    return "\"" + column.asString() + "\"";
                }
                String value = column.asString();
                return "\'" + Utils.escapeSingleQuota(value) + "\'";
            case NULL:
            case BAD:
                return "NULL";
            case BOOL:
            case DOUBLE:
            case INT:
            case LONG:
            default:
                return column.asString();
        }
    }

    /**
     * table: ["stb1"], column: ["ts", "f1", "f2", "t1"]
     * data: [ts, f1, f2, f3, t1, t2] tbColumn: [ts, f1, f2, t1] =&gt; schemaless: stb1,t1=t1 f1=f1,f2=f2 ts
     *
     * @param conn {@link Connection}
     * @param table {@link String}
     * @param recordBatch list of {@link Record}
     * @return {@link Integer}
     */
    private int writeBatchToSupTableBySchemaless(Connection conn, String table, List<Record> recordBatch)
    {
        int count = 0;
        TimestampPrecision timestampPrecision = schemaManager.loadDatabasePrecision();

        List<ColumnMeta> columnMetaList = this.columnMetas.get(table);
        ColumnMeta ts = columnMetaList.stream().filter(colMeta -> colMeta.isPrimaryKey).findFirst().get();

        List<String> lines = new ArrayList<>();
        for (Record record : recordBatch) {
            StringBuilder sb = new StringBuilder();
            sb.append(table).append(",")
                    .append(columnMetaList.stream().filter(colMeta -> columns.contains(colMeta.field)).filter(colMeta -> {
                        return colMeta.isTag;
                    }).map(colMeta -> {
                        String value = record.getColumn(indexOf(colMeta.field)).asString();
                        if (value.contains(" ")) {
                            value = value.replace(" ", "\\ ");
                        }
                        return colMeta.field + "=" + value;
                    }).collect(Collectors.joining(",")))
                    .append(" ")
                    .append(columnMetaList.stream().filter(colMeta -> columns.contains(colMeta.field)).filter(colMeta -> {
                        return !colMeta.isTag && !colMeta.isPrimaryKey;
                    }).map(colMeta -> {
                        return colMeta.field + "=" + buildSchemalessColumnValue(colMeta, record);
//                        return colMeta.field + "=" + record.getColumn(indexOf(colMeta.field)).asString();
                    }).collect(Collectors.joining(",")))
                    .append(" ");
            // timestamp
            Column column = record.getColumn(indexOf(ts.field));
            Object tsValue = column.getRawData();
            if (column.getType() == Column.Type.DATE && tsValue instanceof Date) {
                long time = column.asDate().getTime();
                switch (timestampPrecision) {
                    case NANOSEC:
                        sb.append(time * 1000000);
                        break;
                    case MICROSEC:
                        sb.append(time * 1000);
                        break;
                    case MILLISEC:
                    default:
                        sb.append(time);
                }
            }
            else if (column.getType() == Column.Type.STRING) {
                sb.append(Utils.parseTimestamp(column.asString()));
            }
            else {
                sb.append(column.asLong());
            }
            String line = sb.toString();
            LOG.debug(">>> " + line);
            lines.add(line);
            count++;
        }

        SchemalessWriter writer = new SchemalessWriter(conn);
        SchemalessTimestampType timestampType;
        switch (timestampPrecision) {
            case NANOSEC:
                timestampType = SchemalessTimestampType.NANO_SECONDS;
                break;
            case MICROSEC:
                timestampType = SchemalessTimestampType.MICRO_SECONDS;
                break;
            case MILLISEC:
                timestampType = SchemalessTimestampType.MILLI_SECONDS;
                break;
            default:
                timestampType = SchemalessTimestampType.NOT_CONFIGURED;
        }
        try {
            writer.write(lines, SchemalessProtocolType.LINE, timestampType);
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(TDengineWriterErrorCode.RUNTIME_EXCEPTION, e.getMessage());
        }

        LOG.warn("schemalessWriter does not return affected rows!");
        return count;
    }

    private long dateAsLong(Column column)
    {
        TimestampPrecision timestampPrecision = schemaManager.loadDatabasePrecision();
        long time = column.asDate().getTime();
        switch (timestampPrecision) {
            case NANOSEC:
                return time * 1000000;
            case MICROSEC:
                return time * 1000;
            case MILLISEC:
            default:
                return time;
        }
    }

    private String buildSchemalessColumnValue(ColumnMeta colMeta, Record record)
    {
        Column column = record.getColumn(indexOf(colMeta.field));
        switch (column.getType()) {
            case DATE:
                if (colMeta.type.equals("TIMESTAMP")) {
                    return dateAsLong(column) + "i64";
                }
                return "L'" + column.asString() + "'";
            case NULL:
            case BAD:
                return "NULL";
            case DOUBLE: {
                if (colMeta.type.equals("FLOAT")) {
                    return column.asString() + "f32";
                }
                if (colMeta.type.equals("DOUBLE")) {
                    return column.asString() + "f64";
                }
            }
            case INT:
            case LONG: {
                if (colMeta.type.equals("TINYINT")) {
                    return column.asString() + "i8";
                }
                if (colMeta.type.equals("SMALLINT")) {
                    return column.asString() + "i16";
                }
                if (colMeta.type.equals("INT")) {
                    return column.asString() + "i32";
                }
                if (colMeta.type.equals("BIGINT")) {
                    return column.asString() + "i64";
                }
            }
            case BYTES:
            case STRING:
                if (colMeta.type.equals("TIMESTAMP")) {
                    return column.asString() + "i64";
                }
                String value = column.asString();
                value = value.replace("\"", "\\\"");
                if (colMeta.type.startsWith("BINARY")) {
                    return "\"" + value + "\"";
                }
                if (colMeta.type.startsWith("NCHAR")) {
                    return "L\"" + value + "\"";
                }
            case BOOL:
            default:
                return column.asString();
        }
    }

    /**
     * table: ["tb1"], column: [tbname, ts, f1, f2, t1]
     * if contains("tbname") and tbname != tb1 continue;
     * else if t1 != record[idx(t1)] or t2 != record[idx(t2)]... continue;
     * else
     * insert into tb1 (ts, f1, f2) values( record[idx(ts)], record[idx(f1)], record[idx(f2)])
     *
     * @param conn {@link Connection}
     * @param table {@link String}
     * @param recordBatch list of {@link Record}
     * <p>
     * @return {@link Integer}
     */
    private int writeBatchToSubTable(Connection conn, String table, List<Record> recordBatch)
    {
        List<ColumnMeta> columnMetas = this.columnMetas.get(table);

        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(table).append(" ")
                .append(columnMetas.stream().filter(colMeta -> columns.contains(colMeta.field)).filter(colMeta -> {
                    return !colMeta.isTag;
                }).map(colMeta -> {
                    return colMeta.field;
                }).collect(Collectors.joining(",", "(", ")")))
                .append(" values");
        int validRecords = 0;
        for (Record record : recordBatch) {
            if (columns.contains("tbname") && !table.equals(record.getColumn(indexOf("tbname")).asString())) {
                continue;
            }

            boolean tagsAllMatch = columnMetas.stream().filter(colMeta -> columns.contains(colMeta.field)).filter(colMeta -> {
                return colMeta.isTag;
            }).allMatch(colMeta -> {
                Column column = record.getColumn(indexOf(colMeta.field));
                boolean equals = equals(column, colMeta);
                return equals;
            });

            if (ignoreTagsUnmatched && !tagsAllMatch) {
                continue;
            }

            sb.append(columnMetas.stream().filter(colMeta -> columns.contains(colMeta.field)).filter(colMeta -> {
                return !colMeta.isTag;
            }).map(colMeta -> {
                return buildColumnValue(colMeta, record);
            }).collect(Collectors.joining(", ", "(", ") ")));
            validRecords++;
        }

        if (validRecords == 0) {
            LOG.warn("no valid records in this batch");
            return 0;
        }

        String sql = sb.toString();
        return executeUpdate(conn, sql);
    }

    private boolean equals(Column column, ColumnMeta colMeta)
    {
        switch (column.getType()) {
            case BOOL:
                return column.asBoolean().equals(Boolean.valueOf(colMeta.value.toString()));
            case INT:
            case LONG:
                return column.asLong().equals(Long.valueOf(colMeta.value.toString()));
            case DOUBLE:
                return column.asDouble().equals(Double.valueOf(colMeta.value.toString()));
            case NULL:
                return colMeta.value == null;
            case DATE:
                return column.asDate().getTime() == ((Timestamp) colMeta.value).getTime();
            case BAD:
            case BYTES:
                return Arrays.equals(column.asBytes(), (byte[]) colMeta.value);
            case STRING:
            default:
                return column.asString().equals(colMeta.value.toString());
        }
    }

    /**
     * table: ["weather"], column: ["ts, f1, f2, f3, t1, t2"]
     * sql: insert into weather (ts, f1, f2, f3, t1, t2) values( record[idx(ts), record[idx(f1)], ...)
     *
     * @param conn ${@link Connection}
     * @param table ${@link String}
     * @param recordBatch list of ${@link Record}
     * <p>
     * @return {@link Integer}
     */
    private int writeBatchToNormalTable(Connection conn, String table, List<Record> recordBatch)
    {
        List<ColumnMeta> columnMetas = this.columnMetas.get(table);

        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(table)
                .append(" ")
                .append(columnMetas.stream().filter(colMeta -> columns.contains(colMeta.field)).map(colMeta -> {
                    return colMeta.field;
                }).collect(Collectors.joining(",", "(", ")")))
                .append(" values ");

        for (Record record : recordBatch) {
            sb.append(columnMetas.stream().filter(colMeta -> columns.contains(colMeta.field)).map(colMeta -> {
                return buildColumnValue(colMeta, record);
            }).collect(Collectors.joining(",", "(", ")")));
        }

        String sql = sb.toString();
        return executeUpdate(conn, sql);
    }

    private int indexOf(String colName)
            throws AddaxException
    {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equals(colName)) {
                return i;
            }
        }
        throw AddaxException.asAddaxException(TDengineWriterErrorCode.RUNTIME_EXCEPTION,
                "cannot find col: " + colName + " in columns: " + columns);
    }
}