# iotdbwriter

Writes Addax records to Apache IoTDB. The tree model is written through the IoTDB Session API; the
table model is written through JDBC batches.

## Supported versions

- IoTDB 2.x: tree model and table model.
- IoTDB 1.3.7+: tree model only (the 2.0.x client is used; this is best effort, not a vendor-supported combination).
- IoTDB 1.0-1.2 is not supported.

Timestamps are handled in milliseconds, which matches the IoTDB default precision. Servers configured
with microsecond or nanosecond precision are not supported yet.

## Common parameters

| key | description |
|---|---|
| `username` | IoTDB user, for example `root` |
| `password` | password, may be empty |
| `connection.jdbcUrl` | `jdbc:iotdb://host:6667/` |
| `sqlDialect` | `tree` (default) or `table`; if omitted it is inferred from `sql_dialect` in the URL |
| `timeColumnPosition` | position of the time column in the incoming record, default 0 |
| `batchSize` | records per batch, default 1000 |
| `preSql` | SQL executed once before the tasks start, supports `${table}` / `${device}` |
| `postSql` | SQL executed once after the tasks finish, supports `${table}` / `${device}` |

## Tree model

| key | description |
|---|---|
| `device` | target device path, for example `root.factory.device1` |
| `measurements` | measurement names in record order (the time column is excluded) |
| `columnTypes` | optional `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BOOLEAN`, `TEXT`, `TIMESTAMP`, `BLOB`; by default the type is inferred from the first non-null value of each measurement |

The record must contain `measurements.size() + 1` columns. The time column is taken from
`timeColumnPosition`; the remaining columns map to `measurements` in order. Configure `columnTypes`
when a measurement may be null in the first batch or when the source type must be narrowed (for
example, an Addax `long` column that targets an IoTDB `INT32` measurement).

## Table model

| key | description |
|---|---|
| `table` | target `database.table` |
| `column` | insert column list; must include the time column |

The record must contain exactly `column.size()` columns. The target table must exist before the job
runs unless `preSql` creates it. Column types are read from the table metadata, so a source value is
converted to the target type where possible.

## Examples

Tree model:

```json
{
  "writer": {
    "name": "iotdbwriter",
    "parameter": {
      "username": "root",
      "password": "root",
      "connection": { "jdbcUrl": "jdbc:iotdb://127.0.0.1:6667/" },
      "device": "root.demo.d1",
      "measurements": [ "s1", "s2" ],
      "timeColumnPosition": 0,
      "batchSize": 1000
    }
  }
}
```

Table model:

```json
{
  "writer": {
    "name": "iotdbwriter",
    "parameter": {
      "username": "root",
      "password": "root",
      "connection": { "jdbcUrl": "jdbc:iotdb://127.0.0.1:6667/" },
      "sqlDialect": "table",
      "table": "demo.metrics",
      "column": [ "time", "device_id", "temperature", "humidity" ],
      "timeColumnPosition": 0,
      "batchSize": 1000
    }
  }
}
```
