# iotdbreader

Reads time-series rows from Apache IoTDB through the IoTDB JDBC driver.

## Supported versions

- IoTDB 2.x: tree model and table model.
- IoTDB 1.3.7+: tree model only (the 2.0.x client is used; this is best effort, not a vendor-supported combination).
- IoTDB 1.0-1.2 is not supported.

Timestamps are handled in milliseconds, which matches the IoTDB default precision. Servers configured
with microsecond or nanosecond precision are not supported yet.

## Connection

| key | description |
|---|---|
| `username` | IoTDB user, for example `root` |
| `password` | password, may be empty |
| `connection.jdbcUrl` | `jdbc:iotdb://host:6667/`; add a database path or `?sql_dialect=table` when needed |
| `sqlDialect` | `tree` (default) or `table`; if omitted it is inferred from `sql_dialect` in the URL |
| `fetchSize` | JDBC fetch size, default 10000 |

## Tree model

| key | description |
|---|---|
| `device` | device path, for example `root.factory.device1` |
| `measurements` | measurement names, or `["*"]` |
| `where` | optional time/series filter |
| `timeColumnPosition` | position of the implicit time column in the output record, default 0 |

The result always contains the time column even when it is not listed in `SELECT`; it is inserted at
`timeColumnPosition`. `querySqls` (or `querySql`) can be used instead of `device`/`measurements`:

```json
"connection": {
  "jdbcUrl": "jdbc:iotdb://127.0.0.1:6667/",
  "querySqls": [
    "SELECT s1,s2 FROM root.demo.d1 WHERE time >= 0 AND time < 2000",
    "SELECT s1,s2 FROM root.demo.d1 WHERE time >= 2000 AND time < 4000"
  ]
}
```

## Table model

| key | description |
|---|---|
| `table` | `database.table` |
| `column` | columns to select, defaults to `["*"]`; include `time` when the writer needs it |
| `where` | optional filter |

`timeColumnPosition` is not used in table mode; the time column stays where the `SELECT` puts it.

## Time range split

`beginDateTime`, `endDateTime` and `splitInterval` (for example `1d`, `6h`, `30m`, `10s`, `500ms`)
split the read into independent tasks:

```json
"beginDateTime": "2025-01-01 00:00:00",
"endDateTime": "2025-01-02 00:00:00",
"splitInterval": "1h"
```

The parameters must be configured together. A split cannot be combined with `querySqls`; use one or
the other. Epoch milliseconds are also accepted for the two date bounds.

## Examples

Tree model to a text file:

```json
{
  "reader": {
    "name": "iotdbreader",
    "parameter": {
      "username": "root",
      "password": "root",
      "connection": { "jdbcUrl": "jdbc:iotdb://127.0.0.1:6667/" },
      "device": "root.demo.d1",
      "measurements": [ "s1", "s2" ],
      "timeColumnPosition": 0
    }
  }
}
```

Table model to a text file:

```json
{
  "reader": {
    "name": "iotdbreader",
    "parameter": {
      "username": "root",
      "password": "root",
      "connection": { "jdbcUrl": "jdbc:iotdb://127.0.0.1:6667/" },
      "sqlDialect": "table",
      "table": "demo.metrics",
      "column": [ "time", "device_id", "temperature" ]
    }
  }
}
```
