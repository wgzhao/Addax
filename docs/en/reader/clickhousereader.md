# ClickHouse Reader

`ClickHouseReader` plugin supports reading data from [ClickHouse](https://clickhouse.tech) database.

## Example

### Table Structure and Data Information

Assume the table structure and data to be read are as follows:

```sql
--8<-- "sql/clickhouse.sql"
```

## Configure JSON File

The following configuration file reads specified table data from ClickHouse database and prints to terminal

=== "job/clickhouse2stream.json"

```json
--8<-- "jobs/clickhousereader.json"
```

Save the above configuration file as `job/clickhouse2stream.json`

### Execute Collection Command

Execute the following command for data collection

```shell
bin/addax.sh job/clickhouse2stream.json
```

The output information is as follows (non-critical information removed):

```
--8<-- "output/clickhousereader.txt"
```

## Parameters

This plugin is based on [RDBMS Reader](../rdbmsreader), so you can refer to all parameters of RDBMS Reader.

## Supported Data Types

| Addax Internal Type | ClickHouse Data Type                                                    |
| ------------------- | ----------------------------------------------------------------------- |
| Long                | Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64, Enum8, Enum16 |
| Double              | Float32, Float64, Decimal                                               |
| String              | String, FixedString(N)                                                  |
| Date                | Date, DateTime, DateTime64                                              |
| Boolean             | UInt8                                                                   |
| Bytes               | String                                                                  |

## Limitations

Except for the above listed field types, other types are not supported, such as Array, Nested, etc.