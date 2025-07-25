# ClickHouse Writer

ClickHouse Writer plugin is used to write data to [ClickHouse](https://clickhouse.tech).

## Example

The following example demonstrates reading content from one table in ClickHouse and writing it to another table with the same table structure, to test the data structures supported by the plugin.

### Table Structure and Data

Assume the table structure and data to be read are as follows:

```sql
--8<-- "sql/clickhouse.sql"
```

The table to be written uses the same structure as the read table, with the following DDL statement:

```sql
create table ck_addax_writer as ck_addax;
```

## Configuration

The following is the configuration file

=== "job/clickhouse2clickhouse.json"

  ```json
  --8<-- "jobs/clickhousewriter.json"
  ```

Save the above configuration file as `job/clickhouse2clickhouse.json`

### Execute Collection Command

Execute the following command for data collection

```shell
bin/addax.sh job/clickhouse2clickhouse.json
```

## Parameters

This plugin is based on [RDBMS Writer](../rdbmswriter), so you can refer to all configuration items of RDBMS Writer.