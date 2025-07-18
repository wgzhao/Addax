# SQLite Writer

SQLite Writer plugin implements the functionality of writing data to [SQLite](https://sqlite.org/index.html) database.

## Example

Assume the table to be written is as follows:

```sql
create table addax_tbl
(
    col1 varchar(20) ,
    col2 int(4),
    col3 datetime,
    col4 boolean,
    col5 binary
);
```

Here we use data generated from memory to SQLite.

=== "job/stream2sqlite.json"

```json
--8<-- "jobs/sqlitewriter.json"
```

Save the above configuration file as `job/stream2sqlite.json`

### Execute Collection Command

Execute the following command for data collection

```shell
bin/addax.sh job/stream2sqlite.json
```

## Parameters

This plugin is based on [RDBMS Writer](../rdbmswriter), so you can refer to all configuration items of RDBMS Writer. Since SQLite connection does not require username and password, the `username` and `password` that other database writer plugins need to configure are not needed here.

### writeMode

- `insert` means using `insert into`
- `replace` means using `replace into` method
- `update` means using `ON DUPLICATE KEY UPDATE` statement

## Type Conversion

| Addax Internal Type | SQLite Data Type |
| ------------------- | ---------------- |
| Long                | integer          |
| Double              | real             |
| String              | varchar          |
| Date                | datetime         |
| Boolean             | bool             |
| Bytes               | blob, binary     |