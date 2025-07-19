# Oracle Writer

Oracle Writer plugin implements the functionality of writing data to Oracle destination tables.

## Configuration Example

Here we use data generated from memory to import into Oracle.

=== "job/stream2oracle.json"

  ```json
  --8<-- "jobs/oraclewriter.json"
  ```

## Parameters

This plugin is based on [RDBMS Writer](../rdbmswriter), so you can refer to all configuration items of RDBMS Writer, and adds some OracleWriter-specific configuration items.

| Configuration | Required | Default Value | Description                                         |
| :------------ | :------: | ------------- | --------------------------------------------------- |
| writeMode     | No       | insert        | Write mode, supports insert, update, see below     |

### writeMode

By default, `insert into` syntax is used to write to Oracle tables. If you want to use the mode of updating when primary key exists and inserting when it doesn't exist, which is Oracle's `merge into` syntax, you can use `update` mode. Assuming the table's primary key is `id`, the `writeMode` configuration method is as follows:

```json
"writeMode": "update(id)"
```

If it's a composite unique index, the configuration method is as follows:

```json
"writeMode": "update(col1, col2)"
```