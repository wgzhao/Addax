# DatabendWriter

Databend plugin is used to write data to [Databend](https://databend.rs/zh-CN/doc/) database via JDBC.

Databend is a database backend compatible with MySQL protocol, so Databend writing can use [MySQLWriter](../../writer/mysqlwriter) for access.

## Example

Assume the table to be written has the following DDL statement:

```sql
CREATE DATABASE example_db;
CREATE TABLE `example_db`.`table1`
(
    `siteid`   INT DEFAULT CAST(10 AS INT),
    `citycode` INT,
    `username` VARCHAR,
    `pv`       BIGINT
);
```

The following configures a configuration file to read data from memory and write to databend table:

```json
--8<-- "jobs/databendwriter.json"
```

Save the above configuration file as `job/stream2databend.json`

Execute the following command:

```shell
bin/addax.sh job/stream2Databend.json
```

## Parameters

This plugin is based on [RDBMS Writer](../rdbmswriter), so you can refer to all configuration items of RDBMS Writer, and adds the following configuration items:

| Configuration    | Required | Type   | Default Value | Description                                                                      |
| :--------------- | :------: | ------ | ------------- | -------------------------------------------------------------------------------- |
| writeMode        | No       | string | `insert`      | Write mode, supports `insert` and `replace` modes                               |
| onConflictColumn | No       | string | None          | Conflict column, when writeMode is `replace`, must specify conflict column, otherwise write will fail |

### writeMode

Used to support Databend's `replace into` syntax. When this parameter is set to `replace`, the `onConflictColumn` parameter must also be specified to determine whether data is inserted or updated.

Example of both parameters:

```json
{
  "writeMode": "replace",
  "onConflictColumn": [
    "id"
  ]
}
```