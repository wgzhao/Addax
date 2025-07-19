# Hive Reader

Hive Reader plugin implements the ability to read data from [Apache Hive](https://hive.apache.org) database.

The main purpose of adding this plugin is to solve the problem of Kerberos authentication when using [RDBMS Reader][1] plugin to read Hive database. If your Hive database does not have Kerberos authentication enabled, you can directly use [RDBMS Reader][1]. If Kerberos authentication is enabled, you can use this plugin.

## Example

We create the following table in Hive's test database and insert a record:

```sql
--8<-- "sql/hive.sql"
```

The following configuration reads this table to terminal:

=== "job/hive2stream.json"

```json
--8<-- "jobs/hivereader.json"
```

Save the above configuration file as `job/hive2stream.json`

### Execute Collection Command

Execute the following command for data collection

```shell
bin/addax.sh job/hive2stream.json
```

## Parameters

| Configuration | Required | Type   | Default Value | Description                                                              |
| :------------ | :------: | ------ | ------------- | ------------------------------------------------------------------------ |
| jdbcUrl       | Yes      | list   | None          | JDBC connection information of target database                           |
| driver        | No       | string | None          | Custom driver class name to solve compatibility issues, see description below |
| username      | Yes      | string | None          | Username of data source                                                  |
| password      | No       | string | None          | Password for specified username of data source, can be omitted if no password |

[1]: ../rdbmsreader