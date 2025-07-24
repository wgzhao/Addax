# RDBMS Reader

RDBMS Reader plugin supports reading data from traditional RDBMS. This is a generic relational database reading plugin that can support more relational database reading by registering database drivers.

At the same time, RDBMS Reader is also the base class for other relational database reading plugins. The following reading plugins all depend on this plugin:

- [Oracle Reader](../oraclereader)
- [MySQL Reader](../mysqlreader)
- [PostgreSQL Reader](../postgresqlreader)
- [ClickHouse Reader](../clickhousereader)
- [SQLServer Reader](../sqlserverreader)
- [Access Reader](../accessreader)
- [Databend Reader](../databendreader)

Note: If a dedicated database reading plugin is already provided, it is recommended to use the dedicated plugin. If the database you need to read does not have a dedicated plugin, consider using this generic plugin. Before use, you need to perform the following operations to run normally, otherwise exceptions will occur.

## Configure Driver

Suppose you need to read data from IBM DB2. Since no dedicated reading plugin is provided, we can use this plugin to implement it. Before use, you need to download the corresponding JDBC driver and copy it to the `plugin/reader/rdbmsreader/libs` directory.
If your driver class name is special, you need to find the `driver` item in the task configuration file and fill in the correct JDBC driver name, such as DB2's driver name `com.ibm.db2.jcc.DB2Driver`. If not filled, the plugin will automatically guess the driver name.

The following lists common databases and their corresponding driver names:

- [Apache Impala](http://impala.apache.org/): `com.cloudera.impala.jdbc41.Driver`
- [Enterprise DB](https://www.enterprisedb.com/): `com.edb.Driver`
- [PrestoDB](https://prestodb.io/): `com.facebook.presto.jdbc.PrestoDriver`
- [IBM DB2](https://www.ibm.com/analytics/db2): `com.ibm.db2.jcc.DB2Driver`
- [MySQL](https://www.mysql.com): `com.mysql.cj.jdbc.Driver`
- [Sybase Server](https://www.sap.com/products/sybase-ase.html): `com.sybase.jdbc3.jdbc.SybDriver`
- [TDengine](https://www.taosdata.com/cn/): `com.taosdata.jdbc.TSDBDriver`
- [达梦数据库](https://www.dameng.com/): `dm.jdbc.driver.DmDriver`
- [星环Inceptor](http://transwarp.io/): `io.transwarp.jdbc.InceptorDriver`
- [TrinoDB](https://trino.io): `io.trino.jdbc.TrinoDriver`
- [PrestoSQL](https://trino.io): `io.prestosql.jdbc.PrestoDriver`
- [Oracle DB](https://www.oracle.com/database/): `oracle.jdbc.OracleDriver`
- [PostgreSQL](https://postgresql.org): `org.postgresql.Drive`

## Configuration

The following configuration shows how to read data from Presto database to terminal

=== "job/rdbms2stream.json"

  ```json
  --8<-- "jobs/rdbmsreader.json"
  ```

## Parameters

| Configuration | Required | Data Type | Default Value | Description                                                      |
| :------------ | :------: | --------- | ------------- | ---------------------------------------------------------------- |
| jdbcUrl       | Yes      | list      | None          | JDBC connection information of the target database, jdbcUrl follows RDBMS official specifications and can include connection attachment control information |
| driver        | No       | string    | None          | Custom driver class name to solve compatibility issues, see description below |
| username      | Yes      | string    | None          | Username of the data source                                      |
| password      | No       | string    | None          | Password for the specified username of the data source          |
| table         | Yes      | list      | None          | Selected table names to be synchronized, using JSON data format. When configured for multiple tables, users need to ensure that multiple tables have the same table structure |
| column        | Yes      | list      | None          | Collection of column names to be synchronized in the configured table, detailed description below |
| splitPk       | No       | string    | None          | Use the field represented by splitPk for data sharding, which can greatly improve data synchronization efficiency, see notes below |
| autoPk        | No       | boolean   | false         | Whether to automatically guess the sharding primary key, introduced in version `3.2.6`, see description below |
| where         | No       | string    | None          | Filtering conditions for the table                               |
| session       | No       | list      | None          | For local connections, modify session configuration, see below   |
| querySql      | No       | string    | None          | Use custom SQL instead of specified table to get data. When this item is configured, `table` and `column` configuration items are ignored |
| fetchSize     | No       | int       | 1024          | Defines the number of batch data fetched between plugin and database server each time. Increasing this value may cause Addax OOM |
| excludeColumn | No       | list      | None          | Column name fields to be excluded, only valid when `column` is configured as `*` |

### jdbcUrl

In addition to configuring necessary information, `jdbcUrl` configuration can also add specific configuration properties for each specific driver. Here we particularly mention that we can use configuration properties to support proxies to access databases through proxies. For example, for PrestoSQL database JDBC driver, it supports the `socksProxy` parameter, so the above configured `jdbcUrl` can be modified to:

`jdbc:presto://127.0.0.1:8080/hive?socksProxy=192.168.1.101:1081`

Most relational database JDBC drivers support `socksProxyHost,socksProxyPort` parameters for proxy access. There are also some special cases.

The following are the proxy types and configuration methods supported by various database JDBC drivers:

| Database | Proxy Type | Proxy Configuration           | Example                                            |
| -------- | ---------- | ----------------------------- | -------------------------------------------------- |
| MySQL    | socks      | socksProxyHost,socksProxyPort | `socksProxyHost=192.168.1.101&socksProxyPort=1081` |
| Presto   | socks      | socksProxy                    | `socksProxy=192.168.1.101:1081`                    |
| Presto   | http       | httpProxy                     | `httpProxy=192.168.1.101:3128`                     |

### driver

In most cases, the JDBC driver for a database is fixed, but some have different recommended driver class names due to different versions, such as MySQL. The new MySQL JDBC driver type recommends using `com.mysql.cj.jdbc.Driver` instead of the previous `com.mysql.jdbc.Driver`. If you want to use the old driver name, you can configure the `driver` configuration item. Otherwise, the plugin will automatically guess the driver name based on the string in `jdbcUrl`.

### column

Collection of column names to be synchronized in the configured table, using JSON array to describe field information. Users use `*` to represent default use of all column configurations, such as `["*"]`.

Supports column pruning, i.e., columns can be selected for partial export.

Supports column reordering, i.e., columns can be exported not according to table schema information.

Supports constant configuration, users need to follow JSON format:

``["id", "`table`", "1", "'bazhen.csy'", "null", "to_char(a + 1)", "2.3" , "true"]``

- `id` is ordinary column name
- `` `table` `` is column name containing reserved words
- `1` is integer constant
- `'bazhen.csy'` is string constant
- `null` is null pointer. Note that `null` here must appear as a string, i.e., quoted with double quotes
- `to_char(a + 1)` is expression
- `2.3` is floating point number
- `true` is boolean value. Similarly, boolean values here must also be quoted with double quotes

Column must be explicitly filled and cannot be empty!

### excludeColumn

There is a situation where we need to read most fields of a table. If the table has many fields, configuring `column` is obviously time-consuming.
In particular, when we collect business data to big data platforms, we generally add some additional fields including partition fields and collection information. When we need to write back to business data tables, we need to exclude these fields.
Under this consideration, we introduced the `excludeColumn` configuration item. When `column` is configured as `*`, the `excludeColumn` configuration item takes effect to exclude some fields.

For example:

```json
{
  "column": ["*"],
  "excludeColumn": ["partition_col", "etl_time"]
}
```