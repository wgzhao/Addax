# RDBMS Writer

RDBMS Writer plugin supports writing data to traditional RDBMS. This is a generic relational database writer plugin that can support more relational database writing by registering database drivers.

At the same time, RDBMS Writer is also the base class for other relational database writer plugins. The following writer plugins all depend on this plugin:

- [Oracle Writer](../oraclewriter)
- [MySQL Writer](../mysqlwriter)
- [PostgreSQL Writer](../postgresqlwriter)
- [ClickHouse Writer](../clickhousewriter)
- [SQLServer Writer](../sqlserverwriter)
- [Access Writer](../accesswriter)
- [Databend Writer](../databendwriter)

Note: If a dedicated database writer plugin is already provided, it is recommended to use the dedicated plugin. If the database you need to write to does not have a dedicated plugin, consider using this generic plugin. Before use, you need to perform the following operations to run normally, otherwise exceptions will occur.

## Configure Driver

Suppose you need to write data to IBM DB2. Since no dedicated writer plugin is provided, we can use this plugin to implement it. Before use, you need to perform the following two operations:

1. Download the corresponding JDBC driver and copy it to the `plugin/writer/rdbmswriter/libs` directory
2. Modify the task configuration file, find the `driver` item, and fill in the correct JDBC driver name, such as DB2's driver name `com.ibm.db2.jcc.DB2Driver`

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

Configure a job to write to RDBMS.

```json
--8<-- "jobs/rdbmswriter.json"
```

## Parameters

This plugin provides configuration for writing to relational databases. For detailed parameter descriptions, please refer to the original RDBMS Writer documentation.