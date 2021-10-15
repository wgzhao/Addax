Addax supports the following Data Sources:

| database/filesystem | read   |  write | plugin(reader/writer)                  | memo                            |
| ------------------- | ------ | ------ | -------------------------------------- | ------------------------------- |
| Cassandra           | ✓      | ✓     | cassandrareader/cassandrawriter         |                                 |
| ClickHouse          | ✓      | ✓     | clickhousereader/clickhousewriter       |                                 |
| DB2                 | ✓      | ✓     | rbdmsreader/rdbmswriter                 | not fully tested                |
| DBF                 | ✓      | ✓     | dbfreader/dbfwriter                     |                                 |
| ElasticSearch       | ✓      | ✓     | elasticsearchreader/elasticsearchwriter | originally from [@Kestrong][1]  |
| Excel               | ✓      | ✓     | excelreader/excelwriter                 |                                 |
| FTP                 | ✓      | ✓     | ftpreader/ftpwriter                     |                                 |
| HBase 1.x(API)      | ✓      | ✓     | hbase11xreader/hbase11xwriter           | use HBASE API                   |
| HBase 1.x(SQL)      | ✓      | ✓     | hbase11xsqlreader/hbase11xsqlwriter     | use Phoenix[Phoenix][2]         |
| HBase 2.x(API)      | ✓      | x     | hbase20xreader                          | use HBase API                   |
| HBase 2.x(SQL0      | ✓      | ✓     | hbase20xsqlreader/hbase20xsqlwriter     | via [Phoenix][2]                |
| HDFS                | ✓      | ✓     | hdfsreader/hdfswriter                   | support HDFS 2.0 or later       |
| Hive                | ✓      | x     | hivereader                              |                                 |
| HTTP                | ✓      | x     | httpreader                              | support RestFul API             |
| Greenplum           | ✓      | ✓     | postgresqlreader/greenplumwriter        |                                 |
| InfluxDB            | ✓      | ✓     | influxdbreader/influxdbwriter           | ONLY support InfluxDB 1.x       |
| json                | ✓      | x     | jsonfilereader                          |                                 |
| kudu                | ✓      | ✓     | kudureader/kuduwriter                   |                                 |
| MongoDB             | ✓      | ✓     | mongodbreader/mongodbwriter             |                                 |
| MySQL/MariaDB       | ✓      | ✓     | mysqlreader/mysqlwriter                 |                                 |
| Oracle              | ✓      | ✓     | oraclereader/oraclewriter               |                                 |
| PostgreSQL          | ✓      | ✓     | postgresqlreader/postgresqlwriter       |                                 |
| Trino               | ✓      | ✓     | rdbmsreader/rdbmswriter                 | [trino][3]                      |
| Redis               | ✓      | ✓     | redisreader/rediswriter                 |                                 |
| SQLite              | ✓      | ✓     | sqlitereader/sqlitewriter               |                                 |
| SQL Server          | ✓      | ✓     | sqlserverreader/sqlserverwriter         |                                 |
| TDengine            | ✓      | ✓     | tdenginereader/tdenginewriter           | [TDengine][4]                   |
| TDH Inceptor2       | ✓      | ✓     | rdbmsreader/rdbmswriter                 | [Transwarp TDH][5] 5.1 or later |
| TEXT                | ✓      | ✓     | textfilereader/textfilewriter           |                                 |

[1]: https://github.com/Kestrong/datax-elasticsearch
[2]: https://phoenix.apache.org
[3]: https://trino.io
[4]: https://www.taosdata.com/cn/
[5]: http://transwarp.cn/
