## Addax offers support for the following data sources

| database/filesystem | read | write | plugin(reader/writer)                   | memo                            |
|---------------------|------|-------|-----------------------------------------|---------------------------------|
| Access              | :white_check_mark:    | :white_check_mark:     | accessreader/accesswriter               | suuport [Access][9]             |
| Cassandra           | :white_check_mark:    | :white_check_mark:     | cassandrareader/cassandrawriter         |                                 |
| ClickHouse          | :white_check_mark:    | :white_check_mark:     | clickhousereader/clickhousewriter       |                                 |
| Databend            | :white_check_mark:    | :white_check_mark:     | databendreader/databendwriter           | support [Databend][8]           |
| DB2                 | :white_check_mark:    | :white_check_mark:     | rbdmsreader/rdbmswriter                 | not fully tested                |
| DBF                 | :white_check_mark:    | :white_check_mark:     | dbfreader/dbfwriter                     |                                 |
| ElasticSearch       | :white_check_mark:    | :white_check_mark:     | elasticsearchreader/elasticsearchwriter | originally from [@Kestrong][1]  |
| Excel               | :white_check_mark:    | :white_check_mark:     | excelreader/excelwriter                 |                                 |
| FTP                 | :white_check_mark:    | :white_check_mark:     | ftpreader/ftpwriter                     |                                 |
| GaussDB             | :white_check_mark:    | :white_check_mark:     | gaussdbreader/gaussdbwriter             |                                 |
| HBase 1.x(API)      | :white_check_mark:    | :white_check_mark:     | hbase11xreader/hbase11xwriter           | use HBASE API                   |
| HBase 1.x(SQL)      | :white_check_mark:    | :white_check_mark:     | hbase11xsqlreader/hbase11xsqlwriter     | use Phoenix[Phoenix][2]         |
| HBase 2.x(API)      | :white_check_mark:    | :x:     | hbase20xreader                          | use HBase API                   |
| HBase 2.x(SQL)      | :white_check_mark:    | :white_check_mark:     | hbase20xsqlreader/hbase20xsqlwriter     | via [Phoenix][2]                |
| HDFS                | :white_check_mark:    | :white_check_mark:     | hdfsreader/hdfswriter                   | support HDFS 2.0 or later       |
| Hive                | :white_check_mark:    | :x:     | hivereader                              |                                 |
| HTTP                | :white_check_mark:    | :x:     | httpreader                              | support RestFul API             |
| Greenplum           | :white_check_mark:    | :white_check_mark:     | postgresqlreader/greenplumwriter        |                                 |
| InfluxDB            | :white_check_mark:    | :white_check_mark:     | influxdbreader/influxdbwriter           | ONLY support InfluxDB 1.x       |
| InfluxDB2           | :white_check_mark:    | :white_check_mark:     | influxdb2reader/influxdb2writer         | ONLY InfluxDB 2.0 or later      |
| json                | :white_check_mark:    | :x:     | jsonfilereader                          |                                 |
| Kafka               | :white_check_mark:    | :white_check_mark:     | kafkareader/kafkawriter                 |                                 |
| kudu                | :white_check_mark:    | :white_check_mark:     | kudureader/kuduwriter                   |                                 |
| MongoDB             | :white_check_mark:    | :white_check_mark:     | mongodbreader/mongodbwriter             |                                 |
| MySQL/MariaDB       | :white_check_mark:    | :white_check_mark:     | mysqlreader/mysqlwriter                 |                                 |
| Oracle              | :white_check_mark:    | :white_check_mark:     | oraclereader/oraclewriter               |                                 |
| PostgreSQL          | :white_check_mark:    | :white_check_mark:     | postgresqlreader/postgresqlwriter       |                                 |
| AWS S3              | :white_check_mark:    | :white_check_mark:     | s3reader/s3writer                       | [AWS S3][6], [MinIO][7]         |
| Trino               | :white_check_mark:    | :white_check_mark:     | rdbmsreader/rdbmswriter                 | [trino][3]                      |
| Redis               | :white_check_mark:    | :white_check_mark:     | redisreader/rediswriter                 |                                 |
| SQLite              | :white_check_mark:    | :white_check_mark:     | sqlitereader/sqlitewriter               |                                 |
| SQL Server          | :white_check_mark:    | :white_check_mark:     | sqlserverreader/sqlserverwriter         |                                 |
| Sybase Anywhere     | :white_check_mark:    | :white_check_mark:     | sybasereader/sybasewriter               |                                 |
| TDengine            | :white_check_mark:    | :white_check_mark:     | tdenginereader/tdenginewriter           | [TDengine][4]                   |
| TDH Inceptor2       | :white_check_mark:    | :white_check_mark:     | rdbmsreader/rdbmswriter                 | [Transwarp TDH][5] 5.1 or later |
| TEXT                | :white_check_mark:    | :white_check_mark:     | textfilereader/textfilewriter           |                                 |

[1]: https://github.com/Kestrong/datax-elasticsearch

[2]: https://phoenix.apache.org

[3]: https://trino.io

[4]: https://www.taosdata.com/cn/

[5]: http://transwarp.cn/

[6]: https://aws.amazon.com/s3

[7]: https://min.io/

[8]: https://databend.rs

[9]: https://en.wikipedia.org/wiki/Microsoft_Access
