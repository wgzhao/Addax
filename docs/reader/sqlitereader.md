# SQLite Reader

SQLiteReader 插件用于读取指定目录下的 sqlite 文件， 他继承于 [rdbmsreader](../rdbmsreader)

## 示例

我们创建示例文件：

```shell
$ sqlite3  /tmp/test.sqlite3
SQLite version 3.7.17 2013-05-20 00:56:22
Enter ".help" for instructions
Enter SQL statements terminated with a ";"
sqlite> create table test(id int, name varchar(10), salary double);
sqlite> insert into test values(1,'foo', 12.13),(2,'bar',202.22);
sqlite> .q
```

下面的配置是读取该表到终端的作业:

=== "job/sqlite2stream.json"

  ```json
  --8<-- "jobs/sqlitereader.json"
  ```

将上述配置文件保存为   `job/sqlite2stream.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/sqlite2stream.json
```

## 参数说明

| 配置项          | 是否必须 | 类型       | 默认值 |         描述   |
| :-------------- | :------: | ------ |------------- |--------------|
| jdbcUrl         |    是    | list | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接[附件控制信息][1] |
| driver          |   否     |  string   | 无      | 自定义驱动类名，解决兼容性问题，详见下面描述 |
| username        |    是    | string | 无     | 数据源的用户名, 可随意配置，但不能缺失 |
| password        |    否    | string | 无     | 数据源指定用户名的密码，可不配置 |
| table           |    是    | list | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column          |    是    | list | 无     |  所配置的表中需要同步的列名集合，详细描述 [rdbmreader](../rdbmsreader) |
| splitPk         |    否    | string | 无     | 使用splitPk代表的字段进行数据分片，详细描述见 [rdbmreader](../rdbmsreader)|
| autoPk          |    否    |  bool       | false | 是否自动猜测分片主键，`3.2.6` 版本引入 |
| where           |    否    | string | 无     | 针对表的筛选条件 |
| querySql        |    否    | list | 无     | 使用自定义的SQL而不是指定表来获取数据，当配置了这一项之后，Addax系统就会忽略 `table`，`column`这些配置项 |

[1]: http://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html

## 类型转换

| Addax 内部类型| MySQL 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, mediumint, int, bigint|
| Double   |float, double, decimal|
| String   |varchar, char, tinytext, text, mediumtext, longtext, year   |
| Date     |date, datetime, timestamp, time    |
| Boolean  |bit, bool   |
| Bytes    |tinyblob, mediumblob, blob, longblob, varbinary    |
