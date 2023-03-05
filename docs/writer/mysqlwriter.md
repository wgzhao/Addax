# MySQL Writer

MysqlWriter 插件实现了写入数据到 [MySQL][1] 目的表的功能。

## 示例

假定要写入的 MySQL 表建表语句如下：

```sql
create table test.addax_tbl
(
col1 varchar(20) ,
col2 int(4),
col3 datetime,
col4 boolean,
col5 binary
) default charset utf8;
```

这里使用一份从内存产生到 MySQL 导入的数据。

=== "job/stream2mysql.json"

```json
--8<-- "jobs/mysqlwriter.json"
```

将上述配置文件保存为 `job/stream2mysql.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/stream2mysql.json
```

## 参数说明

| 配置项    | 是否必须 | 类型   | 默认值 | 描述                                                                                              |
| :-------- | :------: | ------ | ------ | ------------------------------------------------------------------------------------------------- |
| jdbcUrl   |    是    | list   | 无     | 对端数据库的 JDBC 连接信息，jdbcUrl 按照 RDBMS 官方规范，并可以填写连接[附件控制信息][2]          |
| driver    |    否    | string | 无     | 自定义驱动类名，解决兼容性问题，详见下面描述                                                      |
| username  |    是    | string | 无     | 数据源的用户名                                                                                    |
| password  |    否    | string | 无     | 数据源指定用户名的密码                                                                            |
| table     |    是    | list   | 无     | 所选取的需要同步的表名,使用 JSON 数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构     |
| column    |    是    | list   | 无     | 所配置的表中需要同步的列名集合，详细描述见 [rdbmswriter][3]                                       |
| session   |    否    | list   | 空     | 获取 MySQL 连接时，执行 session 指定的 SQL 语句，修改当前 connection session 属性         |
| preSql    |    否    | list   | 无     | 数据写入前先执行的 sql 语句，例如清除旧数据,如果 Sql 中有你需要操作到的表名称，可用 `@table` 表示 |
| postSql   |    否    | list   | 无     | 数据写入完成后执行的 sql 语句，例如加上某一个时间戳                                               |
| writeMode |    是    | string | insert | 数据写入表的方式，详见下文                                                                        |
| batchSize |    否    | int    | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数                                                    |

### driver

当前采用的 MySQL JDBC 驱动为 8.0 以上版本，驱动类名使用的 `com.mysql.cj.jdbc.Driver`，而不是 `com.mysql.jdbc.Driver`。
如果你需要采集的 MySQL 服务低于 `5.6`，需要使用到 `Connector/J 5.1` 驱动，则可以采取下面的步骤：

1. 替换插件内置的驱动
  `rm -f plugin/writer/mysqlwriter/lib/mysql-connector-java-*.jar`

2. 拷贝老的驱动到插件目录
  `cp mysql-connector-java-5.1.48.jar plugin/writer/mysqlwriter/lib/`

3. 指定驱动类名称
  在你的 json 文件类，配置 `"driver": "com.mysql.jdbc.Driver"`

### writeMode

- `insert` 表示采用 `insert into`
- `replace`表示采用`replace into`方式
- `update` 表示采用 `ON DUPLICATE KEY UPDATE` 语句

## 类型转换

| Addax 内部类型 | Mysql 数据类型                                       |
| -------------- | ---------------------------------------------------- |
| Long           | int, tinyint, smallint, mediumint, int, bigint, year |
| Double         | float, double, decimal                               |
| String         | varchar, char, tinytext, text, mediumtext, longtext  |
| Date           | date, datetime, timestamp, time                      |
| Boolean        | bit, bool                                            |
| Bytes          | tinyblob, mediumblob, blob, longblob, varbinary      |

bit 类型目前是未定义类型转换

[1]: https://www.mysql.com
[2]: http://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html
[3]: ../rdbmswriter
