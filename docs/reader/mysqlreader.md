# MySQL Reader

MysqlReader 插件实现了从Mysql读取数据

## 示例

我们在 MySQL 的 test 库上创建如下表，并插入一条记录

```sql
--8<-- "sql/mysql.sql"
```

下面的配置是读取该表到终端的作业:

=== "job/mysql2stream.json"

  ```json
  --8<-- "jobs/mysqlreader.json"
  ```

将上述配置文件保存为   `job/mysql2stream.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/mysql2stream.json
```

## 参数说明

| 配置项          | 是否必须 | 类型       | 默认值 |         描述   |
| :-------------- | :------: | ------ |------------- |--------------|
| jdbcUrl         |    是    | list | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接[附件控制信息][1] |
| driver          |   否     |  string   | 无      | 自定义驱动类名，解决兼容性问题，详见下面描述 |
| username        |    是    | string | 无     | 数据源的用户名 |
| password        |    否    | string | 无     | 数据源指定用户名的密码 |
| table           |    是    | list | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column          |    是    | list | 无     |  所配置的表中需要同步的列名集合，详细描述 [rdbmreader](../rdbmsreader) |
| splitPk         |    否    | string | 无     | 使用splitPk代表的字段进行数据分片，详细描述见 [rdbmreader](../rdbmsreader)|
| autoPk          |    否    |  bool       | false | 是否自动猜测分片主键，`3.2.6` 版本引入 |
| where           |    否    | string | 无     | 针对表的筛选条件 |
| querySql        |    否    | list | 无     | 使用自定义的SQL而不是指定表来获取数据，当配置了这一项之后，Addax系统就会忽略 `table`，`column`这些配置项 |

[1]: http://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html

### driver

当前 Addax 采用的 MySQL JDBC 驱动为 8.0 以上版本，驱动类名使用的 `com.mysql.cj.jdbc.Driver`，而不是 `com.mysql.jdbc.Driver`。 如果你需要采集的 MySQL 服务低于 `5.6`，需要使用到 `Connector/J 5.1` 驱动，则可以采取下面的步骤：

**替换插件内置的驱动**

`rm -f plugin/reader/mysqlreader/lib/mysql-connector-java-*.jar`

**拷贝老的驱动到插件目录**

`cp mysql-connector-java-5.1.48.jar plugin/reader/mysqlreader/lib/`

**指定驱动类名称**

在你的 json 文件类，配置 `"driver": "com.mysql.jdbc.Driver"`

## 类型转换

目前MysqlReader支持大部分Mysql类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出MysqlReader针对Mysql类型转换列表:

| Addax 内部类型| MySQL 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, mediumint, int, bigint|
| Double   |float, double, decimal|
| String   |varchar, char, tinytext, text, mediumtext, longtext, year   |
| Date     |date, datetime, timestamp, time    |
| Boolean  |bit, bool   |
| Bytes    |tinyblob, mediumblob, blob, longblob, varbinary    |

请注意:

* 除上述罗列字段类型外，其他类型均不支持
* `tinyint(1)` Addax视作为整形
* `year` Addax视作为字符串类型
* `bit` Addax属于未定义行为

### 数据库编码问题

Mysql本身的编码设置非常灵活，包括指定编码到库、表、字段级别，甚至可以均不同编码。优先级从高到低为字段、表、库、实例。我们不推荐数据库用户设置如此混乱的编码，最好在库级别就统一到UTF-8。

MysqlReader底层使用JDBC进行数据抽取，JDBC天然适配各类编码，并在底层进行了编码转换。因此MysqlReader不需用户指定编码，可以自动获取编码并转码。

对于Mysql底层写入编码和其设定的编码不一致的混乱情况，MysqlReader对此无法识别，对此也无法提供解决方案，对于这类情况，`导出有可能为乱码`。
