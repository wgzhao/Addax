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

MysqlWriter 基于 [rdbmswriter](../rdbmswriter) 实现，因此可以参考 rdbmswriter 的所有配置项，并且增加了一些 MySQL 特有的配置项。

| 配置项    | 是否必须 | 类型   | 默认值 | 描述                                           |
| :-------- | :------: | ------ | ------ | ---------------------------------------------- |
| writeMode |    是    | string | insert | 数据写入表的方式，详见下文                     |
| batchSize |    否    | int    | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数 |

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

