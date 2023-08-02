# DatabendWriter

Databend 插件用于向 [Databend](https://databend.rs/zh-CN/doc/) 数据库以流式方式写入数据。 其实现上是通过访问 Databend
http 连接(8000)
，然后通过 [stream load](https://databend.rs/zh-CN/doc/integrations/api/streaming-load)
加载数据到数据中，相比 `insert into` 方式效率要高不少，也是官方推荐的生产环境下的数据加载方式。

Databend 是一个兼容 MySQL 协议的数据库后端，因此 Databend 读取可以使用 [MySQLReader](../../reader/mysqlreader) 进行访问。

## 示例

假定要写入的表的建表语句如下：

```sql
CREATE
DATABASE example_db;
CREATE TABLE `example_db`.`table1`
(
    `siteid`   INT DEFAULT CAST(10 AS INT),
    `citycode` INT,
    `username` VARCHAR,
    `pv`       BIGINT
);
```

下面配置一个从内存读取数据，然后写入到 databend 表的配置文件

```json
--8<-- "jobs/databendwriter.json"
```

将上述配置文件保存为 `job/stream2databend.json`

执行下面的命令

```shell
bin/addax.sh job/stream2Databend.json
```

## 参数说明

| 配置项              | 是否必须 | 类型     | 默认值      | 描述                                                                           |
|:-----------------|:----:|--------|----------|------------------------------------------------------------------------------|
| jdbcUrl          |  否   | string | 无        | 目的数据库的 JDBC 连接信息，用于执行`preSql`及`postSql`                                      |
| username         |  是   | string | 无        | JDBC 数据源用户名                                                                  |
| password         |  否   | string | 无        | JDBC 数据源密码                                                                   |
| table            |  是   | string | 无        | Databend 表的表名称                                                               |
| column           |  否   | list   | 无        | 所配置的表中需要同步的列名集合，详细描述见 [rdbmswriter][1]                                       |
| preSql           |  否   | list   | 无        | 任务开始前执行的 SQL 语句，多条语句以分号分隔，语句中不能包含分号。                                         |
| postSql          |  否   | list   | 无        | 任务结束后执行的 SQL 语句，多条语句以分号分隔，语句中不能包含分号。                                         |
| batchSize        |  否   | int    | 1024     | 每个批次的记录数                                                                     |
| writeMode        |  否   | string | `insert` | 写入模式，支持 insert 和 replace 两种模式，默认为 insert。若为 replace，务必填写 onConflictColumn 参数 |
| onConflictColumn |  否   | string | 无        | 冲突列，当 writeMode 为 replace 时，必须指定冲突列，否则会导致写入失败。                               |

### writeMode

该参数为 `4.1.2` 版本引入，用来支持 Databend 的 `replace into` 语法，当该参数设定为 `replace`
时，必须同时指定 `onConflictColumn` 参数，用来判断数据是插入还是更新的依据。

两个参数的示例如下：

```json
{
  "writeMode": "replace",
  "onConflictColumn": [
    "id"
  ]
}
```

## 类型转换

默认传入的数据均会被转为字符串，并以`\t`作为列分隔符，`\n`作为行分隔符，组成`csv`文件进行 StreamLoad 导入操作。

[1]: ../rdbmswriter
