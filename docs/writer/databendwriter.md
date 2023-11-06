# DatabendWriter

Databend 插件用于向 [Databend](https://databend.rs/zh-CN/doc/) 数据库以 JDBC 方式写入数据。 

Databend 是一个兼容 MySQL 协议的数据库后端，因此 Databend 写入可以使用 [MySQLWriter](../../writer/mysqlwriter) 进行访问。

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

DatabendWriter 基于 [rdbmswriter](../rdbmswriter) 实现，因此可以参考 rdbmswriter 的所有配置项，并增加了如下配置项：

| 配置项           | 是否必须 | 类型   | 默认值   | 描述                                                                     |
| :--------------- | :------: | ------ | -------- | ------------------------------------------------------------------------ |
| writeMode        |    否    | string | `insert` | 写入模式，支持 insert 和 replace 两种模式，默认为 insert。               |
| onConflictColumn |    否    | string | 无       | 冲突列，当 writeMode 为 replace 时，必须指定冲突列，否则会导致写入失败。 |

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