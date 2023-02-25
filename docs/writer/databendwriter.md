# DatabendWriter

Databend 插件用于向 [Databend](https://databend.rs/zh-CN/doc/) 数据库以流式方式写入数据。 其实现上是通过访问 Databend http 连接(8000)
，然后通过 [stream load](https://databend.rs/zh-CN/doc/integrations/api/streaming-load)
加载数据到数据中，相比 `insert into` 方式效率要高不少，也是官方推荐的生产环境下的数据加载方式。

Databend 是一个兼容 MySQL 协议的数据库后端，因此 Databend 读取可以使用 [MySQLReader](../../reader/mysqlreader) 进行访问。

## 示例

假定要写入的表的建表语句如下：

```sql
CREATE
DATABASE example_db;
CREATE TABLE `example_db`.`table1` (
  `siteid` INT DEFAULT CAST(10 AS INT),
  `citycode` INT,
  `username` VARCHAR,
  `pv` BIGINT
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

| 配置项         | 是否必须 | 类型   | 默认值    | 描述                                                         |
| :------------- | :------: | ------ | --------- | ------------------------------------------------------------ |
| jdbcUrl        |    否    | string | 无        | 目的数据库的 JDBC 连接信息，用于执行`preSql`及`postSql`      |
| loadUrl        |    是    | string | 无        | Databend query 节点的地址用于StreamLoad，可以为多个 query 地址，`query_ip:query_http_port`，从多个地址轮循写入 |
| username       |    是    | string | 无        | HTTP 签名验证帐号                                            |
| password       |    否    | string | 无        | HTTP 签名验证密码                                            |
| database       |    是    | string | 无        | Databend表的数据库名称                                       |
| table          |    是    | string | 无        | Databend表的表名称                                           |
| column         |    否    | list   | 无        | 所配置的表中需要同步的列名集合，详细描述见 [rdbmswriter](../rdbmswriter) |
| maxBatchRows   |    否    | int    | 500000    | 定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 Addax 出现OOM或者目标数据库事务提交失败导致挂起 |
| maxBatchSize   |    否    | int    | 104857600 | 单次StreamLoad导入的最大字节数                               |
| flushInterval  |    否    | int    | 300000    | 上一次StreamLoad结束至下一次开始的时间间隔（单位：ms)        |
| endpoint       |    是    | string | 无        | Databend 的HTTP连接方式，只需要写到主机和端口即可，具体路径插件会自动拼装 |
| username       |    是    | string | 无        | HTTP 签名验证帐号                                            |
| password       |    否    | string | 无        | HTTP 签名验证密码                                            |
| table          |    是    | string | 无        | 所选取的需要同步的表名                                       |
| column         |    否    | list   | 无        | 所配置的表中需要同步的列名集合，详细描述见 [rdbmswriter](../rdbmswriter) |
| batchSize      |    否    | int    | 1024      |                                                              |
| lineDelimiter  |    否    | string | `\n`      | 每行的分隔符,支持高位字节, 例如 `\\x02`                      |
| filedDelimiter |    否    | string | `\t`      | 每列的分隔符,支持高位字节, 例如 `\\x01`                      |
| format         |    否    | string | `csv`     | 被导入数据会被转换成 format 指定格式。                       |


## 类型转换

默认传入的数据均会被转为字符串，并以`\t`作为列分隔符，`\n`作为行分隔符，组成`csv`文件进行StreamLoad导入操作。
