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

下面配置一个从内存读取数据，然后写入到 doris 表的配置文件

```json
--8<-- "jobs/databendwriter.json"
```

将上述配置文件保存为 `job/stream2databend.json`

执行下面的命令

```shell
bin/addax.sh job/stream2databend.json
```

## 参数说明

| 配置项            | 是否必须 | 类型  | 默认值   | 描述                                                          |
|:---------------| :------: | ------ |-------|-------------------------------------------------------------|
| endpoint       |    是    | string | 无     | Databend 的HTTP连接方式，只需要写到主机和端口即可，具体路径插件会自动拼装 ｜               
| username       |    是    | string | 无     | HTTP 签名验证帐号                                                 |
| password       |    否    | string | 无     | HTTP 签名验证密码                                                 |
| table          |    是    | string | 无     | 所选取的需要同步的表名                                                 |
| column         |    否    | list | 无     | 所配置的表中需要同步的列名集合，详细描述见 [rdbmswriter](../rdbmswriter)         |
| batchSize      |    否    | int | 1024  | 定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 Addax 出现OOM或者目标数据库事务提交失败导致挂起 |
| lineDelimiter  |  否     | string | `\n`  | 每行的分隔符,支持多个字节, 例如 `\x02\x03`                                |
| filedDelimiter |  否     | string | `\t`  | 每列的分隔符,支持多个字节, 例如 `\x02\x03`                                |
| format         |  否     | string | `tsv` | 导入数据的格式, 目前仅支持 tsv                                          |
| connectTimeout | 否  | int | -1    | StreamLoad单次请求的超时时间, 单位毫秒(ms)                               |

## 类型转换

默认传入的数据均会被转为字符串，并以`\t`作为列分隔符，`\n`作为行分隔符，组成`tsv`文件进行StreamLoad导入操作。
