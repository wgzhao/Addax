# Doris Writer

DorisWriter 插件用于向 [Doris](http://doris.incubator.apache.org/master/zh-CN/) 数据库以流式方式写入数据。 其实现上是通过访问 Doris http 连接(8030)
，然后通过 [stream load](http://doris.incubator.apache.org/master/zh-CN/administrator-guide/load-data/stream-load-manual.html)
加载数据到数据中，相比 `insert into` 方式效率要高不少，也是官方推荐的生产环境下的数据加载方式。

Doris 是一个兼容 MySQL 协议的数据库后端，因此 Doris 读取可以使用 [MySQLReader](../../reader/mysqlreader) 进行访问。

## 示例

假定要写入的表的建表语句如下：

```sql
CREATE
DATABASE example_db;
CREATE TABLE example_db.table1
(
    siteid   INT         DEFAULT '10',
    citycode SMALLINT,
    username VARCHAR(32) DEFAULT '',
    pv       BIGINT SUM DEFAULT '0'
) AGGREGATE KEY(siteid, citycode, username)
DISTRIBUTED BY HASH(siteid) BUCKETS 10
PROPERTIES("replication_num" = "1");
```

下面配置一个从内存读取数据，然后写入到 doris 表的配置文件

```json
--8<-- "jobs/doriswriter.json"
```

将上述配置文件保存为 `job/stream2doris.json`

执行下面的命令

```shell
bin/addax.sh job/stream2doris.json
```

输出类似如下：

```
--8<-- "output/doriswriter.txt"
```

## 参数说明

| 配置项         | 是否必须 | 类型   | 默认值 | 描述                                                                                                             |
| :------------- | :------: | ------ | ------ | ---------------------------------------------------------------------------------------------------------------- |
| endpoint       |    是    | string | 无     | Doris 的HTTP连接方式，只需要写到主机和端口即可，具体路径插件会自动拼装 ｜                                        |
| username       |    是    | string | 无     | HTTP 签名验证帐号                                                                                                |
| password       |    否    | string | 无     | HTTP 签名验证密码                                                                                                |
| table          |    是    | string | 无     | 所选取的需要同步的表名                                                                                           |
| column         |    否    | list   | 无     | 所配置的表中需要同步的列名集合，详细描述见 [rdbmswriter](../rdbmswriter)                                         |
| batchSize      |    否    | int    | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 Addax 出现OOM或者目标数据库事务提交失败导致挂起 |
| lineDelimiter  |    否    | string | `\n`   | 每行的的分隔符,支持多个字节, 例如 `\x02\x03`                                                                     |
| format         |    否    | string | `csv`  | 导入数据的格式, 可以使是 json 或者 csv                                                                           |
| loadProps      |    否    | map    | `csv`  | streamLoad 的请求参数，详情参照[StreamLoad介绍页面][1]                                                           |
| connectTimeout |    否    | int    | -1     | StreamLoad单次请求的超时时间, 单位毫秒(ms)                                                                       |

[1]: https://doris.apache.org/master/zh-CN/administrator-guide/load-data/load-json-format.html#stream-load

## endpoint

`endpoint` 只是的任意一个 BE 的主机名及 `webserver_port` 端口，官方文档描述也可以填写 FE 主机名和 `http_port` 端口，但实际测试一直处于连接拒绝状态。

### column

该插件中的 `column` 不是必须项，如果没有配置该项，或者配置为 `["*"]` ， 则按照 reader 插件获取的字段值进行顺序拼装。 否则可以按照如下方式指定需要插入的字段

```json
{
  "column": [
    "siteid",
    "citycode",
    "username"
  ]
}
```
