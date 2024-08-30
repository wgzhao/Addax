# Doris Writer

DorisWriter 插件用于向 [Doris](http://doris.incubator.apache.org/master/zh-CN/) 数据库以流式方式写入数据。 其实现上是通过访问
Doris http 连接(8030)，然后通过 [stream load](http://doris.incubator.apache.org/master/zh-CN/administrator-guide/load-data/stream-load-manual.html)
加载数据到数据中，相比 `insert into` 方式效率要高不少，也是官方推荐的生产环境下的数据加载方式。

Doris 是一个兼容 MySQL 协议的数据库后端，因此 Doris 读取可以使用 [MySQL Reader](../../reader/mysqlreader) 进行访问。

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

| 配置项           | 是否必须 | 类型   | 默认值 | 描述                                                               |
| :--------------- | :------: | ------ | ------ | ------------------------------------------------------------------ |
| loadUrl          |    是    | string | 无     | Stream Load 的连接目标 ｜                                          |
| username         |    是    | string | 无     | 访问Doris数据库的用户名                                            |
| password         |    否    | string | 无     | 访问Doris数据库的密码                                              |
| flushInterval    |    否    | int    | 3000   | 数据写入到目标表的间隔时间，单位为毫秒，即每隔多少毫秒写入一次数据 |
| flushQueueLength |    否    | int    | 1      | 上传数据的队列长度                                                 |
| table            |    是    | string | 无     | 所选取的需要同步的表名                                             |
| column           |    是    | list   | 无     | 所配置的表中需要同步的列名集合，详细描述见 [RBDMS Writer][1]       |
| batchSize        |    否    | int    | 2048   | 每批次导入数据的最大行数                                           |
| loadProps        |    否    | map    | `csv`  | streamLoad 的请求参数，详情参照[StreamLoad介绍页面][2]             |
| preSql           |    否    | list   |        | 写入数据到目标表前要执行的 SQL 语句                                |
| postSql          |    否    | list   |        | 数据写完后要执行的 SQL 语句                                        |

[1]: ../rdbmswriter
[2]: https://github.com/apache/doris-streamloader/tree/master

## loadUrl

作为 Stream Load 的连接目标。格式为 "ip:port"。其中 IP 是 FE 节点 IP，port 是 FE 节点的 http_port。可以填写多个，当填写多个时，插件会每个批次随机选择一个有效 FE 节点进行连接。

### column

允许配置为 `["*"]` ， 如果是 "*" , 则尝试从 Doris 数据库中直接读取表字段，然后进行拼装。

### loadProps

StreamLoad 的请求参数，详情参照StreamLoad介绍页面。[Stream load - Apache Doris](https://doris.apache.org/zh-CN/docs/data-operate/import/import-way/stream-load-manual)

这里包括导入的数据格式：format等，导入数据格式默认我们使用csv，支持JSON，具体可以参照下面类型转换部分，也可以参照上面Stream load 官方信息

## 类型转换

默认传入的数据均会被转为字符串，并以\t作为列分隔符，\n作为行分隔符，组成csv文件进行StreamLoad导入操作。

默认是csv格式导入，如需更改列分隔符， 则正确配置 loadProps 即可

```json
{
  "loadProps": {
    "column_separator": "\\x01",
    "line_delimiter": "\\x02"
  }
}
```

如需更改导入格式为json， 则正确配置 loadProps 即可：

```json
{
  "loadProps": {
    "format": "json",
    "strip_outer_array": true
  }
}
```