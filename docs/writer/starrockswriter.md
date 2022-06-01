# StarRocksWriter

StarRocksWriter 插件用于向 [Starrocks](https://www.starrocks.com/zh-CN/index) 数据库以流式方式写入数据。 其实现上是通过访问 Doris http 连接(8030)
，然后通过 [stream load](https://docs.starrocks.com/zh-cn/main/loading/StreamLoad)
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
--8<-- "jobs/starrockswriter.json"
```

将上述配置文件保存为 `job/stream2starrocks.json`

执行下面的命令

```shell
bin/addax.sh job/stream2starrocks.json
```

## 参数说明

| 配置项          | 是否必须 | 类型  | 默认值 |         描述   |
| :-------------- | :------: | ------ |------------- |-------|
| jdbcUrl         |    否    | string | 无    | 目的数据库的 JDBC 连接信息，用于执行`preSql`及`postSql` |
| loadUrl         |    是    | string | 无     | StarRocks FE的地址用于StreamLoad[1]，可以为多个fe地址，`fe_ip:fe_http_port` ｜
| username        |    是    | string | 无     | HTTP 签名验证帐号 |
| password        |    否    | string | 无     | HTTP 签名验证密码 |
| database        |    是    | string | 无     | StarRocks表的数据库名称|
| table           |    是    | string | 无     | StarRocks表的表名称|
| column          |    否    | list | 无     |  所配置的表中需要同步的列名集合，详细描述见 [rdbmswriter](../rdbmswriter) |
| maxBatchRows    |    否    | int | 500000   | 单次StreamLoad导入的最大行数 |
| maxBatchSize    |    否    | int | 104857600 | 单次StreamLoad导入的最大字节数 |
| flushInterval   |    否    | int | 300000  | 上一次StreamLoad结束至下一次开始的时间间隔（单位：ms) |
| loadProps       |  否     | map | `csv` | streamLoad 的请求参数，详情参照[StreamLoad介绍页面][1] |

[1]: https://docs.starrocks.com/zh-cn/main/loading/StreamLoad


## 类型转换

默认传入的数据均会被转为字符串，并以`\t`作为列分隔符，`\n`作为行分隔符，组成`csv`文件进行StreamLoad导入操作。
如需更改列分隔符， 则正确配置 `loadProps` 即可：
```json
"loadProps": {
    "column_separator": "\\x01",
    "row_delimiter": "\\x02"
}
```

如需更改导入格式为`json`， 则正确配置 `loadProps` 即可：
```json
"loadProps": {
    "format": "json",
    "strip_outer_array": true
}
```
