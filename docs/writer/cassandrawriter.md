# Cassandra Writer

CassandraWriter 插件用于向 [Cassandra](https://cassandra.apache.org) 写入数据。

## 配置样例

配置一个从内存产生到 Cassandra 导入的作业:

=== "jobs/stream2cassandra.json"

```json
--8<-- "jobs/cassandrawriter.json"
```

## 参数说明

| 配置项                  | 是否必须 | 默认值       | 描述                                                                                                                        |
| :---------------------- | :------: | ------------ | --------------------------------------------------------------------------------------------------------------------------- |
| host                    |    是    | 无           | Cassandra 连接点的域名或 ip，多个 node 之间用逗号分隔                                                                       |
| port                    |    是    | 9042         | Cassandra 端口                                                                                                              |
| username                |    否    | 无           | 数据源的用户名                                                                                                              |
| password                |    否    | 无           | 数据源指定用户名的密码                                                                                                      |
| useSSL                  |    否    | false        | 是否使用 SSL 连接                                                                                                           |
| connectionsPerHost      |    否    | 8            | 客户端连接池配置：与服务器每个节点建多少个连接                                                                              |
| maxPendingPerConnection |    否    | 128          | 客户端连接池配置：每个连接最大请求数                                                                                        |
| keyspace                |    是    | 无           | 需要同步的表所在的 keyspace                                                                                                 |
| table                   |    是    | 无           | 所选取的需要同步的表                                                                                                        |
| column                  |    是    | 无           | 所配置的表中需要同步的列集合,内容可以是列的名称或 `writetime()`。如果将列名配置为 `writetime()`，会将这一列的内容作为时间戳 |
| consistancyLevel        |    否    | LOCAL_QUORUM | 数据一致性级别, 可选 `ONE, QUORUM, LOCAL_QUORUM, EACH_QUORUM, ALL, ANY, TWO, THREE, LOCAL_ONE`                              |
| batchSize               |    否    | 1            | 一次批量提交(UNLOGGED BATCH)的记录数大小（条数）                                                                            |

## 类型转换

| Addax 内部类型 | Cassandra 数据类型                                                     |
| -------------- | ---------------------------------------------------------------------- |
| Long           | int, tinyint, smallint,varint,bigint,time                              |
| Double         | float, double, decimal                                                 |
| String         | ascii,varchar, text,uuid,timeuuid,duration,list,map,set,tuple,udt,inet |
| Date           | date, timestamp                                                        |
| Boolean        | bool                                                                   |
| Bytes          | blob                                                                   |

请注意:

目前不支持 `counter` 类型和 `custom` 类型。

## 约束限制

### batchSize

1. 不能超过 65535
2. batch 中的内容大小受到服务器端 `batch_size_fail_threshold_in_kb` 的限制。
3. 如果 batch 中的内容超过了 `batch_size_warn_threshold_in_kb` 的限制，会打出 warn 日志，但并不影响写入，忽略即可。
4. 如果批量提交失败，会把这个批量的所有内容重新逐条写入一遍。
