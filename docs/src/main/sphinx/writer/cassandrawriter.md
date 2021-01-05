# CassandraWriter 插件文档

## 1 快速介绍

CassandraWriter插件实现了向Cassandra写入数据。在底层实现上，CassandraWriter通过datastax的java driver连接Cassandra实例，并执行相应的cql语句将数据写入cassandra中。

## 2 实现原理

简而言之，CassandraWriter通过java driver连接到Cassandra实例，并根据用户配置的信息生成INSERT CQL语句，然后发送到Cassandra。

对于用户配置Table、Column的信息，CassandraReader将其拼接为CQL语句发送到Cassandra。

## 3 功能说明

### 3.1 配置样例

配置一个从内存产生到Cassandra导入的作业:

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 5,
        "bytes": -1
      }
    },
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "value": "name",
                "type": "string"
              },
              {
                "value": "false",
                "type": "bool"
              },
              {
                "value": "1988-08-08 08:08:08",
                "type": "date"
              },
              {
                "value": "addr",
                "type": "bytes"
              },
              {
                "value": 1.234,
                "type": "double"
              },
              {
                "value": 12345678,
                "type": "long"
              },
              {
                "value": 2.345,
                "type": "double"
              },
              {
                "value": 3456789,
                "type": "long"
              },
              {
                "value": "4a0ef8c0-4d97-11d0-db82-ebecdb03ffa5",
                "type": "string"
              },
              {
                "value": "value",
                "type": "bytes"
              },
              {
                "value": "-838383838,37377373,-383883838,27272772,393993939,-38383883,83883838,-1350403181,817650816,1630642337,251398784,-622020148",
                "type": "string"
              }
            ],
            "sliceRecordCount": 10000000
          }
        },
        "writer": {
          "name": "cassandrawriter",
          "parameter": {
            "host": "localhost",
            "port": 9042,
            "useSSL": false,
            "keyspace": "stresscql",
            "table": "dst",
            "batchSize": 10,
            "column": [
              "name",
              "choice",
              "date",
              "address",
              "dbl",
              "lval",
              "fval",
              "ival",
              "uid",
              "value",
              "listval"
            ]
          }
        }
      }
    ]
  }
}
```

### 3.2 参数说明

| 配置项                  | 是否必须 | 默认值       | 描述                                                                                                                        |
| :---------------------- | :------: | ------------ | --------------------------------------------------------------------------------------------------------------------------- |
| host                    |    是    | 无           | Cassandra连接点的域名或ip，多个node之间用逗号分隔                                                                           |
| port                    |    是    | 9042         | Cassandra端口                                                                                                               |
| username                |    否    | 无           | 数据源的用户名                                                                                                              |
| password                |    否    | 无           | 数据源指定用户名的密码                                                                                                      |
| useSSL                  |    否    | false        | 是否使用SSL连接                                                                                                             |
| connectionsPerHost      |    否    | 8            | 客户端连接池配置：与服务器每个节点建多少个连接                                                                              |
| maxPendingPerConnection |    否    | 128          | 客户端连接池配置：每个连接最大请求数                                                                                        |
| keyspace                |    是    | 无           | 需要同步的表所在的keyspace                                                                                                  |
| table                   |    是    | 无           | 所选取的需要同步的表                                                                                                        |
| column                  |    是    | 无           | 所配置的表中需要同步的列集合,内容可以是列的名称或 `writetime()`。如果将列名配置为 `writetime()`，会将这一列的内容作为时间戳 |
| consistancyLevel        |    否    | LOCAL_QUORUM | 数据一致性级别, 可选 `ONE|QUORUM|LOCAL_QUORUM|EACH_QUORUM|ALL|ANY|TWO|THREE|LOCAL_ONE`                                      |
| batchSize               |    否    | 1            | 一次批量提交(UNLOGGED BATCH)的记录数大小（条数）                                                                            |

### 3.3 类型转换

目前CassandraReader支持除counter和Custom类型之外的所有类型。

下面列出CassandraReader针对Cassandra类型转换列表:

| DataX 内部类型 | Cassandra 数据类型                                                     |
| -------------- | ---------------------------------------------------------------------- |
| Long           | int, tinyint, smallint,varint,bigint,time                              |
| Double         | float, double, decimal                                                 |
| String         | ascii,varchar, text,uuid,timeuuid,duration,list,map,set,tuple,udt,inet |
| Date           | date, timestamp                                                        |
| Boolean        | bool                                                                   |
| Bytes          | blob                                                                   |

请注意:

目前不支持counter类型和custom类型。

## 4 约束限制

### batchSize

1. 不能超过65535
2. batch中的内容大小受到服务器端 `batch_size_fail_threshold_in_kb` 的限制。
3. 如果batch中的内容超过了 `batch_size_warn_threshold_in_kb` 的限制，会打出warn日志，但并不影响写入，忽略即可。
4. 如果批量提交失败，会把这个批量的所有内容重新逐条写入一遍。


