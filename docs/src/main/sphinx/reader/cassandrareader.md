
# CassandraReader 插件文档


## 1 快速介绍

CassandraReader插件实现了从Cassandra读取数据。在底层实现上，CassandraReader通过datastax的java driver连接Cassandra实例，并执行相应的cql语句将数据从cassandra中SELECT出来。

## 2 实现原理

简而言之，CassandraReader通过java driver连接到Cassandra实例，并根据用户配置的信息生成查询SELECT CQL语句，然后发送到Cassandra，并将该CQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column的信息，CassandraReader将其拼接为CQL语句发送到Cassandra。

## 3 功能说明

### 3.1 配置样例

配置一个从Cassandra同步抽取数据到本地的作业:

```json
{
    "job": {
        "setting": {
            "speed": {
                 "channel": 3
            }
        },
        "content": [
            {
               "reader": {
                    "name": "cassandrareader",
                    "parameter": {
                        "host": "localhost",
                        "port": 9042,
                        "useSSL": false,
                        "keyspace": "test",
                        "table": "datax_src",
                        "column": [
                            "textCol",
                            "blobCol",
                            "writetime(blobCol)",
                            "boolCol",
                            "smallintCol",
                            "tinyintCol",
                            "intCol",
                            "bigintCol",
                            "varintCol",
                            "floatCol",
                            "doubleCol",
                            "decimalCol",
                            "dateCol",
                            "timeCol",
                            "timeStampCol",
                            "uuidCol",
                            "inetCol",
                            "durationCol",
                            "listCol",
                            "mapCol",
                            "setCol"
                            "tupleCol"
                            "udtCol",
                        ]
                    }
               },
               "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print":true
                    }
                }
            }
        ]
    }
}

```

### 3.2 参数说明

| 配置项           | 是否必须 | 默认值       |    描述    |
| :--------------- | :------: | ------------ |-------------|
| host             |    是    | 无           | Cassandra连接点的域名或ip，多个node之间用逗号分隔 |
| port             |    是    | 9042         | Cassandra端口 |
| username         |    否    | 无           | 数据源的用户名 |
| password         |    否    | 无           | 数据源指定用户名的密码 |
| useSSL           |    否    | false        | 是否使用SSL连接 |
| keyspace         |    是    | 无           | 需要同步的表所在的keyspace |
| table            |    是    | 无           | 所选取的需要同步的表 |
| column           |    是    | 无           | 所配置的表中需要同步的列集合,其中的元素可以指定列的名称或 `writetime(column_name)`，后一种形式会读取`column_name`列的时间戳而不是数据 |
| where            |    否    | 无           | 数据筛选条件的 `cql` 表达式 |
| allowFiltering   |    否    | 无           | 是否在服务端过滤数据，详细描述参考官方文档的[相关描述](https://cassandra.apache.org/doc/latest/cql/dml.html#allowing-filtering) |
| consistancyLevel |    否    | LOCAL_QUORUM | 数据一致性级别, 可选 `ONE|QUORUM|LOCAL_QUORUM|EACH_QUORUM|ALL|ANY|TWO|THREE|LOCAL_ONE`  

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
