# Cassandra Reader

`CassandraReader` 插件实现了从 [Cassandra](https://cassandra.apache.org) 读取数据。

## 配置

下面是配置一个从 Cassandra 读取数据到终端的例子

=== "job/cassandra2stream.json"

    ```json
    --8<-- "jobs/cassandrareader.json"
    ```

## 参数说明

`parameter` 配置项支持以下配置

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
| allowFiltering   |    否    | 无           | 是否在服务端过滤数据，详细描述参考官方文档的[相关描述][1] |
| consistencyLevel |    否    | LOCAL_QUORUM | 数据一致性级别, 可选 `ONE, QUORUM, LOCAL_QUORUM, EACH_QUORUM, ALL, ANY, TWO, THREE, LOCAL_ONE` |

[1]: https://cassandra.apache.org/doc/latest/cql/dml.html#allowing-filtering

### 支持的数据类型

目前支持除 `counter` 和 `Custom` 类型之外的所有类型。

下面列出 `CassandraReader` 针对 `Cassandra` 类型转换列表:

| Addax 内部类型 | Cassandra 数据类型                                                     |
| -------------- | ---------------------------------------------------------------------- |
| Long           | int, tinyint, smallint,varint,bigint,time,counter                      |
| Double         | float, double, decimal                                                 |
| String         | ascii,varchar, text,uuid,timeuuid,duration,list,map,set,tuple,udt,inet |
| Date           | date, timestamp                                                        |
| Boolean        | bool                                                                   |
| Bytes          | blob                                                                   |
