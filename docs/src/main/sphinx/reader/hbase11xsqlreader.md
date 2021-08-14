# hbase11xsql reader 


hbase11xsqlreader 插件实现了从Phoenix(HBase SQL)读取数据, 支持的 HBase 版本为 1.x

## 配置样例

配置一个从Phoenix同步抽取数据到本地的作业:

```json
{
    "job": {
        "setting": {
            "speed": {
                "byte":-1,
              "channel": 1
            }
        },  
        "content": [ {
                "reader": {
                    "name": "hbase11xsqlreader",
                    "parameter": {
                        "hbaseConfig": {
                            "hbase.zookeeper.quorum": "node1,node2,node3"
                        },  
                        "table": "US_POPULATION",
                        "column": [],
                        "where": "1=1",
                        "querySql": ""
                    }
                },  
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print":true,
                        "encoding": "UTF-8"
                    }
                }
            }
        ]
    }
}
```

## 参数说明

| 配置项      | 是否必须 | 默认值 | 描述                                                                           
| :---------- | :------: | ------ | --------------------------------------------------------------------------- |
| hbaseConfig |    是    | 无     | 需要通过 Phoenix 客户端去连接 hbase 集群，因此这里需要填写对应 hbase 集群的 `zkurl`地址 |
| table       |    是    | 无     | 指定 Phoenix 中的表名,如果有 namespace，该值设置为 `namespace.tablename`          |
| querySql    |   否     | 无     | 不是直接查询表，而是提供具体的查询语句，如果该参数和 `table` 参数同时存在，则优先使用该参数   |
| column      |    是    | 无     | 填写需要从phoenix表中读取的列名集合，使用JSON的数组描述字段信息，空值或 `"*"` 表示读取所有列    |
| where       |   否     | 无     | `where` 条件  |

## 类型转换

目前支持大部分 Phoenix类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出类型转换列表:

| Addax 内部类型 | Phoenix 数据类型                   |
| -------------- | ---------------------------------- |
| String         | CHAR, VARCHAR                      |
| Bytes          | BINARY, VARBINARY                  |
| Bool           | BOOLEAN                            |
| Long           | INTEGER, TINYINT, SMALLINT, BIGINT |
| Double         | FLOAT, DECIMAL, DOUBLE,            |
| Date           | DATE, TIME, TIMESTAMP              |
