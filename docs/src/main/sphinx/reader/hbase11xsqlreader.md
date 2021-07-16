# hbase11xsqlreader 


hbase11xsqlreader 插件实现了从Phoenix(HBase SQL)读取数据。
在底层实现上，hbase11xsqlreader 通过Phoenix客户端去连接远程的HBase集群，并执行相应的sql语句将数据从Phoenix库中SELECT出来。

## 功能说明

### 配置样例

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

### 参数说明

| 配置项      | 是否必须 | 默认值 | 描述                                                                                                              |
| :---------- | :------: | ------ | ----------------------------------------------------------------------------------------------------------------- |
| hbaseConfig |    是    | 无     | hbase11xsqlreader需要通过Phoenix客户端去连接hbase集群，因此这里需要填写对应hbase集群的zkurl地址，注意不要添加2181 |
| table       |    是    | 无     | 编写Phoenix中的表名,如果有namespace，该值设置为 `namespace.tablename`                                             |
| querySql    |   否     | 无     | 不是直接查询表，而是提供具体的查询语句，如果该参数和 `table` 参数同时存在，则优先使用该参数   |
| column      |    是    | 无     | 填写需要从phoenix表中读取的列名集合，使用JSON的数组描述字段信息，空值或`"*"`表示读取所有列                               |
| where       |   否     | 无     | `where` 条件  |

## 类型转换

目前 hbase11xsqlreader 支持大部分Phoenix类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出MysqlReader针对Mysql类型转换列表:

| Addax 内部类型 | Phoenix 数据类型                   |
| -------------- | ---------------------------------- |
| String         | CHAR, VARCHAR                      |
| Bytes          | BINARY, VARBINARY                  |
| Bool           | BOOLEAN                            |
| Long           | INTEGER, TINYINT, SMALLINT, BIGINT |
| Double         | FLOAT, DECIMAL, DOUBLE,            |
| Date           | DATE, TIME, TIMESTAMP              |
