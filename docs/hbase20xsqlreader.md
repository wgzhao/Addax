# hbase20xsqlreader  插件文档

## 1 快速介绍

hbase20xsqlreader插件实现了从Phoenix(HBase SQL)读取数据，对应版本为HBase2.X和Phoenix5.X。

## 2 实现原理

简而言之，hbase20xsqlreader通过Phoenix轻客户端去连接Phoenix QueryServer，并根据用户配置信息生成查询SELECT 语句，然后发送到QueryServer读取HBase数据，并将返回结果使用DataX自定义的数据类型拼装为抽象的数据集，最终传递给下游Writer处理。

## 3 功能说明

### 3.1 配置样例

配置一个从Phoenix同步抽取数据到本地的作业:

```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "hbase20xsqlreader",  
                    "parameter": {
                        "queryServerAddress": "http://127.0.0.1:8765", 
                        "serialization": "PROTOBUF",  
                        "table": "TEST",    
                        "column": ["ID", "NAME"],   
                        "splitKey": "ID"   
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "encoding": "UTF-8",
                        "print": true
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": "3"
            }
        }
    }
}
```


### 3.2 参数说明

| 配置项             | 是否必须 | 默认值   | 描述                                                                                          |
| :----------------- | :------: | -------- | --------------------------------------------------------------------------------------- |
| queryServerAddress |    是    | 无       | Phoenix QueryServer 地址, 该插件通过 PQS 进行连接                                             |
| serialization      |    否    | PROTOBUF | QueryServer使用的序列化协议                                                                   |
| table              |    是    | 无       | 所要读取表名                                                                                  |
| schema             |    否    | 无       | 表所在的schema                                                                                |
| column             |    否    | 全部列   | 填写需要从phoenix表中读取的列名集合，使用JSON的数组描述字段信息，空值表示读取所有列           |
| splitKey           |    是    | 无       | 根据数据特征动态指定切分点，对表数据按照指定的列的最大、最小值进行切分,仅支持整型和字符串类型 |
| splitPoints        |    否    | 无       | 按照表的split进行切分                                                                         |
| where              |    否    | 无       | 支持对表查询增加过滤条件，每个切分都会携带该过滤条件                                          |
| querySql           |    否    | 无       | 支持指定多个查询语句，但查询列类型和数目必须保持一致                                          |

    
### 3.3 类型转换

目前hbase20xsqlreader支持大部分Phoenix类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出MysqlReader针对Mysql类型转换列表:


| DataX 内部类型 | Phoenix 数据类型                   |
| -------------- | ---------------------------------- |
| String         | CHAR, VARCHAR                      |
| Bytes          | BINARY, VARBINARY                  |
| Bool           | BOOLEAN                            |
| Long           | INTEGER, TINYINT, SMALLINT, BIGINT |
| Double         | FLOAT, DECIMAL, DOUBLE,            |
| Date           | DATE, TIME, TIMESTAMP              |


## 4 性能报告

略

## 5 约束限制

* 切分表时切分列仅支持单个列，且该列必须是表主键
* 不设置splitPoint默认使用自动切分，此时切分列仅支持整形和字符型
* 表名和SCHEMA名及列名大小写敏感，请与Phoenix表实际大小写保持一致
* 仅支持通过Phoenix QeuryServer读取数据，因此您的Phoenix必须启动QueryServer服务才能使用本插件

## 6 FAQ

