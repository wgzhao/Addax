
# ClickHouseReader 插件文档

## 1 快速介绍

ClickHouseReader插件实现了从ClickHouse读取数据。在底层实现上，ClickHouseReader通过JDBC连接远程ClickHouse数据库，并执行相应的sql语句将数据从ClickHouse库中SELECT出来。

## 2 实现原理

简而言之，ClickHouseReader通过JDBC连接器连接到远程的ClickHouse数据库，并根据用户配置的信息生成查询SELECT SQL语句，然后发送到远程ClickHouse数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，ClickHouseReader将其拼接为SQL语句发送到ClickHouse数据库；对于用户配置querySql信息，ClickHouseReader直接将其发送到ClickHouse数据库。

## 3 功能说明

### 3.1 配置样例

配置一个从ClickHouse数据库同步抽取数据到本地的作业:

```json
{
    "job": {
        "setting": {
            "speed": {
                 "channel": 3
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0.02
            }
        },
        "content": [
            {
                "reader": {
                    "name": "clickhousereader",
                    "parameter": {
                        "username": "root",
                        "password": "root",
                        "column": [
                            "id",
                            "name"
                        ],
                        "splitPk": "db_id",
                        "connection": [
                            {
                                "table": [
                                    "table"
                                ],
                                "jdbcUrl": [
     "jdbc:clickhouse://127.0.0.1:8123/default"
                                ]
                            }
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

| 配置项          | 是否必须 | 默认值 |   描述          |
| :-------------- | :------: | ------ |-------------|
| jdbcUrl         |    是    | 无     | ClickHouse JDBC 连接信息 ,可按照官方规范填写连接附件控制信息。具体请参看[ClickHouse官方文档](https://github.com/yandex/clickhouse-jdbc) |
| username        |    是    | 无     | 数据源的用户名 |
| password        |    是    | 无     | 数据源指定用户名的密码 |
| passflag        |    否    | true   | 是否强制需要密码，填写为false时，则忽略password的填写内容 |
| table           |    是    | 无     | 所选取的需要同步的表 ,当配置为多张表时，用户自己需保证多张表是同一schema结构|
| column          |    是    | 无     |所配置的表中需要同步的列名集合, 使用JSON的数组描述字段信息。用户使用 `*` 代表默认使用所有列配置，例如 `"['*']"` |
| splitPk         |    否    | 无     | 希望使用splitPk代表的字段进行数据分片,DataX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能 |
| where           |    否    | 无     | 筛选条件 |
| querySql        |    否    | 无     | 使用SQL查询而不是直接指定表的方式读取数据，当用户配置querySql时，ClickHouseReader直接忽略table、column、where条件的配置 |

### 3.3 类型转换

目前ClickHouseReader支持大部分ClickHouse类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出ClickHouseReader针对ClickHouse类型转换列表:

| DataX 内部类型| ClickHouse 数据类型    |
| -------- | -----  |
| Long     |Uint8,Uint16,Uint32,Uint64,Int8,Int16,Int32,Int64,Enum8,Enum16|
| Double   |Float32,Float64,Decimal|
| String   |String,FixedString(N)|
| Date     |Date, Datetime |
| Boolean  |UInt8 |
| Bytes    |String|

请注意:

除上述罗列字段类型外，其他类型均不支持，如Array、Nested等
