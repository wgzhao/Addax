# ClickHouse Reader 

`ClickHouseReader` 插件支持从 [ClickHouse](https://clickhouse.tech)数据库读取数据。

## 配置

下面的配置文件表示从 ClickHouse 数据库读取指定的表数据并打印到终端

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
                        "column": ["*"],
                        "splitPk": "",
                        "connection": [
                            {
                                "table": [ "tbl"],
                                "jdbcUrl": [ "jdbc:clickhouse://127.0.0.1:8123/default"]
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

## 参数说明

`parameter` 配置项支持以下配置

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

## 支持的数据类型

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


## 限制

除上述罗列字段类型外，其他类型均不支持，如Array、Nested等
