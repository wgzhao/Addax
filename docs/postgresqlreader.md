
# PostgresqlReader 插件文档

## 1 快速介绍

PostgresqlReader插件实现了从PostgreSQL读取数据。在底层实现上，PostgresqlReader通过JDBC连接远程PostgreSQL数据库，并执行相应的sql语句将数据从PostgreSQL库中SELECT出来。

## 2 实现原理

简而言之，PostgresqlReader通过JDBC连接器连接到远程的PostgreSQL数据库，并根据用户配置的信息生成查询SELECT SQL语句并发送到远程PostgreSQL数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，PostgresqlReader将其拼接为SQL语句发送到PostgreSQL数据库；对于用户配置querySql信息，PostgresqlReader直接将其发送到PostgreSQL数据库。

## 3 功能说明

### 3.1 配置样例

配置一个从PostgreSQL数据库同步抽取数据到本地的作业:

```json
{
    "job": {
        "setting": {
            "speed": {
                 "byte": 1048576
            }
        },
        "content": [
            {
                "reader": {
                    "name": "postgresqlreader",
                    "parameter": {
                        "username": "xx",
                        "password": "xx",
                        "column": [
                            "id"，"name"
                        ],
                        "splitPk": "id",
                        "connection": [
                            {
                                "table": [
                                    "table"
                                ],
                                "jdbcUrl": [
                "jdbc:postgresql://host:port/database"
                                ]
                            }
                        ]
                    }
                },
               "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print":true,
                    }
                }
            }
        ]
    }
}

```

### 3.2 参数说明

| 配置项    | 是否必须 | 默认值 | 描述                                                                                                                                   |
| :-------- | :------: | ------ | -----------------------------------------------------------------------------------------------------------------------------------|
| jdbcUrl   |    是    | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接[附件控制信息](http://jdbc.postgresql.org/documentation/93/connect.html)  |
| username  |    是    | 无     | 数据源的用户名                                                                                                                                 |
| password  |    是    | 无     | 数据源指定用户名的密码                                                                                                                         |
| passflag  |    否    | true   | 是否强制需要密码，设置为false时，连接数据库将会忽略`password` 配置项                                                                           |
| table     |    是    | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构                                                    |
| column    |    是    | 无     | 所配置的表中需要同步的列名集合，详细描述见[rdbmsreader](rdbmsreader.md)                                                                        |
| splitPk   |    否    | 无     | 使用splitPk代表的字段进行数据分片，DataX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能                                         |
| where     |    否    | 无     | 针对表的筛选条件                                                                                                                               |
| querySql  |    否    | 无     | 使用自定义的SQL而不是指定表来获取数据，当配置了这一项之后，DataX系统就会忽略 `table`，`column`这些配置项                                       |
| fetchSize |    否    | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 DataX 出现OOM                                                                 |

### 3.3 类型转换

目前PostgresqlReader支持大部分PostgreSQL类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出PostgresqlReader针对PostgreSQL类型转换列表:


| DataX 内部类型 | PostgreSQL 数据类型                          |
| -------------- | -------------------------------------------- |
| Long           | bigint, bigserial, integer, smallint, serial |
| Double         | double precision, money, numeric, real       |
| String         | varchar, char, text, bit, inet               |
| Date           | date, time, timestamp                        |
| Boolean        | bool                                         |
| Bytes          | bytea                                        |

请注意:

除上述罗列字段类型外，其他类型均不支持; `money`,`inet`,`bit` 需用户使用 `a_inet::varchar` 类似的语法转换
