# PostgresqlWriter 插件文档

## 1 快速介绍

PostgresqlWriter插件实现了写入数据到 PostgreSQL主库目的表的功能。在底层实现上，PostgresqlWriter通过JDBC连接远程 PostgreSQL 数据库，并执行相应的 insert into ... sql 语句将数据写入 PostgreSQL，内部会分批次提交入库。

PostgresqlWriter面向ETL开发工程师，他们使用PostgresqlWriter从数仓导入数据到PostgreSQL。同时 PostgresqlWriter亦可以作为数据迁移工具为DBA等用户提供服务。

## 2 实现原理

PostgresqlWriter通过 DataX 框架获取 Reader 生成的协议数据，根据你配置生成相应的SQL插入语句 `insert into...`(当主键/唯一性索引冲突时会写不进去冲突的行)

## 3 功能说明

### 3.1 配置样例

这里使用一份从内存产生到 PostgresqlWriter导入的数据。

```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column" : [
                            {
                                "value": "DataX",
                                "type": "string"
                            },
                            {
                                "value": 19880808,
                                "type": "long"
                            },
                            {
                                "value": "1988-08-08 08:08:08",
                                "type": "date"
                            },
                            {
                                "value": true,
                                "type": "bool"
                            },
                            {
                                "value": "test",
                                "type": "bytes"
                            }
                        ],
                        "sliceRecordCount": 1000
                    }
                },
                "writer": {
                    "name": "postgresqlwriter",
                    "parameter": {
                        "username": "xx",
                        "password": "xx",
                        "column": [
                            "id",
                            "name"
                        ],
                        "preSql": [
                            "delete from test"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:postgresql://127.0.0.1:3002/datax",
                                "table": [
                                    "test"
                                ]
                            }
                        ]
                    }
                }
            }
        ]
    }
}
```

### 3.2 参数说明

| 配置项          | 是否必须 | 默认值 |         描述   |
| :-------------- | :------: | ------ |------------- |
| jdbcUrl         |    是    | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接[附件控制信息](http://jdbc.postgresql.org/documentation/93/connect.html)  ｜
| username        |    是    | 无     | 数据源的用户名 |
| password        |    是    | 无     | 数据源指定用户名的密码 |
| writeMode       |    否    | insert     | 写入模式，支持insert, update 详见如下 |
| passflag        |    否    | true   | 是否强制需要密码，设置为false时，连接数据库将会忽略`password` 配置项 |
| table           |    是    | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column          |    是    | 无     |  所配置的表中需要同步的列名集合，详细描述[rdbmswriter](rdbmswriter.md) |
| preSql         |    否    | 无     | 执行数据同步任务之前率先执行的sql语句，目前只允许执行一条SQL语句，例如清除旧数据,涉及到的表可用 `@table`表示 |
| postSql        |   否      | 无    | 执行数据同步任务之后执行的sql语句，目前只允许执行一条SQL语句，例如加上某一个时间戳|
| batchSize       |    否    | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 DataX 出现OOM或者目标数据库事务提交失败导致挂起 |

#### writeMode

默认情况下， 采取 `insert into ` 语法写入 postgresql 表，如果你希望采取主键存在时更新，不存在则写入的方式，
可以使用 `update` 模式。假定表的主键为 `id` ,则 `writeMode` 配置方法如下：

```
"writeMode": "update(id)"
```

如果是联合唯一索引，则配置方法如下：

```
"writeMode": "update(col1, col2)"
```

注： `update` 模式在 `3.1.6` 版本首次增加，之前版本并不支持。

### 3.3 类型转换

目前 PostgresqlWriter支持大部分 PostgreSQL类型，但也存在部分没有支持的情况，请注意检查你的类型。

下面列出 PostgresqlWriter针对 PostgreSQL类型转换列表:

| DataX 内部类型| PostgreSQL 数据类型    |
| -------- | -----  |
| Long     |bigint, bigserial, integer, smallint, serial |
| Double   |double precision, money, numeric, real |
| String   |varchar, char, text, bit|
| Date     |date, time, timestamp |
| Boolean  |bool|
| Bytes    |bytea|
