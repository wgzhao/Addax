# OracleWriter 插件文档

## 1 快速介绍

OracleWriter 插件实现了写入数据到 Oracle 主库的目的表的功能。在底层实现上， OracleWriter 通过 JDBC 连接远程 Oracle 数据库，并执行相应的 `insert into ...` 语句将数据写入 Oracle，内部会分批次提交入库。

OracleWriter 面向 ETL 开发工程师，他们使用 OracleWriter 从数仓导入数据到 Oracle。同时 OracleWriter 亦可以作为数据迁移工具为DBA等用户提供服务。

## 2 实现原理

OracleWriter 通过 DataX 框架获取 Reader 生成的协议数据，根据你配置生成相应的SQL语句

注意：

1. 目的表所在数据库必须是主库才能写入数据；整个任务至少需具备 `insert into...`的权限，是否需要其他权限，取决于你任务配置中在 `preSql` 和 `postSql` 中指定的语句。
2. OracleWriter 和 `MysqlWriter`不同，不支持配置 `writeMode` 参数。

## 3 功能说明

### 3.1 配置样例

* 这里使用一份从内存产生到 Oracle 导入的数据。

```json
{
"job": {
    "setting": {
        "speed": {
            "channel": 1
        }
    },
    "content": [{
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
            "name": "oraclewriter",
            "parameter": {
                "username": "root",
                "password": "root",
                "column": [
                    "id",
                    "name"
                ],
                "preSql": [
                    "delete from test"
                ],
                "connection": [{
                    "jdbcUrl": "jdbc:oracle:thin:@[HOST_NAME]:PORT:[DATABASE_NAME]",
                    "table": ["test"]
                }]
            }
        }
    }]
}
}

```

### 3.2 参数说明

| 配置项    | 是否必须 | 默认值 | 描述                                                                                                                                                                        |
| :-------- | :------: | ------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| jdbcUrl   |    是    | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接[附件控制信息](http://www.oracle.com/technetwork/database/enterprise-edition/documentation/index.html) ｜ |
| username  |    是    | 无     | 数据源的用户名                                                                                                                                                              |
| password  |    是    | 无     | 数据源指定用户名的密码                                                                                                                                                      |
| passflag  |    否    | true   | 是否强制需要密码，设置为false时，连接数据库将会忽略`password` 配置项                                                                                                        |
| table     |    是    | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构                                                                                 |
| column    |    是    | 无     | 所配置的表中需要同步的列名集合，详细描述[rdbmswriter](rdbmswriter.md) ｜                                                                                                    |
| preSql    |    否    | 无     | 执行数据同步任务之前率先执行的sql语句，目前只允许执行一条SQL语句，例如清除旧数据,涉及到的表可用 `@table`表示                                                                |
| postSql   |    否    | 无     | 执行数据同步任务之后执行的sql语句，目前只允许执行一条SQL语句，例如加上某一个时间戳                                                                                          |
| batchSize |    否    | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 DataX 出现OOM或者目标数据库事务提交失败导致挂起                                                            |
| session   |    否    | 无     | 设置oracle连接时的session信息, 详见下文                                                                                                                                     |

#### session

描述：设置oracle连接时的session信息，格式示例如下：

```json
"session":[
    "alter session set nls_date_format = 'dd.mm.yyyy hh24:mi:ss';"
    "alter session set NLS_LANG = 'AMERICAN';"
]

```

### 3.3 类型转换

类似 [OracleReader](oraclereader.md) ，目前 OracleWriter 支持大部分 Oracle 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出 OracleWriter 针对 Oracle 类型转换列表:

| DataX 内部类型 | Oracle 数据类型                                                                                                                                                                                |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Long           | NUMBER,INTEGER,INT,SMALLINT                                                                                                                                                                    |
| Double         | NUMERIC,DECIMAL,FLOAT,DOUBLE PRECISION,REAL                                                                                                                                                    |
| String         | LONG,CHAR,NCHAR,VARCHAR,VARCHAR2,NVARCHAR2,CLOB,NCLOB,CHARACTER,CHARACTER VARYING,CHAR VARYING,NATIONAL CHARACTER,NATIONAL CHAR,NATIONAL CHARACTER VARYING,NATIONAL CHAR VARYING,NCHAR VARYING |
| Date           | TIMESTAMP,DATE                                                                                                                                                                                 |
| Boolean        | bit, bool                                                                                                                                                                                      |
| Bytes          | BLOB,BFILE,RAW,LONG RAW                                                                                                                                                                        |
