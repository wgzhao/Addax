# SqlServerWriter 插件文档

## 1 快速介绍

SqlServerWriter 插件实现了写入数据到 SqlServer 库的目的表的功能。在底层实现上， SqlServerWriter 通过 JDBC 连接远程 SqlServer 数据库，并执行相应的 insert into ...  sql 语句将数据写入 SqlServer，内部会分批次提交入库。

SqlServerWriter 面向ETL开发工程师，他们使用 SqlServerWriter 从数仓导入数据到 SqlServer。同时 SqlServerWriter 亦可以作为数据迁移工具为DBA等用户提供服务。

## 2 实现原理

SqlServerWriter 通过 DataX 框架获取 Reader 生成的协议数据，根据你配置生成相应的SQL语句 `insert into...`(当主键/唯一性索引冲突时会写不进去冲突的行)

注意：

1. 目的表所在数据库必须是主库才能写入数据；整个任务至少需具备 `insert into...` 的权限，是否需要其他权限，取决于你任务配置中在 `preSql` 和 `postSql` 中指定的语句。
2. SqlServerWriter和MysqlWriter不同，不支持配置writeMode参数。

## 3 功能说明

### 3.1 配置样例

这里使用一份从内存产生到 SqlServer 导入的数据。

```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": 5
            }
        },
        "content": [
            {
                "reader": {},
                "writer": {
                    "name": "sqlserverwriter",
                    "parameter": {
                        "username": "root",
                        "password": "root",
                        "column": [
                            "db_id",
                            "db_type",
                            "db_ip",
                            "db_port",
                            "db_role",
                            "db_name",
                            "db_username",
                            "db_password",
                            "db_modify_time",
                            "db_modify_user",
                            "db_description",
                            "db_tddl_info"
                        ],
                        "connection": [
                            {
                                "table": [
                                    "db_info_for_writer"
                                ],
                                "jdbcUrl": "jdbc:sqlserver://[HOST_NAME]:PORT;DatabaseName=[DATABASE_NAME]"
                            }
                        ],
                        "preSql": [
                            "delete from @table where db_id = -1;"
                        ],
                        "postSql": [
                            "update @table set db_modify_time = now() where db_id = 1;"
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
| jdbcUrl         |    是    | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接附件控制信息 |
| username        |    是    | 无     | 数据源的用户名 |
| password        |    是    | 无     | 数据源指定用户名的密码 |
| passflag        |    否    | true   | 是否强制需要密码，设置为false时，连接数据库将会忽略`password` 配置项 |
| table           |    是    | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column          |    是    | 无     |  所配置的表中需要同步的列名集合，详细描述见[rdbmswriter](../writer/rdbmswriter.md) |
| splitPk         |    否    | 无     | 使用splitPk代表的字段进行数据分片，详细描述见[rdbms](../reader/rdbmsreader.md)|
| preSql | 否  | 无 | 数据写入前先执行的sql语句 |
| postSql        |   否      | 无    | 数据写入完成后,再执行的SQL语句 |
| batchSize       |    否    | 1024   |  定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 DataX 出现OOM |

### 3.3 类型转换

类似 SqlServerReader ，目前 SqlServerWriter 支持大部分 SqlServer 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。
