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

| 配置项          | 是否必须 | 默认值 |
| :-------------- | :------: | ------ |
| jdbcUrl         |    是    | 无     |
| username        |    是    | 无     |
| password        |    是    | 无     |
| passflag        |    否    | true   |
| table           |    是    | 无     |
| column          |    是    | 无     |
| splitPk         |    否    | 无     |
| preSql         |    否    | 无     |
| postSql        |   否      | 无    |
| batchSize       |    否    | 1024   |

#### jdbcUrl

`jdbcUrl` 是到对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接附件控制信息。
请注意不同的数据库jdbc的格式是不同的，DataX会根据具体jdbc的格式选择合适的数据库驱动完成数据读取。
  
- db2格式 `jdbc:db2://ip:port/database`
- prestosql格式 `jdbc:presto://ip:port/catalog`
  
#### username

描述：数据源的用户名

#### password

数据源指定用户名的密码。

#### passflag

默认情况下， `rdbmsreader` 要求的 `username`, `password` 必须且不能为空。
但实际可能存在无密码的数据库，比如我们增加的 [prestosql](https://prestosql.io) 就存在这种情况。

因此当设置 `passflag` 为 `false` ，

则表示忽略 `password` 参数填写的内容，否则 `password` 不能为空

#### table

所选取的需要同步的表名

#### column

所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。用户使用 `*` 代表默认使用所有列配置，例如 `["*"]`。  
  
支持列裁剪，即列可以挑选部分列进行导出。

支持列换序，即列可以不按照表schema信息进行导出。

支持常量配置，用户需要按照JSON格式:

``["id", "`table`", "1", "'bazhen.csy'", "null", "to_char(a + 1)", "2.3" , "true"]``

- `id` 为普通列名
- `` `table` `` 为包含保留在的列名，
- `1` 为整形数字常量，
- `'bazhen.csy'`为字符串常量
- `null` 为空指针，注意，这里的 `null` 必须以字符串形式出现，即用双引号引用
- `to_char(a + 1)`为表达式，
- `2.3` 为浮点数，
- `true` 为布尔值，同样的，这里的布尔值也必须用双引号引用

Column必须显示填写，不允许为空！

#### preSql

执行数据同步任务之前率先执行的sql语句，目前只允许执行一条SQL语句，例如清除旧数据。

#### postSql

执行数据同步任务之后执行的sql语句，目前只允许执行一条SQL语句，例如加上某一个时间戳。

#### batchSize

一次性批量提交的记录数大小，该值可以极大减少DataX与RDBMS的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。

### 3.3 类型转换

类似 SqlServerReader ，目前 SqlServerWriter 支持大部分 SqlServer 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

## 4 性能报告

## 5 约束限制
