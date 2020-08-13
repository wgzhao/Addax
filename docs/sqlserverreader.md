
# SqlServerReader 插件文档

## 1 快速介绍

SqlServerReader插件实现了从SqlServer读取数据。在底层实现上，SqlServerReader通过JDBC连接远程SqlServer数据库，并执行相应的sql语句将数据从SqlServer库中SELECT出来。

## 2 实现原理

简而言之，SqlServerReader通过JDBC连接器连接到远程的SqlServer数据库，并根据用户配置的信息生成查询SELECT SQL语句并发送到远程SqlServer数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，SqlServerReader将其拼接为SQL语句发送到SqlServer数据库；对于用户配置querySql信息，SqlServer直接将其发送到SqlServer数据库。

## 3 功能说明

### 3.1 配置样例

配置一个从SqlServer数据库同步抽取数据到本地的作业:

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
                    "name": "sqlserverreader",
                    "parameter": {
                        "username": "root",
                        "password": "root",
                        "column": [
                            "id"
                        ],
                        "splitPk": "db_id",
                        "connection": [
                            {
                                "table": [
                                    "table"
                                ],
                                "jdbcUrl": [
                                "jdbc:sqlserver://localhost:3433;DatabaseName=dbname"
                                ]
                            }
                        ]
                    }
                },
               "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": true,
                        "encoding": "UTF-8"
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
| where           |    否    | 无     |
| querySql        |    否    | 无     |
| fetchSize       |    否    | 1024   |
| session         |    否    | 无     |
| compress        |    否    | 无     |
| hadoopConfig    |    否    | 无     |
| csvReaderConfig |    否    | 无     |

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

#### splitPk

OracleReader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片，DataX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能。

推荐splitPk用户使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。

目前splitPk仅支持整形、字符串型数据切分，不支持浮点、日期等其他类型。
如果用户指定其他非支持类型，OracleReader将报错！

splitPk如果不填写，将视作用户不对单表进行切分，OracleReader使用单通道同步全量数据。

#### where

筛选条件，OracleReader 根据指定的 `column`、`table`、`where` 条件拼接SQL，并根据这个SQL进行数据抽取。
在实际业务场景中，往往会选择当天的数据进行同步，可以将 `where` 条件指定为 `logdate >= current_date()` 。
注意：不可以将 `where` 条件指定为 `limit 10`，`limit` 不是SQL的合法where子句。

where条件可以有效地进行业务增量同步。

#### querySql

在有些业务场景下，where 这一配置项不足以描述所筛选的条件，用户可以通过该配置型来自定义筛选 SQL。
当用户配置了这一项之后，DataX系统就会忽略 `table`，`column`这些配置型，直接使用这个配置项的内容对数据进行筛选，
例如需要进行多表 join 后同步数据，使用 `select a,b from table_a join table_b on table_a.id = table_b.id` 

#### fetchSize

该配置项定义了插件和数据库服务器端每次批量数据获取条数，该值决定了DataX和服务器端的网络交互次数，能够较大的提升数据抽取性能。

注意，该值过大(`>2048`)可能造成DataX进程OOM。

### 3.3 类型转换

目前SqlServerReader支持大部分SqlServer类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出SqlServerReader针对SqlServer类型转换列表:

| DataX 内部类型| SqlServer 数据类型    |
| -------- | -----  |
| Long     |bigint, int, smallint, tinyint|
| Double   |float, decimal, real, numeric|
|String  |char,nchar,ntext,nvarchar,text,varchar,nvarchar(MAX),varchar(MAX)|
| Date     |date, datetime, time    |
| Boolean  |bit|
| Bytes    |binary,varbinary,varbinary(MAX),timestamp|

请注意:

- 除上述罗列字段类型外，其他类型均不支持
- timestamp类型作为二进制类型

## 4 性能报告

暂无
