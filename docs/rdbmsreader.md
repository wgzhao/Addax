# RDBMSReader 插件文档

## 1 快速介绍

RDBMSReader插件实现了从RDBMS读取数据。在底层实现上，RDBMSReader通过JDBC连接远程RDBMS数据库，并执行相应的sql语句将数据从RDBMS库中SELECT出来。目前支持[db2](https://www.ibm.com/products/db2-database)、[presto](https://prestosql.io) 数据库的读取。

RDBMSReader是一个通用的关系数据库读插件，您可以通过注册数据库驱动等方式增加任意多样的关系数据库读支持。

## 2 实现原理

简而言之，RDBMSReader通过JDBC连接器连接到远程的RDBMS数据库，并根据用户配置的信息生成查询SELECT SQL语句并发送到远程RDBMS数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，RDBMSReader将其拼接为SQL语句发送到RDBMS数据库；对于用户配置querySql信息，RDBMS直接将其发送到RDBMS数据库。

## 3 功能说明

### 3.1 配置样例

配置一个从RDBMS数据库同步抽取数据作业:

```json
{
    "job": {
        "setting": {
            "speed": {
                "byte": 1048576
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0.02
            }
        },
        "content": [
            {
                "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "xxx",
                        "passflag": "true",
                        "password": "xxx",
                        "column": [
                            "id",
                            "name"
                        ],
                        "splitPk": "pk",
                        "connection": [
                            {
                                "table": [
                                    "table"
                                ],
                                "jdbcUrl": [
                                    "jdbc:dm://ip:port/database"
                                ]
                            }
                        ],
                        "fetchSize": 1024,
                        "where": "1 = 1"
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": true
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
| column          |    是    | 无     |  所配置的表中需要同步的列名集合，详细描述见后 |
| splitPk         |    否    | 无     | 使用splitPk代表的字段进行数据分片，DataX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能，注意事项见后|
| where           |    否    | 无     | 针对表的筛选条件 |
| querySql        |    否    | 无     | 使用自定义的SQL而不是指定表来获取数据，当配置了这一项之后，DataX系统就会忽略 `table`，`column`这些配置项 |
| fetchSize       |    否    | 1024   |  定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 DataX 出现OOM |

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

### 3.3 类型转换

目前RDBMSReader支持大部分通用得关系数据库类型如数字、字符等，但也存在部分个别类型没有支持的情况，请注意检查你的类型，根据具体的数据库做选择。


## 4. 当前支持的数据库

- [PrestoSQL](https://prestosql.io)
- [TDH Inceptor2](http://transwarp.io/transwarp/)
- [IBM DB2](https://www.ibm.com/analytics/db2)
- [Apache Hive](https://hive.apache.org)