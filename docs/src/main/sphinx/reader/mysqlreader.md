
# MysqlReader 插件文档

## 1 快速介绍

MysqlReader插件实现了从Mysql读取数据。在底层实现上，MysqlReader通过JDBC连接远程Mysql数据库，并执行相应的sql语句将数据从mysql库中SELECT出来。

不同于其他关系型数据库，MysqlReader不支持FetchSize

## 2 实现原理

简而言之，MysqlReader通过JDBC连接器连接到远程的Mysql数据库，并根据用户配置的信息生成查询SELECT SQL语句，然后发送到远程Mysql数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，MysqlReader将其拼接为SQL语句发送到Mysql数据库；对于用户配置querySql信息，MysqlReader直接将其发送到Mysql数据库。

## 3 功能说明

### 3.1 配置样例

配置一个从Mysql数据库同步抽取数据到本地的作业:

```json
{
"job": {
    "setting": {
        "speed": {
            "channel": 3
        }
    },
    "content": [{
        "reader": {
            "name": "mysqlreader",
            "parameter": {
                "username": "root",
                "password": "root",
                "column": [ "id","name" ],
                "splitPk": "db_id",
                "connection": [{
                    "table": ["table"],
                    "jdbcUrl": ["jdbc:mysql://127.0.0.1:3306/database"]
                }]
            }
        },
        "writer": {
            "name": "streamwriter",
            "parameter": {
                "print":true
            }
        }
    }]
}
}
```

### 3.2 参数说明

| 配置项          | 是否必须 | 默认值 |         描述   |
| :-------------- | :------: | ------ |------------- |
| jdbcUrl         |    是    | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接[附件控制信息](http://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html) ｜
| username        |    是    | 无     | 数据源的用户名 |
| password        |    是    | 无     | 数据源指定用户名的密码 |
| passflag        |    否    | true   | 是否强制需要密码，设置为false时，连接数据库将会忽略`password` 配置项 |
| table           |    是    | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column          |    是    | 无     |  所配置的表中需要同步的列名集合，详细描述[rdbmreader](rdbmsreader.md) ｜
| splitPk         |    否    | 无     | 使用splitPk代表的字段进行数据分片，详细描述见[rdbmreader](rdbmsreader.md)|
| where           |    否    | 无     | 针对表的筛选条件 |
| querySql        |    否    | 无     | 使用自定义的SQL而不是指定表来获取数据，当配置了这一项之后，DataX系统就会忽略 `table`，`column`这些配置项 |

### 3.3 类型转换

目前MysqlReader支持大部分Mysql类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出MysqlReader针对Mysql类型转换列表:

| DataX 内部类型| Mysql 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, mediumint, int, bigint|
| Double   |float, double, decimal|
| String   |varchar, char, tinytext, text, mediumtext, longtext, year   |
| Date     |date, datetime, timestamp, time    |
| Boolean  |bit, bool   |
| Bytes    |tinyblob, mediumblob, blob, longblob, varbinary    |

请注意:

* 除上述罗列字段类型外，其他类型均不支持
* `tinyint(1)` DataX视作为整形
* `year` DataX视作为字符串类型
* `bit` DataX属于未定义行为

### 3.4 数据库编码问题

Mysql本身的编码设置非常灵活，包括指定编码到库、表、字段级别，甚至可以均不同编码。优先级从高到低为字段、表、库、实例。我们不推荐数据库用户设置如此混乱的编码，最好在库级别就统一到UTF-8。

MysqlReader底层使用JDBC进行数据抽取，JDBC天然适配各类编码，并在底层进行了编码转换。因此MysqlReader不需用户指定编码，可以自动获取编码并转码。

对于Mysql底层写入编码和其设定的编码不一致的混乱情况，MysqlReader对此无法识别，对此也无法提供解决方案，对于这类情况，`导出有可能为乱码`。
