# SqlServer Reader

SqlServerReader插件用于从从SqlServer读取数据。

## 配置样例

配置一个从SqlServer数据库同步抽取数据到本地的作业:

```json
{
  "job": {
    "setting": {
      "speed": {
        "byte": -1,
        "channel": 1
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
              "*"
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

## 参数说明

| 配置项          | 是否必须 | 默认值 |         描述   |
| :-------------- | :------: | ------ |------------- |
| jdbcUrl         |    是    | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接附件控制信息 |
| username        |    是    | 无     | 数据源的用户名 |
| password        |    否    | 无     | 数据源指定用户名的密码 |
| table           |    是    | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column          |    是    | 无     |  所配置的表中需要同步的列名集合，详细描述见 [rdbmsreader](rdbmsreader) |
| splitPk         |    否    | 无     | 使用splitPk代表的字段进行数据分片，详细描述见 [rdbmsreader](rdbmsreader)|
| autoPk          |    否    | false | 是否自动猜测分片主键，`3.2.6` 版本引入 |
| where           |    否    | 无     | 针对表的筛选条件 |
| querySql        |    否    | 无     | 使用自定义的SQL而不是指定表来获取数据，当配置了这一项之后，Addax系统就会忽略 `table`，`column`这些配置项 |
| fetchSize       |    否    | 1024   |  定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 Addax 出现OOM |

## 类型转换

| Addax 内部类型| SqlServer 数据类型    |
| -------- | -----  |
| Long     |bigint, int, smallint, tinyint|
| Double   |float, decimal, real, numeric|
|String  |char,nchar,ntext,nvarchar,text,varchar,nvarchar(MAX),varchar(MAX)|
| Date     |date, datetime, time    |
| Boolean  |bit|
| Bytes    |binary,varbinary,varbinary(MAX),timestamp, image|

请注意:

- 除上述罗列字段类型外，其他类型均不支持
- timestamp类型作为二进制类型

