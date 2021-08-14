# Oracle Writer

OracleWriter 插件实现了写入数据到 Oracle 主库的目的表的功能。

## 配置样例

这里使用一份从内存产生到 Oracle 导入的数据。

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": -1
      }
    },
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "value": "Addax",
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
            "connection": [
              {
                "jdbcUrl": "jdbc:oracle:thin:@[HOST_NAME]:PORT:[DATABASE_NAME]",
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

## 参数说明

| 配置项    | 是否必须 | 默认值 | 描述                                                                                                                                                                        |
| :-------- | :------: | ------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| jdbcUrl   |    是    | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接 [附件控制信息](http://www.oracle.com/technetwork/database/enterprise-edition/documentation/index.html)  |
| username  |    是    | 无     | 数据源的用户名                                                                                                                                                              |
| password  |    否    | 无     | 数据源指定用户名的密码                                                                                                                                                      |
| writeMode |    否    | insert | 写入方式，支持 insert， update，详见下文 |
| table     |    是    | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构                                                                                 |
| column    |    是    | 无     | 所配置的表中需要同步的列名集合，详细描述见 [rdbmswriter](rdbmswriter) |                                                                                                    |
| preSql    |    否    | 无     | 执行数据同步任务之前率先执行的sql语句，目前只允许执行一条SQL语句，例如清除旧数据,涉及到的表可用 `@table`表示                                                                |
| postSql   |    否    | 无     | 执行数据同步任务之后执行的sql语句，目前只允许执行一条SQL语句，例如加上某一个时间戳                                                                                          |
| batchSize |    否    | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 Addax 出现OOM或者目标数据库事务提交失败导致挂起                                                            |
| session   |    否    | 无     | 设置oracle连接时的session信息, 详见下文                                                                                                                                     |

### writeMode

默认情况下， 采取 `insert into ` 语法写入 Oracle 表，如果你希望采取主键存在时更新，不存在则写入的方式，也就是 Oracle 的 `merge into` 语法， 可以使用 `update` 模式。假定表的主键为 `id` ,则 `writeMode` 配置方法如下：

```
"writeMode": "update(id)"
```

如果是联合唯一索引，则配置方法如下：

```
"writeMode": "update(col1, col2)"
```

注： `update` 模式在 `3.1.6` 版本首次增加，之前版本并不支持。

### session

描述：设置oracle连接时的session信息，格式示例如下：

```json
{
  "session": [
    "alter session set nls_date_format = 'dd.mm.yyyy hh24:mi:ss';",
    "alter session set NLS_LANG = 'AMERICAN';"
  ]
}
```

##类型转换


| Addax 内部类型 | Oracle 数据类型                                                                                  |
| -------------- | ---------------------------------------------------------------------------------------------- |
| Long           | NUMBER,INTEGER,INT,SMALLINT                                                                    |
| Double         | NUMERIC,DECIMAL,FLOAT,DOUBLE PRECISION,REAL                                                     |
| String         | LONG,CHAR,NCHAR,VARCHAR,VARCHAR2,NVARCHAR2,CLOB,NCLOB,CHARACTER,CHARACTER VARYING,CHAR VARYING |
| String         | NATIONAL CHARACTER,NATIONAL CHAR,NATIONAL CHARACTER VARYING,NATIONAL CHAR VARYING,NCHAR VARYING |
| Date           | TIMESTAMP,DATE                                                                                  |
| Boolean        | BIT, BOOL                                                                                       |
| Bytes          | BLOB,BFILE,RAW,LONG RAW                                                                         |
