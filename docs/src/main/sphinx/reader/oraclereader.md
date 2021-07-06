# OracleReader 插件文档

## 1 快速介绍

OracleReader插件实现了从Oracle读取数据。在底层实现上，OracleReader通过JDBC连接远程Oracle数据库，并执行相应的sql语句将数据从Oracle库中SELECT出来。

## 2 实现原理

简而言之，OracleReader通过JDBC连接器连接到远程的Oracle数据库，并根据用户配置的信息生成查询SELECT SQL语句并发送到远程Oracle数据库， 并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，OracleReader将其拼接为SQL语句发送到Oracle数据库；对于用户配置querySql信息，Oracle直接将其发送到Oracle数据库。

## 3 功能说明

### 3.1 配置样例

配置一个从Oracle数据库同步抽取数据到本地的作业:

```json
{
  "job": {
    "setting": {
      "speed": {
        "byte": 1048576,
        "channel": 1
      }
    },
    "content": [
      {
        "reader": {
          "name": "oraclereader",
          "parameter": {
            "username": "root",
            "password": "root",
            "column": [
              "id",
              "name"
            ],
            "splitPk": "db_id",
            "connection": [
              {
                "table": [
                  "table"
                ],
                "jdbcUrl": [
                  "jdbc:oracle:thin:@<HOST_NAME>:PORT:<DATABASE_NAME>"
                ]
              }
            ]
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

| 配置项    | 是否必须 | 默认值 | 描述                                                                                                                                                                        |
| :-------- | :------: | ------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| jdbcUrl   |    是    | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接[附件控制信息](http://www.oracle.com/technetwork/database/enterprise-edition/documentation/index.html) ｜ |
| username  |    是    | 无     | 数据源的用户名                                                                                                                                                              |
| password  |    否    | 无     | 数据源指定用户名的密码                                                                                                                                                      |
| table     |    是    | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构                                                                                 |
| column    |    是    | 无     | 所配置的表中需要同步的列名集合，详细描述见[rdbmsreader](rdbmsreader.md)                                                                                                     |
| splitPk   |    否    | 无     | 使用splitPk代表的字段进行数据分片，DataX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能                                                                      |
| autoPk    |    否    | false | 是否自动猜测分片主键，`3.2.6` 版本引入 |
| where     |    否    | 无     | 针对表的筛选条件                                                                                                                                                            |
| querySql  |    否    | 无     | 使用自定义的SQL而不是指定表来获取数据，当配置了这一项之后，DataX系统就会忽略 `table`，`column`这些配置项                                                                    |
| fetchSize |    否    | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 DataX 出现OOM                                                                                              |
| session   |    否    | 无     | 针对本地连接,修改会话配置,详见下文                                                                                                                                          |

#### session

控制写入数据的时间格式，时区等的配置，如果表中有时间字段，配置该值以明确告知写入 oracle 的时间格式。通常配置的参数为：`NLS_DATE_FORMAT`,`NLS_TIME_FORMAT`。其配置的值为 `json` 格式，例如：

```json
"session": [
"alter session set NLS_DATE_FORMAT='yyyy-mm-dd hh24:mi:ss'",
"alter session set NLS_TIMESTAMP_FORMAT='yyyy-mm-dd hh24:mi:ss'",
"alter session set NLS_TIMESTAMP_TZ_FORMAT='yyyy-mm-dd hh24:mi:ss'",
"alter session set TIME_ZONE='Asia/Chongqing'"
]
```

注意 `&quot;`是 `"` 的转义字符串

### 3.3 类型转换

目前OracleReader支持大部分Oracle类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出OracleReader针对Oracle类型转换列表:

| DataX 内部类型 | Oracle 数据类型                                                                                                                                                                                               |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Long           | NUMBER, INTEGER, INT, SMALLINT                                                                                                                                                                                |
| Double         | NUMERIC, DECIMAL, FLOAT, DOUBLE PRECISION, REAL                                                                                                                                                               |
| String         | LONG ,CHAR, NCHAR, VARCHAR, VARCHAR2, NVARCHAR2, CLOB, NCLOB, CHARACTER, CHARACTER VARYING, CHAR VARYING, NATIONAL CHARACTER, NATIONAL CHAR, NATIONAL CHARACTER VARYING, NATIONAL CHAR VARYING, NCHAR VARYING |
| Date           | TIMESTAMP, DATE                                                                                                                                                                                               |
| Boolean        | bit, bool                                                                                                                                                                                                     |
| Bytes          | BLOB, BFILE, RAW, LONG RAW                                                                                                                                                                                    |

请注意: 除上述罗列字段类型外，其他类型均不支持

### 数据库编码问题

OracleReader底层使用JDBC进行数据抽取，JDBC天然适配各类编码，并在底层进行了编码转换。因此OracleReader不需用户指定编码，可以自动获取编码并转码。

对于Oracle底层写入编码和其设定的编码不一致的混乱情况，OracleReader对此无法识别，对此也无法提供解决方案，对于这类情况，**导出有可能为乱码**。
