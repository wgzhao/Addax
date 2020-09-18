# RDBMS Writer 

RDBMSWriter 插件支持从传统 RDBMS 读取数据。这是一个通用关系数据库读取插件，可以通过注册数据库驱动等方式支持更多关系数据库读取。

同时 RDBMS Writer 又是其他关系型数据库读取插件的的基础类。以下读取插件均依赖该插件

- Oracle Writer
- MySQL Writer
- PostgreSQL Writer
- ClickHouse Writer
- SQLServer Writer


## 配置说明

配置一个写入RDBMS的作业。

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
                        "column": [
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
                    "name": "rdbmswriter",
                    "parameter": {
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:dm://ip:port/database",
                                "table": [
                                    "table"
                                ]
                            }
                        ],
                        "username": "username",
                        "password": "password",
                        "table": "table",
                        "column": [
                            "*"
                        ],
                        "preSql": [
                            "delete from XXX;"
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
| jdbcUrl         |    是    | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接附件控制信息 ｜
| username        |    是    | 无     | 数据源的用户名 |
| password        |    是    | 无     | 数据源指定用户名的密码 |
| passflag        |    否    | true   | 是否强制需要密码，设置为false时，连接数据库将会忽略`password` 配置项 |
| table           |    是    | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column          |    是    | 无     |  所配置的表中需要同步的列名集合，详细描述见后 ｜
| preSql         |    否    | 无     | 执行数据同步任务之前率先执行的sql语句，目前只允许执行一条SQL语句，例如清除旧数据,涉及到的表可用 `@table`表示 |
| postSql        |   否      | 无    | 执行数据同步任务之后执行的sql语句，目前只允许执行一条SQL语句，例如加上某一个时间戳|
| batchSize       |    否    | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 DataX 出现OOM或者目标数据库事务提交失败导致挂起 |

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

### jdbcUrl

`jdbcUrl` 配置除了配置必要的信息外，我们还可以在增加每种特定驱动的特定配置属性，这里特别提到我们可以利用配置属性对代理的支持从而实现通过代理访问数据库的功能。
比如对于 PrestoSQL 数据库的 JDBC 驱动而言，支持 `socksProxy` 参数，比如一个可能的 `jdbcUrl` 为 

`jdbc:presto://127.0.0.1:8080/hive?socksProxy=192.168.1.101:1081` 

大部分关系型数据库的 JDBC 驱动支持 `socksProxyHost,socksProxyPort` 参数来支持代理访问。也有一些特别的情况。

以下是各类数据库 JDBC 驱动所支持的代理类型以及配置方式

| 数据库 | 代理类型    | 代理配置                       |   例子        |
| ------| ----------| -----------------------------|--------------------|
| MySQL | socks     | socksProxyHost,socksProxyPort | `socksProxyHost=192.168.1.101&socksProxyPort=1081` |
| Presto | socks    | socksProxy   | `socksProxy=192.168.1.101:1081` |
| Presto | http     | httpProxy   | `httpProxy=192.168.1.101:3128` |
