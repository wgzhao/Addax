# SQLite Writer 

SQLiteWriter 插件实现了写入数据到 SQLite 数据库的功能。

## 示例

假定要写入的表如下：

```sql
create table addax_tbl 
(
col1 varchar(20) ,
col2 int(4),
col3 datetime,
col4 boolean,
col5 binary
);
```

这里使用一份从内存产生到 Mysql 导入的数据。

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
          "name": "sqlitewriter",
          "parameter": {
            "writeMode": "insert",
            "column": [
              "*"
            ],
            "preSql": [
              "delete from @table"
            ],
            "connection": [
              {
                "jdbcUrl": "jdbc:sqlite://tmp/writer.sqlite3",
                "table": [
                  "addax_tbl"
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

将上述配置文件保存为  `job/stream2sqlite.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/stream2sqlite.json
```

## 参数说明

| 配置项          | 是否必须 | 类型  | 默认值 |         描述   |
| :-------------- | :------: | ------ |------------- |-------|
| jdbcUrl         |    是    | list | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范 |
| driver          |   否     |  string   | 无      | 自定义驱动类名，解决兼容性问题，详见下面描述 |
| table           |    是    | list | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column          |    是    | list | 无     |  所配置的表中需要同步的列名集合，详细描述见 [rdbmswriter](rdbmswriter) |
| session         | 否      | list | 空  | Addax在获取连接时，执行session指定的SQL语句，修改当前connection session属性 |
| preSql         |    否    | list  | 无     | 数据写入钱先执行的sql语句，例如清除旧数据,如果 Sql 中有你需要操作到的表名称，可用 `@table` 表示 |
| postSql        |   否      | list | 无    | 数据写入完成后执行的sql语句，例如加上某一个时间戳|
| writeMode       | 是 |     string | insert | 数据写入表的方式, `insert` 表示采用 `insert into` , `replace`表示采用`replace into`方式 `update` 表示采用 `ON DUPLICATE KEY UPDATE` 语句 |
| batchSize       |    否    | int | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 Addax 出现OOM或者目标数据库事务提交失败导致挂起 |

注： 因为 SQLite 连接无需账号密码，因此其他数据库写入插件需要配置的 `username`, `password` 在这里不需要。

## 类型转换

| Addax 内部类型| SQLite 数据类型    |
| -------- | -----  |
| Long     |integer |
| Double   |real|
| String   | varchar   |
| Date     |datetime  |
| Boolean  |bool   |
| Bytes    |blob, binary    |

