# SQLite Writer

SQLiteWriter 插件实现了写入数据到 [SQLite](https://sqlite.org/index.html) 数据库的功能。

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

这里使用一份从内存产生到 SQLite 的数据。

=== "job/stream2sqlite.json"

```json
--8<-- "jobs/sqlitewriter.json"
```

将上述配置文件保存为 `job/stream2sqlite.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/stream2sqlite.json
```

## 参数说明

| 配置项    | 是否必须 | 类型   | 默认值 | 描述                                                                                              |
| :-------- | :------: | ------ | ------ | ------------------------------------------------------------------------------------------------- |
| jdbcUrl   |    是    | list   | 无     | 对端数据库的 JDBC 连接信息，jdbcUrl 按照 RDBMS 官方规范                                           |
| driver    |    否    | string | 无     | 自定义驱动类名，解决兼容性问题，详见下面描述                                                      |
| table     |    是    | list   | 无     | 所选取的需要同步的表名,使用 JSON 数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构     |
| column    |    是    | list   | 无     | 所配置的表中需要同步的列名集合，详细描述见 [rdbmswriter](../rdbmswriter)                          |
| session   |    否    | list   | 空     | Addax 在获取连接时，执行 session 指定的 SQL 语句，修改当前 connection session 属性                |
| preSql    |    否    | list   | 无     | 数据写入钱先执行的 sql 语句，例如清除旧数据,如果 sql 中有你需要操作到的表名称，可用 `@table` 表示 |
| postSql   |    否    | list   | 无     | 数据写入完成后执行的 sql 语句，例如加上某一个时间戳                                               |
| writeMode |    是    | string | insert | 数据写入表的方式, 详见下文                                                                        |
| batchSize |    否    | int    | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数                                                    |

注： 因为 SQLite 连接无需账号密码，因此其他数据库写入插件需要配置的 `username`, `password` 在这里不需要。

### writeMode

- `insert` 表示采用 `insert into`
- `replace`表示采用`replace into`方式
- `update` 表示采用 `ON DUPLICATE KEY UPDATE` 语句

## 类型转换

| Addax 内部类型 | SQLite 数据类型 |
| -------------- | --------------- |
| Long           | integer         |
| Double         | real            |
| String         | varchar         |
| Date           | datetime        |
| Boolean        | bool            |
| Bytes          | blob, binary    |
