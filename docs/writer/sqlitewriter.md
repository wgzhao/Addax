# SQLite Writer

SQLite Writer 插件实现了写入数据到 [SQLite](https://sqlite.org/index.html) 数据库的功能。

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

该插件基于 [RDBMS Writer](../rdbmswriter) 实现，因此可以参考 RDBMS Writer 的所有配置项。因为 SQLite 连接无需账号密码，因此其他数据库写入插件需要配置的 `username`, `password` 在这里不需要。

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
