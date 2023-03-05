# Postgresql Writer

PostgresqlWriter 插件实现了写入数据到 [PostgreSQL](https://postgresql.org) 数据库库表的功能。

## 示例

以下配置演示从 postgresql 指定的表读取数据，并插入到具有相同表结构的另外一张表中，用来测试该插件所支持的数据类型。

### 表结构信息

假定建表语句以及输入插入语句如下：

```sql
--8<-- "sql/postgresql.sql"
```

创建需要插入的表的语句如下:

```sql
create table addax_tbl1 like addax_tbl;
```

### 任务配置

以下是配置文件

=== "job/pg2pg.json"

```json
--8<-- "jobs/pgwriter.json"
```

将上述配置文件保存为 `job/pg2pg.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/pg2pg.json
```

## 参数说明

| 配置项    | 是否必须 | 默认值 | 描述                                                                                                             |
| :-------- | :------: | ------ | ---------------------------------------------------------------------------------------------------------------- |
| jdbcUrl   |    是    | 无     | 对端数据库的 JDBC 连接信息，jdbcUrl 按照 RDBMS 官方规范，并可以填写连接 [附件控制信息][1]                        |
| username  |    是    | 无     | 数据源的用户名                                                                                                   |
| password  |    否    | 无     | 数据源指定用户名的密码                                                                                           |
| writeMode |    否    | insert | 写入模式，支持 insert, update 详见如下                                                                           |
| table     |    是    | 无     | 所选取的需要同步的表名,使用 JSON 数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构                    |
| column    |    是    | 无     | 所配置的表中需要同步的列名集合，详细描述见 [rdbmswriter](../rdbmswriter)                                         |
| preSql    |    否    | 无     | 执行数据同步任务之前率先执行的 sql 语句，目前只允许执行一条 SQL 语句，例如清除旧数据,涉及到的表可用 `@table`表示 |
| postSql   |    否    | 无     | 执行数据同步任务之后执行的 sql 语句，目前只允许执行一条 SQL 语句，例如加上某一个时间戳                           |
| batchSize |    否    | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数                                                                   |

[1]: http://jdbc.postgresql.org/documentation/93/connect.html

### writeMode

默认情况下， 采取 `insert into` 语法写入 postgresql 表，如果你希望采取主键存在时更新，不存在则写入的方式， 可以使用 `update` 模式。假定表的主键为 `id` ,则 `writeMode` 配置方法如下：

```json
"writeMode": "update(id)"
```

如果是联合唯一索引，则配置方法如下：

```json
"writeMode": "update(col1, col2)"
```

注： `update` 模式在 `3.1.6` 版本首次增加，之前版本并不支持。

## 类型转换

目前 PostgresqlWriter 支持大部分 PostgreSQL 类型，但也存在部分没有支持的情况，请注意检查你的类型。

下面列出 PostgresqlWriter 针对 PostgreSQL 类型转换列表:

| Addax 内部类型 | PostgreSQL 数据类型                                       |
| -------------- | --------------------------------------------------------- |
| Long           | bigint, bigserial, integer, smallint, serial              |
| Double         | double precision, money, numeric, real                    |
| String         | varchar, char, text, bit, inet,cidr,macaddr,uuid,xml,json |
| Date           | date, time, timestamp                                     |
| Boolean        | bool                                                      |
| Bytes          | bytea                                                     |

## 已知限制

除以上列出的数据类型外，其他数据类型理论上均为转为字符串类型，但不确保准确性
