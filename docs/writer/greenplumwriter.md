# Greenplum Writer

GreenplumWriter 插件使用 `COPY FROM` 语法 将数据写入 [Greenplum](https://greenplum.org) 数据库。

## 示例

以下配置演示从 greenplum 指定的表读取数据，并插入到具有相同表结构的另外一张表中，用来测试该插件所支持的数据类型。

```sql
--8<-- "sql/gp.sql"
```

创建需要插入的表的语句如下:

```sql
create table gp_test like addax_tbl;
```

### 任务配置

以下是配置文件

=== "job/pg2gp.json"

```json
--8<-- "jobs/gpwriter.json"
```

将上述配置文件保存为 `job/pg2gp.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/pg2gp.json
```

## 参数说明

| 配置项    | 是否必须 | 默认值 | 描述                                                                                                             |
| :-------- | :------: | ------ | ---------------------------------------------------------------------------------------------------------------- |
| jdbcUrl   |    是    | 无     | 对端数据库的 JDBC 连接信息，jdbcUrl 按照 RDBMS 官方规范，并可以填写连接 [附件控制信息][1]                        |
| username  |    是    | 无     | 数据源的用户名                                                                                                   |
| password  |    否    | 无     | 数据源指定用户名的密码                                                                                           |
| table     |    是    | 无     | 所选取的需要同步的表名,使用 JSON 数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构                    |
| column    |    是    | 无     | 所配置的表中需要同步的列名集合，详细描述见 [rdbmswriter](../rdbmswriter)                                         |
| preSql    |    否    | 无     | 执行数据同步任务之前率先执行的 sql 语句，目前只允许执行一条 SQL 语句，例如清除旧数据,涉及到的表可用 `@table`表示 |
| postSql   |    否    | 无     | 执行数据同步任务之后执行的 sql 语句，目前只允许执行一条 SQL 语句，例如加上某一个时间戳                           |
| batchSize |    否    | 1024   | 一次写入的记录数，这里表示 `COPY FROM` 指令后接收的记录数                                                        |

[1]: http://jdbc.postgresql.org/documentation/93/connect.html

### 类型转换

| Addax 内部类型 | Greenplum 数据类型                                        |
| -------------- | --------------------------------------------------------- |
| Long           | bigint, bigserial, integer, smallint, serial              |
| Double         | double precision, money, numeric, real                    |
| String         | varchar, char, text, bit, inet,cidr,macaddr,uuid,xml,json |
| Date           | date, time, timestamp                                     |
| Boolean        | bool                                                      |
| Bytes          | bytea                                                     |
