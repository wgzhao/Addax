# Postgresql Reader

PostgresqlReader 插件用于从 [PostgreSQL](https://postgresql.org) 读取数据

## 示例

假定建表语句以及输入插入语句如下：

```sql
--8<-- "sql/postgresql.sql"
```

配置一个从PostgreSQL数据库同步抽取数据到本地的作业:

=== "job/postgres2stream.json"

  ```json
  --8<-- "jobs/pgreader.json"
  ```

将上述配置文件保存为   `job/postgres2stream.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/postgres2stream.json
```

## 参数说明

| 配置项    | 是否必须 | 默认值 | 描述                                                                                                     |
| :-------- | :------: | ------ | -------------------------------------------------------------------------------------------------------- |
| jdbcUrl   |    是    | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接 [附件控制信息][1]                     |
| username  |    是    | 无     | 数据源的用户名                                                                                           |
| password  |    否    | 无     | 数据源指定用户名的密码                                                                                   |
| table     |    是    | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构              |
| column    |    是    | 无     | 所配置的表中需要同步的列名集合，详细描述见 [rdbmsreader](../rdbmsreader/)                                |
| splitPk   |    否    | 无     | 使用splitPk代表的字段进行数据分片，Addax因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能   |
| autoPk    |    否    | false  | 是否自动猜测分片主键，`3.2.6` 版本引入                                                                   |
| where     |    否    | 无     | 针对表的筛选条件                                                                                         |
| querySql  |    否    | 无     | 使用自定义的SQL而不是指定表来获取数据，当配置了这一项之后，Addax系统就会忽略 `table`，`column`这些配置项 |
| fetchSize |    否    | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 Addax 出现OOM                           |

[1]: http://jdbc.postgresql.org/documentation/93/connect.html

## 类型转换

| Addax 内部类型 | PostgreSQL 数据类型                                                    |
| -------------- | ---------------------------------------------------------------------- |
| Long           | bigint, bigserial, integer, smallint, serial                           |
| Double         | double precision, money, numeric, real                                 |
| String         | varchar, char, text, bit(>1), inet, cidr, macaddr, array,uuid,json,xml |
| Date           | date, time, timestamp                                                  |
| Boolean        | bool,bit(1)                                                            |
| Bytes          | bytea                                                                  |

## 已知限制

除上述罗列字段类型外，其他类型均不支持; 
