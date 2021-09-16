# ClickHouse Writer

ClickHouseWriter 插件用于了向 [ClickHouse](https://clickhouse.tech) 写入数据。 

## 示例

以下示例我们演示从 clickhouse 中读取一张表的内容，并写入到相同表结构的另外一张表中，用来测试插件所支持的数据结构

### 表结构以数据

假定要读取的表结构及数据如下：

```sql
--8<-- "sql/clickhouse.sql"
```

要写入的表采取和读取表结构相同，其建表语句如下：

```sql
create table ck_addax_writer as ck_addax;
```

## 配置

以下为配置文件

=== "job/clickhouse2clickhouse.json"

  ```json
  --8<-- "jobs/clickhousewriter.json"
  ```

将上述配置文件保存为   `job/clickhouse2clickhouse.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/clickhouse2clickhouse.json
```

## 参数说明

| 配置项    | 是否必须 | 默认值 | 描述                                                                                                        |
| :-------- | :------: | ------ | ------------------------------------------------------------------------------------------------------- |
| jdbcUrl   |    是    | 无     | ClickHouse JDBC 连接信息 ,可按照官方规范填写连接附件控制信息。具体请参看[ClickHouse官方文档][1]                 |
| username  |    是    | 无     | 数据源的用户名                                                                                                  |
| password  |    否    | 无     | 数据源指定用户名的密码                                                                                          |
| table     |    是    | 无     | 所选取的需要同步的表 ,当配置为多张表时，用户自己需保证多张表是同一schema结构                                    |
| column    |    是    | 无     | 所配置的表中需要同步的列名集合, 使用JSON的数组描述字段信息。用户使用 `*` 代表默认使用所有列配置，例如 `"['*']"` |
| batchSize |    否    | 2048   | 每次批量数据的条数                                                                                              |

[1]: https://github.com/yandex/clickhouse-jdbc