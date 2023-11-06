# ClickHouse Writer

ClickHouseWriter 插件用于向 [ClickHouse](https://clickhouse.tech) 写入数据。 

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

ClickHouseWriter 基于 [rdbmswriter](../rdbmswriter) 实现，因此可以参考 rdbmswriter 的所有配置项。