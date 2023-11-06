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

GreenplumWriter 基于 [rdbmswriter](../rdbmswriter) 实现，因此可以参考 rdbmswriter 的所有配置项。