# PostgreSQL Reader

PostgreSQL Reader 插件用于从 [PostgreSQL](https://postgresql.org) 读取数据

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

该插件基于 [RDBMS Reader](../rdbmsreader) 实现，因此可以参考 RDBMS Reader 的所有配置项。