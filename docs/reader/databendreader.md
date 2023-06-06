# Databend Reader

DatabendReader 插件实现了从 [Databend](https://databend.rs) 读取数据

注意，databender 有兼容 MySQL 客户端的协议实现，因此你可以直接使用 [mysqlreader](../mysqlreader) 来读取 Databend 数据。
## 示例

我们可以通过如下方式启动 Databend 数据库

```shell
docker run  -tid  --rm  -p 8000:8000 \
   -e QUERY_DEFAULT_USER=databend \
   -e QUERY_DEFAULT_PASSWORD=databend \
   datafuselabs/databend
```

然后创建一张需要读取的表

```sql
(
	id int,
	name varchar(255),
	salary float,
	created_at datetime,
	updated_at datetime
);
```

并填充必要的数据


下面的配置是读取该表到终端的作业:

=== "job/databend2stream.json"

  ```json
  --8<-- "jobs/databend2stream.json"
  ```

将上述配置文件保存为   `job/databend2stream.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/databend2stream.json
```

## 参数说明

| 配置项          | 是否必须 | 类型       | 默认值 |         描述   |
| :-------------- | :------: | ------ |------------- |--------------|
| jdbcUrl         |    是    | list | 无     | 对端数据库的JDBC连接信息 |
| username        |    是    | string | 无     | 数据源的用户名 |
| password        |    否    | string | 无     | 数据源指定用户名的密码 |
| table           |    是    | list | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column          |    是    | list | 无     |  所配置的表中需要同步的列名集合，详细描述 [rdbmreader](../rdbmsreader) |
| where           |    否    | string | 无     | 针对表的筛选条件 |
| querySql        |    否    | list | 无     | 使用自定义的SQL而不是指定表来获取数据，当配置了这一项之后，Addax系统就会忽略 `table`，`column`这些配置项 |


## 类型转换

目前MysqlReader支持大部分Mysql类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出MysqlReader针对Mysql类型转换列表:

| Addax 内部类型| MySQL 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, mediumint, int, bigint|
| Double   |float, double, decimal|
| String   |varchar, char, tinytext, text, mediumtext, longtext, year   |
| Date     |date, datetime, timestamp, time    |
| Boolean  |bit, bool   |
| Bytes    |tinyblob, mediumblob, blob, longblob, varbinary    |

