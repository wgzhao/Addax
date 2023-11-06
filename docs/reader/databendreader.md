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

DatabendReader 基于 [rdbmsreader](../rdbmsreader) 实现，因此可以参考 rdbmsreader 的所有配置项。

## 限制

暂无