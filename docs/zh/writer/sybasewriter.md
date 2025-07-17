# Sybase Writer

Sybase Writer 插件实现了写入数据到 [Sybase](https://en.wikipedia.org/wiki/Sybase) 库表的功能。

## 配置样例

我们可以用 Docker 容器来启动一个 Sybase 数据库

```shell
docker run -tid --rm  -h dksybase --name sybase  -p 5000:5000  ifnazar/sybase_15_7 bash /sybase/start
```

然后创建一张如下表

```sql
create table addax_writer 
(
	id int,
	name varchar(255),
	salary float(2),
	created_at datetime,
	updated_at datetime
);
```

再使用下面的任务配置文件

```json
--8<-- "jobs/sybasewriter.json"
```

## 参数说明

该插件基于 [RDBMS Writer](../rdbmswriter) 实现，因此可以参考 RDBMS Writer 的所有配置项。