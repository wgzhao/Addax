# Sybase Writer

SybaseWriter 插件实现了写入数据到 [Sybase][1] 库表的功能。

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

| 配置项    | 是否必须 | 默认值 | 描述                                               |
| :-------- | :------: | ------ |--------------------------------------------------|
| jdbcUrl   |    是    | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接附件控制信息  |
| username  |    是    | 无     | 数据源的用户名                                          |
| password  |    否    | 无     | 数据源指定用户名的密码                                      |
| writeMode |    否    | insert | 写入方式，支持 insert， update，详见下文                      |
| table     |    是    | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column    |    是    | 无     | 所配置的表中需要同步的列名集合，详细描述见 [rdbmswriter][2]           |
| splitPk   |    否    | 无     | 使用splitPk代表的字段进行数据分片，详细描述见 [rdbmsreader][3]      |
| preSql    |    否    | 无     | 数据写入前先执行的sql语句                                   |
| postSql   |    否    | 无     | 数据写入完成后,再执行的SQL语句                                |
| batchSize |    否    | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 Addax 出现OOM     |
| session   |    否    | 无     | 针对本地连接,修改会话配置                                    |


## 类型转换

类似 SqlServerReader ，目前 SqlServerWriter 支持大部分 SqlServer 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

[1]: https://www.sap.com/products/technology-platform/sql-anywhere.html
[2]: ../rdbmswriter
[3]: ../../reader/rdbmsreader