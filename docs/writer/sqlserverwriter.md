# SQLServer Writer

SqlServerWriter 插件实现了写入数据到 [SQL Server](https://www.microsoft.com/en-us/sql-server/sql-server-downloads) 库表的功能。

## 配置样例

这里使用一份从内存产生到 SQL Server 导入的数据。

```json
--8<-- "jobs/sqlserverwriter.json"
```

## 参数说明

| 配置项    | 是否必须 | 默认值 | 描述                                                                                        |
| :-------- | :------: | ------ | ------------------------------------------------------------------------------------------- |
| jdbcUrl   |    是    | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接附件控制信息              |
| username  |    是    | 无     | 数据源的用户名                                                                              |
| password  |    否    | 无     | 数据源指定用户名的密码                                                                      |
| writeMode |    否    | insert | 写入方式，支持 insert， update，详见下文                                                    |
| table     |    是    | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column    |    是    | 无     | 所配置的表中需要同步的列名集合，详细描述见 [rdbmswriter][1]                                 |
| splitPk   |    否    | 无     | 使用splitPk代表的字段进行数据分片，详细描述见 [rdbmsreader][2]                              |
| preSql    |    否    | 无     | 数据写入前先执行的sql语句                                                                   |
| postSql   |    否    | 无     | 数据写入完成后,再执行的SQL语句                                                              |
| batchSize |    否    | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 Addax 出现OOM              |
| session   |    否    | 无     | 针对本地连接,修改会话配置                                                                   |

### writeMode

默认情况下， 采取 `insert into` 语法写入 SQL Server 表，如果你希望采取主键存在时更新，不存在则写入的方式，也就是 SQL Server 的 `MERGE INTO` 语法, 可以使用 `update` 模式。 假定表的主键为 `id`
,则 `writeMode` 配置方法如下：

```json
{
  "writeMode": "update(id)"
}
```

如果是联合唯一索引，则配置方法如下：

```json
{
  "writeMode": "update(col1, col2)"
}
```

注： `update` 模式在 `4.0.8` 版本首次增加，之前版本并不支持。

## 类型转换

类似 SqlServerReader ，目前 SqlServerWriter 支持大部分 SqlServer 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。


[1]: ../rdbmswriter
[2]: ../../reader/rdbmsreader