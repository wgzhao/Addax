# SQLServer Writer

SQLServer Writer 插件实现了写入数据到 [SQL Server](https://www.microsoft.com/en-us/sql-server/sql-server-downloads) 库表的功能。

## 配置样例

这里使用一份从内存产生到 SQL Server 导入的数据。

```json
--8<-- "jobs/sqlserverwriter.json"
```

## 参数说明

该插件基于 [RDBMS Writer](../rdbmswriter) 实现，因此可以参考 RDBMS Writer 的所有配置项。

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
