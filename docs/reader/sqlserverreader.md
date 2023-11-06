# SQLServer Reader

SqlServerReader 插件用于从从 SQLServer 读取数据。

## 配置样例

配置一个从 SQLServer 数据库同步抽取数据到本地的作业:

=== "job/sqlserver2stream.json"

  ```json
  --8<-- "jobs/sqlserverreader.json"
  ```

## 参数说明

SqlServerReader 基于 [rdbmsreader](../rdbmsreader) 实现，因此可以参考 rdbmsreader 的所有配置项。

