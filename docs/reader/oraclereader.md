# Oracle Reader

Oracle Reader 插件用于从 Oracle 读取数据

## 配置样例

配置一个从Oracle数据库同步抽取数据到本地的作业:

=== "job/oracle2stream.json"

  ```json
  --8<-- "jobs/oraclereader.json"
  ```

## 参数说明

该插件基于 [RDBMS Reader](../rdbmsreader) 实现，因此可以参考 RDBMS Reader 的所有配置项。


## 对 GEOMETRY 类型的支持

从 Addax `4.0.13` 开始，实验性的支持 Oracle GEOMETRY 类型，该插件会把该类型的数据转为 JSON 数组字符串。

假定你有这样的的表和数据

```sql
--8<-- "assets/sql/oracle_geom.sql
```

读取表该的数据的最后输出结果类似如下：

```
--8<-- "assets/output/oracle_geom_reader.txt
```

注意：该数据类型目前还处于实验支持阶段，作者对此数据类型的理解并不深刻，也未经过全面的测试，请勿直接在生产环境使用。
