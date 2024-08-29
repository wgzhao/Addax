# MySQL Reader

MysqlReader 插件实现了从 MySQL 读取数据的能力

## 示例

我们在 MySQL 的 test 库上创建如下表，并插入一条记录

```sql
--8<-- "sql/mysql.sql"
```

下面的配置是读取该表到终端的作业:

=== "job/mysql2stream.json"

  ```json
  --8<-- "jobs/mysqlreader.json"
  ```

将上述配置文件保存为   `job/mysql2stream.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/mysql2stream.json
```

## 参数说明

该插件基于 [RDBMS Reader](../rdbmsreader) 实现，因此可以参考 RDBMS Reader 的所有配置项。

### driver

当前 Addax 采用的 MySQL JDBC 驱动为 8.0 以上版本，驱动类名使用的 `com.mysql.cj.jdbc.Driver`，而不是 `com.mysql.jdbc.Driver`。 如果你需要采集的 MySQL 服务低于 `5.6`，需要使用到 `Connector/J 5.1` 驱动，则可以采取下面的步骤：

**替换插件内置的驱动**

`rm -f plugin/reader/mysqlreader/libs/mysql-connector-java-*.jar`

**拷贝老的驱动到插件目录**

`cp mysql-connector-java-5.1.48.jar plugin/reader/mysqlreader/libs/`

**指定驱动类名称**

在你的 json 文件类，配置 `"driver": "com.mysql.jdbc.Driver"`

## 类型转换注意事项

* `tinyint(1)` 会视为整形
* `year` 被视为整形
* `bit` 如果是 `bit(1)` 被视为布尔类型，否则当作二进制类型


