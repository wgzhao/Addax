# Doris Reader

DorisReader 插件实现了从 Apache Doris 读取数据的能力，支持以下两种连接方式：

- `jdbc:arrow-flight-sql`：使用 Doris 的 Arrow Flight SQL 协议（推荐）
- `jdbc:mysql`：回退到 Doris 的 MySQL 兼容协议

如果 `jdbcUrl` 不是上述两种前缀，将直接报错。

## 示例

下面的配置示例使用 Arrow Flight SQL 协议从 Doris 读取数据到终端：

=== "job/doris2stream.json"

  ```json
  --8<-- "jobs/dorisreader.json"
  ```

将上述配置文件保存为 `job/doris2stream.json`。

### 执行采集命令

```shell
bin/addax.sh job/doris2stream.json
```

## 参数说明

该插件基于 [RDBMS Reader](../rdbmsreader) 实现，因此可以参考 RDBMS Reader 的所有配置项。

### jdbcUrl

- Arrow Flight SQL：
  `jdbc:arrow-flight-sql://<fe_host>:<port>?useServerPrepStmts=false&cachePrepStmts=true&useSSL=false&useEncryption=false`
- MySQL 兼容协议：
  `jdbc:mysql://<fe_host>:<port>/<db>`

### 连接模式说明

- 当使用 `jdbc:arrow-flight-sql` 前缀时，DorisReader 会启用 Arrow Flight SQL 协议。
- 当使用 `jdbc:mysql` 前缀时，DorisReader 会按 MySQLReader 的方式连接，相关行为与 MySQLReader 保持一致。

### JVM 兼容性

如果使用 Java 9 及以上版本运行 Arrow Flight SQL 协议，需在 JVM 参数中加入：

```shell
--add-opens=java.base/java.nio=ALL-UNNAMED
```

## 注意事项

- Arrow Flight SQL 与 MySQL 协议的参数不完全一致，请根据 Doris 官方文档进行配置。
