# Hive Reader

HiveReader 插件实现了从 [Apache Hive](https://hive.apache.org) 数据库读取数据的能力

新增该插件的主要目的是解决使用 RDBMS Reader 插件读取 Hive 数据库时不能解决 Kerberos 认证的问题， 如果你的 Hive 数据库没有启用 Kerberos 认证，那么直接使用 [RDBMS Reader](../rdbmsreader/) 也可以。 如果启用了 Kerberos 认证，则可以使用该插件。

## 示例

我们在 Hive 的 test 库上创建如下表，并插入一条记录

```sql
--8<-- "sql/hive.sql"
```

下面的配置是读取该表到终端的作业:

=== "job/hive2stream.json"

  ```json
  --8<-- "jobs/hivereader.json"
  ```

将上述配置文件保存为   `job/hive2stream.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/hive2stream.json
```

## 参数说明

| 配置项          | 是否必须 | 类型       | 默认值 |         描述   |
| :-------------- | :------: | ------ |------------- |--------------|
| jdbcUrl         |    是    | list | 无     | 对端数据库的JDBC连接信息 |
| driver          |   否     |  string   | 无      | 自定义驱动类名，解决兼容性问题，详见下面描述 |
| username        |    是    | string | 无     | 数据源的用户名 |
| password        |    否    | string | 无     | 数据源指定用户名的密码，若无密码，可不指定 |
| table           |    是    | list | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column          |    是    | list | 无     |  所配置的表中需要同步的列名集合，详细描述 [rdbmreader](../rdbmsreader) |
| splitPk         |    否    | string | 无     | 使用splitPk代表的字段进行数据分片，详细描述见 [rdbmreader](../rdbmsreader)|
| where           |    否    | string | 无     | 针对表的筛选条件 |
| querySql        |    否    | list | 无     | 使用自定义的SQL而不是指定表来获取数据，当配置了这一项之后，Addax系统就会忽略 `table`，`column`这些配置项 |
| haveKerberos           |    否  | string  | 无         | 是否启用 Kerberos 认证，如果启用，则需要同时配置 `kerberosKeytabFilePath`，`kerberosPrincipal`  |
| kerberosKeytabFilePath |    否  | string  | 无         | 用于 Kerberos 认证的凭证文件路径, 比如 `/your/path/addax.service.keytab` |
| kerberosPrincipal      |    否  | string  | 无         | 用于 Kerberos 认证的凭证主体, 比如 `addax/node1@EXAMPLE.COM` |

### jdbcUrl

连接 Hive 的 JDBC URL 有多种写法，一种是直接指定 HiveServer/HiveServer2 服务的主机名和端口即可，比如： `jdbc:hive2://node1:10000/default`

如果你有多个 HiveServer/HiveServer2 服务，并采取用了服务发现，则可以通过指定 zookeeper 的方式来获得故障转移功能，类似如下：

```
jdbc:hive2://node1:2181,node2:2181,node3:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2
```

如果你的 Hive 启用了 Kerberos 认证，还需要在 URL 后指定 `principal` 参数，一般为 `principal=hive/_HOST@EXAMPLE.COM`，其中 `EXAMPLE.COM` 为 `realm` 值。

### driver

当前 Addax 采用的 Hive JDBC 驱动为 3.1.0 以上版本，驱动类名使用的 `org.apache.hive.jdbc.HiveDriver`， 如果当前的 Hive JDBC 驱动不兼容 Hive 数据库， 则可以通过以下步骤替换驱动。

**替换插件内置的驱动**

`rm -f plugin/reader/hivereader/lib/hive-jdbc-*.jar`

**拷贝兼容驱动到插件目录**

`cp hive-jdbc-<version>.jar plugin/reader/hivereader/lib/`

**指定驱动类名称**

在你的 json 文件类，配置 `"driver": "<your jdbc class name>"`

## 类型转换

目前 HiveReader 支持大部分 Hive 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出 HiveReader 针对 Hive 类型转换列表:

| Addax 内部类型| Hive 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, mediumint, int, bigint|
| Double   |float, double, decimal|
| String   |varchar, char, string   |
| Date     |date, timestamp    |
| Boolean  |boolean   |
| Bytes    |binary    |

