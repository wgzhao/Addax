# HBase20x SQL Writer

HBase20x SQL Writer 插件利用 Phoenix 向 HBase 2.x 写入数据。

如果 HBase 是 1.X 版本，则可以使用 [HBase11xsqlWriter](../hbase11xsqlwriter) 或[HBase11xWriter](../hbase11xwriter) 插件

## 配置样例

```json
--8<-- "jobs/hbase20xsqlwriter.json"
```

## 参数说明

| 配置项                 | 是否必须 | 数据类型 | 默认值 | 描述                                                        |
| :--------------------- | :------: | -------- | ------ | ----------------------------------------------------------- |
| jdbcUrl                |    是    | string   | 无     | Phoenix 连接地址                                            |
| table                  |    是    | string   | 无     | 所要读取表名                                                |
| schema                 |    否    | string   | 无     | 表所在的 schema                                             |
| batchSize              |    否    | int      | 256    | 一次批量写入的最大行数                                      |
| column                 |    否    | list     | 无     | 列名，大小写敏感，通常phoenix的列名都是**大写**             |
| nullMode               |    否    | string   | skip   | 读取的 null 值时，如何处理, 详述见下                        |
| haveKerberos           |    否    | boolean  | false  | 是否启用Kerberos认证, true 表示启用, false 表示不启用       |
| kerberosPrincipal      |    否    | string   | 无     | kerberos 凭证信息，仅当 `havekerberos` 启用后有效           |
| kerberosKeytabFilePath |    否    | string   | 无     | kerberos 凭证文件的绝对路径，仅当 `havekerberos` 启用后有效 |

### jdbcUrl

`queryServerAddress` 是满足 Phoenix 链接的地址，具体格式和要求可以参考[官方文档][1] ，其 jdbc 连接串格式如下：

```
jdbc:phoenix [ :<zookeeper quorum> [ :<port number> [ :<root node> [ :<principal> [ :<keytab file> ] ] ] ] ] 
```

- zookeeper quorum: zookeeper 集群地址，多个地址用逗号分隔，如：`node1,node2,node3`
- port number: zookeeper 集群端口，默认为 2181
- root node: zookeeper 集群根节点，默认为 `/hbase`，启用 kerberos 后，默认为 `/hbase-secure`
- principal: kerberos 凭证信息，仅当 `havekerberos` 启用后有效
- keytab file: kerberos 凭证文件的绝对路径，仅当 `havekerberos` 启用后有效

如果你希望通过连接 Phoenix Query Server (a.k.a PQS) ，则 JDBC 连接串如下：

```
jdbc:phoenix:thin:url=<scheme>://<server-hostname>:<port>[;option=value...]
```

- schema: 传输协议，`http` 或 `https`，默认为 `http`
- server-hostname: Phoenix Query Server 地址，如：`node1`
- port: Phoenix Query Server 端口，默认为 8765
- option: 可选参数，可以是多个，用逗号分隔，如：`option1=value1,option2=value2`

更详细的描述，可以参考[官方文档][2]

### nullMode

`skip` 表示不向hbase写这列；`empty`：写入 `HConstants.EMPTY_BYTE_ARRAY`，即`new byte [0]`


注意：启用kerberos认证后，程序需要知道`hbase-site.xml` 所在的路径，一种办法是运行执行在环境变量 `CLASSPATH` 中增加该文件的所在路径。

另外一个解决办法是将 `hbase-site.xml` 文件拷贝到插件的 `libs` 目录里。

[1]: http://phoenix.apache.org/index.html
[2]: https://phoenix.apache.org/documentation/queryserver/connect.html