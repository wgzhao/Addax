# HBase11xsql Writer

HBase11xsqlwriter 插件利用 [Phoniex](https://phoenix.apache.org)， 用于向 HBase 1.x 版本的数据库写入数据。

如果你希望通过调用原生接口写入数据，则需要使用[HBase11xWriter](../hbase11xwriter) 插件

如果 HBase 是 2.X 版本，则需要使用 [HBase20xsqlwriter](../hbase20xsqlwriter) 插件

## 配置样例

```json
--8<-- "job/hbase11xsqlwriter.json"
```

## 参数说明

| 配置项                 | 是否必须 | 默认值 | 描述                                                                                                          |
| :--------------------- | :------: | ------ | ----------------------------------------------------------------------------------------------------------|
| hbaseConfig            |    是    | 无     | hbase集群地址，zk为必填项，格式：`ip1,ip2,ip3[:port]`，`znode` 是可选的，默认值是 `/hbase`                                    |
| table                  |    是    | 无     | 要导入的表名，大小写敏感，通常phoenix表都是 **大写** 表名                                                             |
| column                 |    是    | 无     | 列名，大小写敏感，通常phoenix的列名都是 **大写** ,数据类型无需填写,会自动获取列                                       |
| batchSize              |    否    | 256    | 一次写入的最大记录数                                                                                                |
| nullMode               |    否    | skip   | 读取到的列值为null时，如何处理。支持 `skip`, `empty`,前者表示跳过该列,后者表示插入空值,数值类型为0,字符类型为`null` |
| haveKerberos           |    否    | false  | 是否启用Kerberos认证, true 表示启用, false 表示不启用                                                               |
| kerberosPrincipal      |    否    | null   | kerberos 凭证信息，仅当 `havekerberos` 启用后有效                                                                   |
| kerberosKeytabFilePath |    否    | null   | kerberos 凭证文件的绝对路径，仅当 `havekerberos` 启用后有效                                                         |

注意：启用kerberos认证后，程序需要知道`hbase-site.xml` 所在的路径，一种办法是运行执行在环境变量 `CLASSPATH` 中增加该文件的所在路径。

另外一个解决办法是将 `hbase-site.xml` 文件拷贝到插件的 `libs` 目录里。 
