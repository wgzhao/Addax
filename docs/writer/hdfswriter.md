# Hdfs Writer

HdfsWriter 提供向HDFS文件系统指定路径中写入 `TEXTFile` ， `ORCFile` , `PARQUET` 等格式文件的能力， 文件内容可与 hive 中表关联。

## 配置样例

```json
--8<-- "jobs/hdfswriter.json"
```

## 参数说明

| 配置项                 | 是否必须 | 默认值              | 说明                                                      |
| :--------------------- | :------: | -------- |----------------------------------------------------------------|
| path                   |    是    | 无        | 要读取的文件路径
| defaultFS              |    是    | 无        | Hadoop hdfs 文件系统 `NAMENODE` 节点地址，如果配置了 HA 模式，则为 `defaultFS` 的值 |
| fileType               |    是    | 无        | 文件的类型                                                          |
| fileName               |    是    | 无        | 要写入的文件名，用于当作前缀
| column                 |    是    | 无        | 写入的字段列表                                                        |
| writeMode              |    是    | 无        | 写入模式，支持 `append`, `overwrite`, `nonConflict`                   |
| skipTrash              |    否    | false     | 是否跳过垃圾回收站，和 `writeMode` 配置相关详见下面描述                             |
| fieldDelimiter         |    否    | `,`        | 指定文本文件（即`fileType` 指定为 `text`)的字段分隔符，二进制文件不需要指定该项              |
| encoding               |    否    | `utf-8`    | 文件的编码配置， 目前仅支持 `utf-8`                                         |
| nullFormat             |    否    | 无         | 自定义哪些字符可以表示为空,例如如果用户配置: `"\\N"` ，那么如果源头数据是 `"\N"` ，视作 `null` 字段 |
| haveKerberos           |    否    | 无         | 是否启用 Kerberos 认证，如果启用，则需要同时配置 `kerberosKeytabFilePath`，`kerberosPrincipal` |
| kerberosKeytabFilePath |    否    | 无         | 用于 Kerberos 认证的凭证文件路径, 比如 `/your/path/addax.service.keytab`    |
| kerberosPrincipal      |    否    | 无         | 用于 Kerberos 认证的凭证主体, 比如 `addax/node1@WGZHAO.COM`
| compress               |    否    | 无         | 文件的压缩格式                                                        |
| hadoopConfig           |    否    | 无         | 里可以配置与 Hadoop 相关的一些高级参数，比如HA的配置                                |

### path

存储到 Hadoop hdfs文件系统的路径信息，HdfsWriter 会根据并发配置在 `Path` 目录下写入多个文件。为与hive表关联，请填写hive表在hdfs上的存储路径。 例：Hive上设置的数据仓库的存储路径为：`/user/hive/warehouse/`
，已建立数据库：`test`，表：`hello`； 则对应的存储路径为：`/user/hive/warehouse/test.db/hello` (如果建表时指定了`location` 属性，则依据该属性的路径)

### defaultFS

Hadoop hdfs文件系统 namenode 节点地址。格式：`hdfs://ip:port` ；例如：`hdfs://127.0.0.1:9000` , 如果启用了HA，则为 servicename 模式，比如 `hdfs://sandbox`

### fileType

描述：文件的类型，目前只支持用户配置为

- text 表示 Text file文件格式
- orc 表示 OrcFile文件格式
- parquet 表示 Parquet 文件格式
- rc 表示 Rcfile 文件格式
- seq 表示sequence file文件格式
- csv 表示普通hdfs文件格式（逻辑二维表）

### column

写入数据的字段，不支持对部分列写入。为与hive中表关联，需要指定表中所有字段名和字段类型， 其中：`name` 指定字段名，`type` 指定字段类型。

用户可以指定 `column` 字段信息，配置如下：

```json
{
  "column": [
    {
      "name": "userName",
      "type": "string"
    },
    {
      "name": "age",
      "type": "long"
    },
    {
      "name": "salary",
      "type": "decimal(8,2)"
    }
  ]
}
```

对于数据类型是 `decimal` 类型的，需要注意：

1. 如果没有指定精度和小数位，则使用默认的 `decimal(38,10)` 表示
2. 如果仅指定了精度但未指定小数位，则小数位用0表示，即 `decimal(p,0)`
3. 如果都指定，则使用指定的规格，即 `decimal(p,s)`

### writeMode

写入前数据清理处理模式：

- append，写入前不做任何处理，直接使用 `filename` 写入，并保证文件名不冲突。
- overwrite 如果写入目录存在数据，则先删除，后写入
- nonConflict，如果目录下有 `fileName` 前缀的文件，直接报错。

### skipTrash

当 `writeMode` 为 `overwrite` 模式时，当前要删除的文件或文件夹是否进入回收站，默认为进回收站，仅当配置为 `true` 时为直接删除。
刚功能的实现方式为获取 Hadoop HDFS 的  `fs.trash.interval` 参数，如果该参数没有设置，或设置为0时，会在删除时，设置该参数为 10080 ，表示 7 天。
这样，进入回收站的文件会保留7天。
修改删除的默认行为是为了给因为错误的采集而导致删除的数据有挽回的机会。

#### compress

描述：hdfs文件压缩类型，默认不填写意味着没有压缩。其中：text类型文件支持压缩类型有gzip、bzip2;orc类型文件支持的压缩类型有NONE、SNAPPY（需要用户安装SnappyCodec）

### hadoopConfig

`hadoopConfig` 里可以配置与 Hadoop 相关的一些高级参数，比如HA的配置

```json
{
  "hadoopConfig": {
    "dfs.nameservices": "cluster",
    "dfs.ha.namenodes.cluster": "nn1,nn2",
    "dfs.namenode.rpc-address.cluster.nn1": "node1.example.com:8020",
    "dfs.namenode.rpc-address.cluster.nn2": "node2.example.com:8020",
    "dfs.client.failover.proxy.provider.cluster": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
  }
}
```

这里的 `cluster` 表示 HDFS 配置成HA时的名字，也是 `defaultFS` 配置项中的名字 如果实际环境中的名字不是 `cluster` ，则上述配置中所有写有 `cluster` 都需要替换

#### haveKerberos

是否有Kerberos认证，默认 `false`, 如果用户配置true，则配置项 `kerberosKeytabFilePath`，`kerberosPrincipal` 为必填。

#### kerberosKeytabFilePath

Kerberos认证 keytab文件路径，绝对路径

#### kerberosPrincipal

描述：Kerberos认证Principal名，如 `xxxx/hadoopclient@xxx.xxx`

## 类型转换

| Addax 内部类型| HIVE 数据类型    |
| -------- | -----  |
| Long     | TINYINT,SMALLINT,INT,INTEGER,BIGINT |
| Double   | FLOAT,DOUBLE,DECIMAL |
| String   | STRING,VARCHAR,CHAR |
| Boolean  | BOOLEAN |
| Date     | DATE,TIMESTAMP |
| Bytes    | BINARY |

## 功能与限制

1. 目前不支持：`binary`、`arrays`、`maps`、`structs`、`union类型`
