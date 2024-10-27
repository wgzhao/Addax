# HDFS Writer

HDFS Writer 提供向 HDFS 文件系统指定路径中写入 `TextFile` ， `ORCFile` , `Parquet` 等格式文件的能力， 文件内容可与 hive 中表关联。

## 配置样例

```json
--8<-- "jobs/hdfswriter.json"
```

## 参数说明

| 配置项                    | 是否必须 | 数据类型        | 默认值  | 说明                                                                                         |
|:-----------------------| :------: |-------------| ------- | -------------------------------------------------------------------------------------------- |
| path                   |    是    | string      | 无      | 要读取的文件路径                                                                             |
| defaultFS              |    是    | string      | 无      | 详述见下                                                                                     |
| fileType               |    是    | string      | 无      | 文件的类型，详述见下                                                                         |
| fileName               |    是    | string      | 无      | 要写入的文件名，用于当作前缀                                                                 |
| column                 |    是    | `list<map>` | 无      | 写入的字段列表                                                                               |
| writeMode              |    是    | string      | 无      | 写入模式，详述见下                                                                           |
| skipTrash              |    否    | boolean     | false   | 是否跳过垃圾回收站，和 `writeMode` 配置相关详见下面描述                                      |
| fieldDelimiter         |    否    | string      | `,`     | 文本文件的字段分隔符，二进制文件不需要指定该项                                               |
| encoding               |    否    | string      | `utf-8` | 文件的编码配置， 目前仅支持 `utf-8`                                                          |
| nullFormat             |    否    | string      | 无      | 定义表示为空的字符，例如如果用户配置: `"\\N"` ，那么如果源头数据是 `"\N"` ，视作 `null` 字段 |
| haveKerberos           |    否    | boolean     | false   | 是否启用 Kerberos 认证，如果启用，则需要同时配置以下两项                                     |
| kerberosKeytabFilePath |    否    | string      | 无      | 用于 Kerberos 认证的凭证文件路径, 比如 `/your/path/addax.service.keytab`                     |
| kerberosPrincipal      |    否    | string      | 无      | 用于 Kerberos 认证的凭证主体, 比如 `addax/node1@WGZHAO.COM`                                  |
| compress               |    否    | string      | 无      | 文件的压缩格式，详见下文                                                                     |
| hadoopConfig           |    否    | map         | 无      | 里可以配置与 Hadoop 相关的一些高级参数，比如HA的配置                                         |
| preShell               |    否    | `list`      | 无      | 写入数据前执行的shell命令，比如 `hive -e "truncate table test.hello"`                        |
| postShell             |    否    | `list`      | 无      | 写入数据后执行的shell命令，比如 `hive -e "select count(1) from test.hello"`                 |

### path

存储到 Hadoop hdfs文件系统的路径信息，HdfsWriter 会根据并发配置在 `Path` 目录下写入多个文件。为与hive表关联，请填写hive表在hdfs上的存储路径。 例：Hive上设置的数据仓库的存储路径为：`/user/hive/warehouse/`
，已建立数据库：`test`，表：`hello`； 则对应的存储路径为：`/user/hive/warehouse/test.db/hello` (如果建表时指定了`location` 属性，则依据该属性的路径)

### defaultFS

Hadoop hdfs 文件系统 namenode 节点地址。格式：`hdfs://ip:port` ；例如：`hdfs://127.0.0.1:9000` , 如果启用了HA，则为 servicename 模式，比如 `hdfs://sandbox`

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

该功能的实现方式为获取 Hadoop HDFS 的  `fs.trash.interval` 参数，如果该参数没有设置，或设置为0时，会在删除时，设置该参数为 10080 ，表示 7 天。

这样，进入回收站的文件会保留7天。

修改删除的默认行为是为了给因为错误的采集而导致删除的数据有挽回的机会。

#### compress

当 fileType（文件类型）为 csv 下的文件压缩方式，目前仅支持 gzip、bz2、zip、lzo、lzo_deflate、hadoop-snappy、framing-snappy 压缩；
值得注意的是，lzo 存在两种压缩格式：lzo 和 lzo_deflate，用户在配置的时候需要留心，不要配错了；

另外，由于 snappy 目前没有统一的 stream format，addax 目前只支持最主流的两种：hadoop-snappy（hadoop 上的 snappy stream format）
和 framing-snappy（google 建议的 snappy stream format）;

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

### preShell 与 postShell

引入 `preShell` 与 `postShell` 的目的是为了在写入数据前后执行一些额外的操作，比如在写入数据前清空表，写入数据后查询表的行数等。一个典型的生产环境场景时，采集的数据按日分区保存在 HDFS 上，
采集之前需要创建分区，这样就可以通过配置 `preShell` 来实现，比如 `hive -e "alter table test.hello add partition(dt='${logdate}')"`

## 类型转换

| Addax 内部类型 | HIVE 数据类型                       |
| -------------- | ----------------------------------- |
| Long           | TINYINT,SMALLINT,INT,INTEGER,BIGINT |
| Double         | FLOAT,DOUBLE,DECIMAL                |
| String         | STRING,VARCHAR,CHAR                 |
| Boolean        | BOOLEAN                             |
| Date           | DATE,TIMESTAMP                      |
| Bytes          | BINARY                              |

## 功能与限制

1. 目前不支持：`binary`、`arrays`、`maps`、`structs`、`union` 类型
