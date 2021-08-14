# HDFS Reader 

HdfsReade r提供了读取分布式文件系统数据存储的能力。

目前HdfsReader支持的文件格式如下：

- textfile（text）
- orcfile（orc）
- rcfile（rc）
- sequence file（seq）
- Csv(csv)
- parquet

## 功能与限制

1. 支持textfile、orcfile、parquet、rcfile、sequence file和csv格式的文件，且要求文件内容存放的是一张逻辑意义上的二维表。
2. 支持多种类型数据读取(使用String表示)，支持列裁剪，支持列常量
3. 支持递归读取、支持正则表达式（`*`和 `?`）。
4. 支持常见的压缩算法，包括 GZIP， SNAPPY， ZLIB等。
5. 多个File可以支持并发读取。
6. 支持sequence file数据压缩，目前支持lzo压缩方式。
7. csv类型支持压缩格式有：gzip、bz2、zip、lzo、lzo_deflate、snappy。
8. 目前插件中Hive版本为 `3.1.1`，Hadoop版本为`3.1.1`, 在Hadoop `2.7.x`, Hadoop `3.1.x` 和Hive `2.x`, hive `3.1.x` 测试环境中写入正常；其它版本理论上都支持，但在生产环境使用前，请进一步测试；
9. 支持`kerberos` 认证


## 配置样例

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 3,
        "bytes": -1
      }
    },
    "content": [
      {
        "reader": {
          "name": "hdfsreader",
          "parameter": {
            "path": "/user/hive/warehouse/mytable01/*",
            "defaultFS": "hdfs://xxx:port",
            "column": [
              {
                "index": 0,
                "type": "long"
              },
              {
                "index": 1,
                "type": "boolean"
              },
              {
                "type": "string",
                "value": "hello"
              },
              {
                "index": 2,
                "type": "double"
              }
            ],
            "fileType": "orc",
            "encoding": "UTF-8",
            "fieldDelimiter": ","
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": true
          }
        }
      }
    ]
  }
}
```

## 配置项说明

| 配置项                 | 是否必须 | 默认值              |   说明          |
| :--------------------- | :------: | -------- |-------------------------  |
| path                   |    是    | 无        | 要读取的文件路径
| defaultFS              |    是    | 无        | Hadoop hdfs 文件系统 `NAMENODE` 节点地址，如果配置了 HA 模式，则为 `defaultFS` 的值 |
| fileType               |    是    | 无        | 文件的类型 |
| column                 |    是    | 无        | 读取字段列表 |
| fieldDelimiter         |    否    | `,`        | 指定文本文件的字段分隔符，二进制文件不需要指定该项 |
| encoding               |    否    | `utf-8`    | 读取文件的编码配置， 目前仅支持 `utf-8` |
| nullFormat             |    否    | 无         | 自定义哪些字符可以表示为空,例如如果用户配置: `"\\N"` ，那么如果源头数据是 `"\N"` ，视作 `null` 字段 |
| haveKerberos           |    否    | 无         | 是否启用 Kerberos 认证，如果启用，则需要同时配置 `kerberosKeytabFilePath`，`kerberosPrincipal`  | 
| kerberosKeytabFilePath |    否    | 无         | 用于 Kerberos 认证的凭证文件路径, 比如 `/your/path/addax.service.keytab` |
| kerberosPrincipal      |    否    | 无         | 用于 Kerberos 认证的凭证主体, 比如 `addax/node1@WGZHAO.COM`
| compress               |    否    | 无         | 指定要读取的文件的压缩格式 | 
| hadoopConfig           |    否    | 无         | 里可以配置与 Hadoop 相关的一些高级参数，比如HA的配置 |

### path

要读取的文件路径，如果要读取多个文件，可以使用正则表达式 `*`，注意这里可以支持填写多个路径：

1. 当指定单个Hdfs文件，HdfsReader暂时只能使用单线程进行数据抽取。二期考虑在非压缩文件情况下针对单个File可以进行多线程并发读取。
2. 当指定多个Hdfs文件，HdfsReader支持使用多线程进行数据抽取。线程并发数通过通道数指定。
3. 当指定通配符，HdfsReader尝试遍历出多个文件信息。例如: 指定 `/*` 代表读取 `/` 目录下所有的文件，指定 `/bazhen/*` 代表读取 bazhen 目录下游所有的文件。HdfsReader目前只支持 `*`和 `?` 作为文件通配符。

特别需要注意的是，Addax 会将一个作业下同步的所有的文件视作同一张数据表。用户必须自己保证所有的File能够适配同一套schema信息。并且提供给Addax权限可读。

### fileType

描述：文件的类型，目前只支持用户配置为

- text 表示 `textfile` 文件格式
- orc 表示 `orcfile` 文件格式
- rc 表示 `rcfile` 文件格式
- seq 表示 `sequence file` 文件格式
- csv 表示普通 hdfs 文件格式（逻辑二维表）
- parquet 表示 `parquet` 文件格式

特别需要注意的是，HdfsReader能够自动识别文件是 `orcfile`、`textfile` 或者还是其它类型的文件，但该项是必填项，HdfsReader则会只读取用户配置的类型的文件，忽略路径下其他格式的文件

另外需要注意的是，由于 `textfile` 和 `orcfile` 是两种完全不同的文件格式，所以HdfsReader对这两种文件的解析方式也存在差异，这种差异导致hive支持的复杂复合类型(比如map,array,struct,union)在转换为支持的String类型时，转换的结果格式略有差异，比如以map类型为例：

- `orcfile`: map类型转换成 string 类型后，结果为 `{job=80, team=60, person=70}`

- `textfile`: map类型转换成 string 类型后，结果为 `job:80,team:60,person:70`

从上面的转换结果可以看出，数据本身没有变化，但是表示的格式略有差异，所以如果用户配置的文件路径中要同步的字段在Hive中是复合类型的话，建议配置统一的文件格式。

如果需要统一复合类型解析出来的格式，我们建议用户在hive客户端将 `textfile` 格式的表导成 `orcfile` 格式的表

### column

读取字段列表，`type` 指定源数据的类型，`index` 指定当前列来自于文本第几列(以0开始)，`value` 指定当前类型为常量，不从源头文件读取数据，而是根据 `value` 值自动生成对应的列。

默认情况下，用户可以全部按照String类型读取数据，配置如下：

```json
{
  "column": [
    "*"
  ]
}
```

用户可以指定Column字段信息，配置如下：

```json
[
  {
    "type": "long",
    "index": 0,
    "description": "从本地文件文本第一列获取int字段"
  },
  {
    "type": "string",
    "value": "addax",
    "description": "HdfsReader内部生成alibaba的字符串字段作为当前字段"
  }
]
```

对于用户指定Column信息，type必须填写，index/value必须选择其一。


### compress

当fileType（文件类型）为csv下的文件压缩方式，目前仅支持 gzip、bz2、zip、lzo、lzo_deflate、hadoop-snappy、framing-snappy压缩；
值得注意的是，lzo存在两种压缩格式：lzo和lzo_deflate，用户在配置的时候需要留心，不要配错了；另外，由于snappy目前没有统一的stream
format，addax目前只支持最主流的两种：hadoop-snappy（hadoop上的snappy stream format）和 framing-snappy（google建议的snappy stream format）;

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

#### csvReaderConfig

读取CSV类型文件参数配置，Map类型。读取CSV类型文件使用的CsvReader进行读取，会有很多配置，不配置则使用默认值。

常见配置：

```json
{
  "csvReaderConfig": {
    "safetySwitch": false,
    "skipEmptyRecords": false,
    "useTextQualifier": false
  }
}
```

所有配置项及默认值,配置时 csvReaderConfig 的map中请 **严格按照以下字段名字进行配置**：

```
boolean caseSensitive = true;
char textQualifier = 34;
boolean trimWhitespace = true;
boolean useTextQualifier = true;//是否使用csv转义字符
char delimiter = 44;//分隔符
char recordDelimiter = 0;
char comment = 35;
boolean useComments = false;
int escapeMode = 1;
boolean safetySwitch = true;//单列长度是否限制100000字符
boolean skipEmptyRecords = true;//是否跳过空行
boolean captureRawRecord = true;
```

## 类型转换

| Addax 内部类型| Hive表 数据类型    |
| -------- | -----  |
| Long     |TINYINT, SMALLINT, INT, BIGINT|
| Double   |FLOAT, DOUBLE|
| String   |String, CHAR, VARCHAR, STRUCT, MAP, ARRAY, UNION, BINARY|
| Boolean  |BOOLEAN|
| Date     |Date, TIMESTAMP|
| Bytes     | BINARY |

其中：

* Long 是指Hdfs文件文本中使用整形的字符串表示形式，例如 `123456789`
* Double 是指Hdfs文件文本中使用Double的字符串表示形式，例如 `3.1415`
* Boolean 是指Hdfs文件文本中使用Boolean的字符串表示形式，例如 `true`、`false`。不区分大小写。
* Date 是指Hdfs文件文本中使用Date的字符串表示形式，例如 `2014-12-31`
* Bytes 是指HDFS文件中使用二进制存储的内容，比如一张图片的数据

特别提醒：

* Hive支持的数据类型 TIMESTAMP 可以精确到纳秒级别，所以 `textfile`、`orcfile` 中 `TIMESTAMP` 存放的数据类似于 `2015-08-21 22:40:47.397898389`，
  如果转换的类型配置为Addax的Date，转换之后会导致纳秒部分丢失，所以如果需要保留纳秒部分的数据，请配置转换类型为 `String` 类型。
  

##  FAQ

Q: 如果报java.io.IOException: Maximum column length of 100,000 exceeded in column...异常信息，说明数据源column字段长度超过了100000字符。

A: 需要在json的reader里增加如下配置

 ```json
{
  "csvReaderConfig": {
    "safetySwitch": false,
    "skipEmptyRecords": false,
    "useTextQualifier": false
  }
}
 ```

`safetySwitch = false`  表示单列长度不限制100000字符
