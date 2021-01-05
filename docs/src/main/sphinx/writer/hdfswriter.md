# DataX HdfsWriter 插件文档

## 1 快速介绍

HdfsWriter提供向HDFS文件系统指定路径中写入 `TEXTFile` ， `ORCFile` , `PARQUET` 文件， 文件内容可与 hive 中表关联。

## 2 功能与限制

1. 目前HdfsWriter仅支持 textfile ，orcfile， parquet 三种格式的文件，且文件内容存放的必须是一张逻辑意义上的二维表;
2. 由于HDFS是文件系统，不存在schema的概念，因此不支持对部分列写入;
3. 目前仅支持与以下Hive数据类型：
    - 数值型：TINYINT(txt或ORC), SMALLINT(txt或ORC), INT(orc或parquet), INTEGER(txt或ORC), BIGINT(txt或ORC), LONG(parquet), FLOAT(orc或parquet), DOUBLE(orc或parquet), DECIMAL(orc或TXT), DECIMAL(18.9) (
      只有PARQUET必须带精度)
    - 字符串类型：STRING(TXT/orc或parquet),VARCHAR(TXT/orc),CHAR(TXT/orC)
    - 布尔类型：BOOLEAN(TXT/orc或parquet)
    - 时间类型：DATE(TXT/orC),TIMESTAMP(TXT/orC)

**目前不支持：binary、arrays、maps、structs、union类型**

1. 对于Hive分区表目前仅支持一次写入单个分区;
2. 对于textfile需用户保证写入hdfs文件的分隔符**与在Hive上创建表时的分隔符一致**,从而实现写入hdfs数据与Hive表字段关联;
3. HdfsWriter实现过程是：首先根据用户指定的path，在path目录下，创建点开头(`.`)的临时目录，创建规则：`.path_<uuid>`；然后将读取的文件写入这个临时目录；全部写入后再将这个临时目录下的文件移动到用户指定目录（在创建文件时保证文件名不重复）; 最后删除临时目录。如果在中间过程发生网络中断等情况造成无法与hdfs建立连接，需要用户手动删除已经写入的文件和临时目录。
4. 目前插件中Hive版本为3.1.1，Hadoop版本为 3.1.1， ,在Hadoop 2.7.x, Hadoop 3.1.x 和 Hive 2.x, Hive 3.1.x 测试环境中写入正常；其它版本理论上都支持，但用于生产之前建议进一步测试；
5. 目前HdfsWriter支持Kerberos认证

## 3 功能说明

### 3.1 配置样例

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 2,
        "bytes": -1
      }
    },
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "value": "DataX",
                "type": "string"
              },
              {
                "value": 19890604,
                "type": "long"
              },
              {
                "value": "1989-06-04 00:00:00",
                "type": "date"
              },
              {
                "value": true,
                "type": "bool"
              },
              {
                "value": "test",
                "type": "bytes"
              }
            ],
            "sliceRecordCount": 1000
          },
          "writer": {
            "name": "hdfswriter",
            "parameter": {
              "defaultFS": "hdfs://xxx:port",
              "fileType": "orc",
              "path": "/user/hive/warehouse/writerorc.db/orcfull",
              "fileName": "xxxx",
              "column": [
                {
                  "name": "col1",
                  "type": "string"
                },
                {
                  "name": "col2",
                  "type": "int"
                },
                {
                  "name": "col3",
                  "type": "string"
                },
                {
                  "name": "col4",
                  "type": "boolean"
                },
                {
                  "name": "col5",
                  "type": "string"
                }
              ],
              "writeMode": "overwrite",
              "fieldDelimiter": "\u0001",
              "compress": "SNAPPY"
            }
          }
        }
      }
    ]
  }
}
```

### 3.2 配置项说明

| 配置项                 | 是否必须 | 默认值              |
| :--------------------- | :------: | ------------------- |
| path                   |    是    | 无                  |
| defaultFS              |    是    | 无                  |
| fileType               |    是    | 无                  |
| fileName               |    是    | 无                 |
| column                 |    是    | 默认类型为 `String` |
| writeMode              |    是    | 无                 |
| fieldDelimiter         |    是    | `,`                 |
| encoding               |    否    | `utf-8`             |
| nullFormat             |    否    | 无                  |
| haveKerberos           |    否    | 无                  |
| kerberosKeytabFilePath |    否    | 无                  |
| kerberosPrincipal      |    否    | 无                  |
| compress               |    否    | 无                  |
| hadoopConfig           |    否    | 无                  |

#### path

存储到Hadoop hdfs文件系统的路径信息，HdfsWriter 会根据并发配置在 `Path` 目录下写入多个文件。为与hive表关联，请填写hive表在hdfs上的存储路径。 例：Hive上设置的数据仓库的存储路径为：`/user/hive/warehouse/` ，已建立数据库：`test`，表：`hello`；
则对应的存储路径为：`/user/hive/warehouse/test.db/hello` (如果建表时指定了`location` 属性，则依据该属性的路径)

#### defaultFS

Hadoop hdfs文件系统namenode节点地址。格式：`hdfs://ip:port` ；例如：`hdfs://127.0.0.1:9000` , 如果启用了HA，则为 servicename 模式，比如 `hdfs://sandbox`

#### fileType

描述：文件的类型，目前只支持用户配置为

- text 表示 Text file文件格式
- orc 表示 OrcFile文件格式
- parquet 表示 Parquet 文件格式
- rc 表示 Rcfile 文件格式
- seq 表示sequence file文件格式
- csv 表示普通hdfs文件格式（逻辑二维表）

#### fileName

HdfsWriter写入时的文件名，实际执行时会在该文件名后添加随机的后缀作为每个线程写入实际文件名。

#### column

写入数据的字段，不支持对部分列写入。为与hive中表关联，需要指定表中所有字段名和字段类型， 其中：name指定字段名，type指定字段类型。

用户可以指定 column 字段信息，配置如下：

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
    }
  ]
}
```

#### writeMode

描述：hdfswriter写入前数据清理处理模式：

- append，写入前不做任何处理，DataX hdfswriter直接使用filename写入，并保证文件名不冲突。
- overwrite 如果写入目录存在数据，则先删除，后写入
- nonConflict，如果目录下有fileName前缀的文件，直接报错。

#### fieldDelimiter

hdfswriter写入时的字段分隔符， 需要用户保证与创建的Hive表的字段分隔符一致，否则无法在Hive表中查到数据，如果写入的文件格式为 orc， parquet ，rcfile 等二进制格式，则该参数并不起作用

#### encoding

写文件的编码配置，默认为 `utf-8` **慎重修改**

#### compress

描述：hdfs文件压缩类型，默认不填写意味着没有压缩。其中：text类型文件支持压缩类型有gzip、bzip2;orc类型文件支持的压缩类型有NONE、SNAPPY（需要用户安装SnappyCodec）

#### hadoopConfig

`hadoopConfig` 里可以配置与 Hadoop 相关的一些高级参数，比如HA的配置

```json
"hadoopConfig":{
"dfs.nameservices": "testDfs",
"dfs.ha.namenodes.testDfs": "nn01,nn02",
"dfs.namenode.rpc-address.testDfs.namenode1": "192.168.1.1",
"dfs.namenode.rpc-address.testDfs.namenode2": "192.168.1.2",
"dfs.client.failover.proxy.provider.testDfs": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
}
```

#### haveKerberos

是否有Kerberos认证，默认 `false`, 如果用户配置true，则配置项 `kerberosKeytabFilePath`，`kerberosPrincipal` 为必填。

#### kerberosKeytabFilePath

Kerberos认证 keytab文件路径，绝对路径

#### kerberosPrincipal

描述：Kerberos认证Principal名，如 `xxxx/hadoopclient@xxx.xxx`

### 3.3 类型转换

目前 HdfsWriter 支持大部分 Hive 类型，请注意检查你的类型。

下面列出 HdfsWriter 针对 Hive 数据类型转换列表:

| DataX 内部类型| HIVE 数据类型    |
| -------- | -----  |
| Long     |TINYINT,SMALLINT,INT,INTEGER,BIGINT |
| Double   |FLOAT,DOUBLE,DECIMAL |
| String   |STRING,VARCHAR,CHAR |
| Boolean  |BOOLEAN |
| Date     |DATE,TIMESTAMP |
