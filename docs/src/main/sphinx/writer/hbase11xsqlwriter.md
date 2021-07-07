# HBase11xsqlwriter 插件文档

## 1. 快速介绍

HBase11xsqlwriter实现了向hbase中的SQL表(phoenix)批量导入数据的功能。Phoenix因为对rowkey做了数据编码，所以，直接使用HBaseAPI进行写入会面临手工数据转换的问题，麻烦且易错。本插件提供了单间的SQL表的数据导入方式。

在底层实现上，通过Phoenix的JDBC驱动，执行UPSERT语句向hbase写入数据。

### 1.1 支持的功能

支持带索引的表的数据导入，可以同步更新所有的索引表

### 1.2 限制

- 仅支持1.x系列的hbase
- 仅支持通过phoenix创建的表，不支持原生HBase表
- 不支持带时间戳的数据导入

## 2. 实现原理

通过Phoenix的JDBC驱动，执行UPSERT语句向表中批量写入数据。因为使用上层接口，所以，可以同步更新索引表。

## 3. 配置说明

### 3.1 配置样例

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "txtfilereader",
          "parameter": {
            "path": "/tmp/normal.txt",
            "charset": "UTF-8",
            "column": [
              {
                "index": 0,
                "type": "String"
              },
              {
                "index": 1,
                "type": "string"
              },
              {
                "index": 2,
                "type": "string"
              },
              {
                "index": 3,
                "type": "string"
              }
            ],
            "fieldDelimiter": ","
          }
        },
        "writer": {
          "name": "hbase11xsqlwriter",
          "parameter": {
            "batchSize": "256",
            "column": [
              "UID",
              "TS",
              "EVENTID",
              "CONTENT"
            ],
            "haveKerberos": "true",
            "kerberosPrincipal": "hive@EXAMPLE.COM",
            "kerberosKeytabFilePath": "/tmp/hive.headless.keytab",
            "hbaseConfig": {
              "hbase.zookeeper.quorum": "node1,node2,node3:2181",
              "zookeeper.znode.parent": "/hbase-secure"
            },
            "nullMode": "skip",
            "table": "TEST_TBL"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 5,
        "bytes": -1
      }
    }
  }
}
```

### 3.2 参数说明

| 配置项                 | 是否必须 | 默认值 | 描述                                                                                                          |
| :--------------------- | :------: | ------ | ----------------------------------------------------------------------------------------------------------|
| hbaseConfig            |    是    | 无     | hbase集群地址，zk为必填项，格式：`ip1,ip2,ip3[:port]`，znode是可选的，默认值是 `/hbase`                                    |
| table                  |    是    | 无     | 要导入的表名，大小写敏感，通常phoenix表都是**大写**表名                                                             |
| column                 |    是    | 无     | 列名，大小写敏感，通常phoenix的列名都是**大写**,数据类型无需填写,会自动获取列                                       |
| batchSize              |    否    | 256    | 一次写入的最大记录数                                                                                                |
| nullMode               |    否    | skip   | 读取到的列值为null时，如何处理。支持 `skip`, `empty`,前者表示跳过该列,后者表示插入空值,数值类型为0,字符类型为`null` |
| haveKerberos           |    否    | false  | 是否启用Kerberos认证, true 表示启用, false 表示不启用                                                               |
| kerberosPrincipal      |    否    | null   | kerberos 凭证信息，仅当 `havekerberos` 启用后有效                                                                   |
| kerberosKeytabFilePath |    否    | null   | kerberos 凭证文件的绝对路径，仅当 `havekerberos` 启用后有效                                                         |

注意：启用kerberos认证后，程序需要知道`hbase-site.xml` 所在的路径，一种办法是运行执行在环境变量 `CLASSPATH` 中增加该文件的所在路径。

另外一个解决办法是修改 `addax.py` 中的 `CLASS_PATH` 变量，增加 `hbase-site.xml` 的路径
