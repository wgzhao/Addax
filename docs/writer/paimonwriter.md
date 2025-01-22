# Paimon Writer

Paimon Writer 提供向 已有的paimon表写入数据的能力。

## 配置样例

```json
--8<-- "jobs/paimonwriter.json"
```

## 参数说明

| 配置项          | 是否必须 | 数据类型   | 默认值 | 说明                                             |
|:-------------|:----:|--------|----|------------------------------------------------|
| dbName       |  是   | string | 无  | 要写入的paimon数据库名                                 |
| tableName    |  是   | string | 无  | 要写入的paimon表名                                   |
| writeMode    |  是   | string | 无  | 写入模式，详述见下                                      |
| paimonConfig |  是   | json   | {} | 里可以配置与 Paimon catalog和Hadoop 相关的一些高级参数，比如HA的配置 |



### writeMode

写入前数据清理处理模式：

- append，写入前不做任何处理，直接写入，不清除原来的数据。
- truncate 写入前先清空表，再写入。

### paimonConfig

`paimonConfig` 里可以配置与 Paimon catalog和Hadoop 相关的一些高级参数，比如HA的配置
```json
{
					"name": "paimonwriter",
					"parameter": {
						"dbName": "test",
                        "tableName": "test2",
                        "writeMode": "truncate",
                        "paimonConfig": {
                           "warehouse": "file:///g:/paimon",
                           "metastore": "filesystem"
                         }
					}
}
```
```json
{
  "paimonConfig": {
    "warehouse": "hdfs://nameservice1/user/hive/paimon",
    "metastore": "filesystem",
    "fs.defaultFS":"hdfs://nameservice1",
    "hadoop.security.authentication" : "kerberos",
    "hadoop.kerberos.principal" : "hive/_HOST@XXXX.COM",
    "hadoop.kerberos.keytab" : "/tmp/hive@XXXX.COM.keytab",
    "ha.zookeeper.quorum" : "test-pr-nn1:2181,test-pr-nn2:2181,test-pr-nn3:2181",
    "dfs.nameservices" : "nameservice1",
    "dfs.namenode.rpc-address.nameservice1.namenode371" : "test-pr-nn2:8020",
    "dfs.namenode.rpc-address.nameservice1.namenode265": "test-pr-nn1:8020",
    "dfs.namenode.keytab.file" : "/tmp/hdfs@XXXX.COM.keytab",
    "dfs.namenode.keytab.enabled" : "true",
    "dfs.namenode.kerberos.principal" : "hdfs/_HOST@XXXX.COM",
    "dfs.namenode.kerberos.internal.spnego.principal" : "HTTP/_HOST@XXXX.COM",
    "dfs.ha.namenodes.nameservice1" : "namenode265,namenode371",
    "dfs.datanode.keytab.file" : "/tmp/hdfs@XXXX.COM.keytab",
    "dfs.datanode.keytab.enabled" : "true",
    "dfs.datanode.kerberos.principal" : "hdfs/_HOST@XXXX.COM",
    "dfs.client.use.datanode.hostname" : "false",
    "dfs.client.failover.proxy.provider.nameservice1" : "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
    "dfs.balancer.keytab.file" : "/tmp/hdfs@XXXX.COM.keytab",
    "dfs.balancer.keytab.enabled" : "true",
    "dfs.balancer.kerberos.principal" : "hdfs/_HOST@XXXX.COM"
  }
}
```


## 类型转换

| Addax 内部类型 | Paimon 数据类型                  |
|------------|------------------------------|
| Integer    | TINYINT,SMALLINT,INT,INTEGER |
| Long       | BIGINT                       |
| Double     | FLOAT,DOUBLE,DECIMAL         |
| String     | STRING,VARCHAR,CHAR          |
| Boolean    | BOOLEAN                      |
| Date       | DATE,TIMESTAMP               |
| Bytes      | BINARY                       |

