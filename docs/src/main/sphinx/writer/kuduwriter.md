# KuduWriter 

KuduWriter 插件实现了将数据写入到 [kudu](https://kudu.apache.org) 的能力，当前是通过调用原生RPC接口来实现的。
后期希望通过 [impala](https://impala.apache.org) 接口实现，从而增加更多的功能。


## 示例

以下示例演示了如何从内存读取样例数据并写入到 kudu 表中的。

### 表结构

我们用 [trino](https://trino.io) 工具连接到 kudu 服务，然后通过下面的 SQL 语句创建表

```sql
CREATE TABLE kudu.default.users (
  user_id int WITH (primary_key = true),
  user_name varchar,
  salary double
) WITH (
  partition_by_hash_columns = ARRAY['user_id'],
  partition_by_hash_buckets = 2
);
```

### job 配置文件

创建 `job/stream2kudu.json` 文件，内容如下：

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1,
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
                "random": "1,1000",
                "type": "long"
              },
              {
                "random": "1,10",
                "type": "string"
              },
              {
                "random": "1000,50000",
                "type": "double"
              }
            ],
            "sliceRecordCount": 1000
          }
        },
        "writer": {
          "name": "kuduwriter",
          "parameter": {
            "masterAddress": "127.0.0.1:7051,127.0.0.1:7151,127.0.0.1:7251",
            "timeout": 60,
            "table": "users",
            "writeMode": "upsert",
            "column": [
              {"name":"user_id","type":"int8"},
              {"name":"user_name", "type":"string"},
              {"name":"salary", "type":"double"}
            ],
            "batchSize": 1024,
            "bufferSize": 2048,
            "skipFail": false,
            "encoding": "UTF-8"
          }
        }
      }
    ]
  }
}
```

### 运行

执行下下面的命令进行数据采集 

```bash
bin/datax.py job/stream2kudu.json
```

## 参数说明

| 配置项    | 是否必须 |  类型      |默认值 | 描述                                                                                                                                   |
| :-------- | :------: | ------ | -----|------------------------------------------------------------------------------------------------------------------------------|
| masterAddress | 必须 | string  |  无  | Kudu Master集群RPC地址,多个地址用逗号(,)分隔 |
| table | 必须  |  string | 无 | kudu 表名 |
| writeMode | 否 | string | upsert | 表数据写入模式，支持 upsert, insert 两者 |
| timeout | 否 | int  | 60 | 写入数据超时时间(秒) |
| column | 是  | list | 无  | 要写入的表字段及类型，如果配置为 `"*"` ，则会从目标表中读取所有字段|
| skipFail | 否 | boolean | false | 是否跳过插入失败的记录，如果设置为true，则插件不会把插入失败的当作异常 |

## 已知限制

1. 暂时不支持 `truncate table` 








