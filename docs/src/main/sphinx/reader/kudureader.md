# Kudu Reader

KuduReader 插件利用 Kudu 的java客户端KuduClient进行Kudu的读操作。


## 配置示例

我们通过 [Trino](https://trino.io)  的 `kudu connector` 连接 kudu 服务，然后进行表创建以及数据插入

### 建表语句以及数据插入语句

```sql
CREATE TABLE kudu.default.users (
  user_id int WITH (primary_key = true),
  user_name varchar,
  age int,
  salary double,
  longtitue decimal(18,6),
  latitude decimal(18,6),
  p decimal(21,20),
  mtime timestamp
) WITH (
  partition_by_hash_columns = ARRAY['user_id'],
  partition_by_hash_buckets = 2
);

insert into kudu.default.users 
values 
(1, cast('wgzhao' as varchar), 18, cast(18888.88 as double), 
 cast(123.282424 as decimal(18,6)), cast(23.123456 as decimal(18,6)),
 cast(1.12345678912345678912 as decimal(21,20)), 
 timestamp '2021-01-10 14:40:41'),
(2, cast('anglina' as varchar), 16, cast(23456.12 as double), 
 cast(33.192123 as decimal(18,6)), cast(56.654321 as decimal(18,6)), 
 cast(1.12345678912345678912 as decimal(21,20)), 
 timestamp '2021-01-10 03:40:41');
-- ONLY insert primary key value
 insert into kudu.default.users(user_id) values  (3);
```

### 配置

以下是读取kudu表并输出到终端的配置

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 3
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0.02
      }
    },
    "content": [
      {
        "reader": {
          "name": "kudureader",
          "parameter": {
            "masterAddress": "localhost:7051,localhost:7151,localhost:7251",
            "table": "users",
            "splitPk": "user_id",
            "lowerBound": 1,
            "upperBound": 100,
            "readTimeout": 5,
            "scanTimeout": 10
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

把上述配置文件保存为 `job/kudu2stream.json`

### 执行

执行下面的命令进行采集

```shell
bin/addax.sh job/kudu2stream.json
```

输出结果类似如下（删除了不必需要的内容)

```
2021-01-10 15:46:59.303 [main] INFO  VMInfo - VMInfo# operatingSystem class => sun.management.OperatingSystemImpl

2021-01-10 15:46:59.329 [main] INFO  Engine -
{
	"content":[
		{
			"reader":{
				"parameter":{
					"masterAddress":"localhost:7051,localhost:7151,localhost:7251",
					"upperBound":100,
					"readTimeout":5,
					"lowerBound":1,
					"splitPk":"user_id",
					"table":"users",
					"scanTimeout":10
				},
				"name":"kudureader"
			},
			"writer":{
				"parameter":{
					"print":true
				},
				"name":"streamwriter"
			}
		}
	],
	"setting":{
		"errorLimit":{
			"record":0,
			"percentage":0.02
		},
		"speed":{
			"channel":3
		}
	}
}

3	null	null	null	null	null	null	null
1	wgzhao	18	18888.88	123.282424	23.123456	1.12345678912345678912	2021-01-10 22:40:41
2	anglina	16	23456.12	33.192123	56.654321	1.12345678912345678912	2021-01-10 11:40:41

任务启动时刻                    : 2021-01-10 15:46:59
任务结束时刻                    : 2021-01-10 15:47:02
任务总计耗时                    :                  3s
任务平均流量                    :               52B/s
记录写入速度                    :              0rec/s
读出记录总数                    :                   2
读写失败总数                    :                   0
```

## 参数说明

| 配置项    | 是否必须 |  类型      |默认值 | 描述                                                                                                                                   |
| :-------- | :------: | ------ | -----|------------------------------------------------------------------------------------------------------------------------------|
| masterAddress | 必须 | string  |  无  | Kudu Master集群RPC地址,多个地址用逗号(,)分隔 |
| table | 必须  |  string | 无 | kudu 表名 |
| splitPk | 否 |  string | 无  | 并行读取数据分片字段 |
| lowerBound | 否 | string | 无 | 并行读取数据分片范围下界 |
| upperBound | 否 | string | 无 | 并行读取数据分片范围上界 |
| readTimeout | 否 | int  | 10 | 读取数据超时(秒) |
| scanTimeout | 否  | int | 20  | 数据扫描请求超时(秒) |

## 类型转换

| Addax 内部类型| Kudu 数据类型    |
| -------- | -----  |
| Long     | byte, short, int, long |
| Double   | float, double, decimal |
| String   | string |
| Date     | timestamp  |
| Boolean  | boolean |
| Bytes    | binary |
