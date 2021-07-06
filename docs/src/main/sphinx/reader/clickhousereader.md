# ClickHouse Reader

`ClickHouseReader` 插件支持从 [ClickHouse](https://clickhouse.tech)数据库读取数据。

## 示例

### 表结构及数据信息

假定需要的读取的表的结构以及数据如下：

```sql
CREATE TABLE ck_datax (
    c_int8 Int8,
    c_int16 Int16,
    c_int32 Int32,
    c_int64 Int64,
    c_uint8 UInt8,
    c_uint16 UInt16,
    c_uint32 UInt32,
    c_uint64 UInt64,
    c_float32 Float32,
    c_float64 Float64,
    c_decimal Decimal(38,10),
    c_string String,
    c_fixstr FixedString(36),
    c_uuid UUID,
    c_date Date,
    c_datetime DateTime('Asia/Chongqing'),
    c_datetime64 DateTime64(3, 'Asia/Chongqing'),
    c_enum Enum('hello' = 1, 'world'=2)
) ENGINE = MergeTree() ORDER BY (c_int8, c_int16) SETTINGS index_granularity = 8192;

insert into ck_datax values(
    127,
    -32768,
    2147483647,
    -9223372036854775808,
    255,
    65535,
    4294967295,
    18446744073709551615,
    0.999999999999,
    0.99999999999999999,
    1234567891234567891234567891.1234567891,
    'Hello String',
    '2c:16:db:a3:3a:4f',
    '5F042A36-5B0C-4F71-ADFD-4DF4FCA1B863',
    '2021-01-01',
    '2021-01-01 00:00:00',
    '2021-01-01 00:00:00',
    'hello'
);
```

## 配置 json 文件

下面的配置文件表示从 ClickHouse 数据库读取指定的表数据并打印到终端

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": -1
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0.02
      }
    },
    "content": [
      {
        "reader": {
          "name": "clickhousereader",
          "parameter": {
            "username": "root",
            "password": "root",
            "column": [
              "*"
            ],
            "connection": [
              {
                "table": [
                  "ck_datax"
                ],
                "jdbcUrl": [
                  "jdbc:clickhouse://127.0.0.1:8123/default"
                ]
              }
            ]
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

将上述配置文件保存为   `job/clickhouse2stream.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/datax.py job/clickhouse2stream.json
```

其输出信息如下（删除了非关键信息)

```
2021-01-06 14:39:35.742 [main] INFO  VMInfo - VMInfo# operatingSystem class => com.sun.management.internal.OperatingSystemImpl

2021-01-06 14:39:35.767 [main] INFO  Engine -
{
	"content":[
		{
			"reader":{
				"parameter":{
					"column":[
						"*"
					],
					"connection":[
						{
							"jdbcUrl":[
								"jdbc:clickhouse://127.0.0.1:8123/"
							],
							"table":[
								"ck_datax"
							]
						}
					],
					"username":"default"
				},
				"name":"clickhousereader"
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

127	-32768	2147483647	-9223372036854775808	255	65535	4294967295	18446744073709551615	1	1	1234567891234567891234567891.1234567891Hello String	2c:16:db:a3:3a:4f	
5f042a36-5b0c-4f71-adfd-4df4fca1b863	2021-01-01	2021-01-01 00:00:00	2021-01-01 00:00:00	hello

任务启动时刻                    : 2021-01-06 14:39:35
任务结束时刻                    : 2021-01-06 14:39:39
任务总计耗时                    :                  3s
任务平均流量                    :               77B/s
记录写入速度                    :              0rec/s
读出记录总数                    :                   1
读写失败总数                    :                   0
```

## 参数说明

`parameter` 配置项支持以下配置

| 配置项          | 是否必须   | 类型     | 默认值 |   描述          |
| :-------------- | :------: | ------ |--------|----------------|
| jdbcUrl         |    是    | array | 无     | ClickHouse JDBC 连接信息 ,可按照官方规范填写连接附件控制信息。具体请参看[ClickHouse官方文档](https://github.com/yandex/clickhouse-jdbc) |
| username        |    是    | string | 无     | 数据源的用户名 |
| password        |    否    | string | 无     | 数据源指定用户名的密码 |
| table           |    是    | array | 无     | 所选取的需要同步的表 ,当配置为多张表时，用户自己需保证多张表是同一schema结构|
| column          |    是    | array | 无     |所配置的表中需要同步的列名集合, 使用JSON的数组描述字段信息。用户使用 `*` 代表默认使用所有列配置，例如 `"['*']"` |
| splitPk         |    否    | string | 无     | 希望使用splitPk代表的字段进行数据分片,DataX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能 |
| autoPk          |    否    |  bool  | false | 是否自动猜测分片主键，`3.2.6` 版本引入 |
| where           |    否    | string | 无     | 筛选条件 |
| querySql        |    否    | array | 无     | 使用SQL查询而不是直接指定表的方式读取数据，当用户配置querySql时，ClickHouseReader直接忽略table、column、where条件的配置 |

## 支持的数据类型

目前ClickHouseReader支持大部分ClickHouse类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出ClickHouseReader针对ClickHouse类型转换列表:

| DataX 内部类型| ClickHouse 数据类型    |
| -------- | -----  |
| Long     |Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64, Enum8, Enum16|
| Double   |Float32, Float64, Decimal|
| String   |String, FixedString(N)|
| Date     |Date, DateTime, DateTime64 |
| Boolean  |UInt8 |
| Bytes    |String|

## 限制

除上述罗列字段类型外，其他类型均不支持，如Array、Nested等
