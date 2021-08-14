# Postgresql Reader

PostgresqlReader 插件用于从 [PostgreSQL](https://postgresql.org) 读取数据

## 示例

假定建表语句以及输入插入语句如下：

```sql
create table if not exists addax_tbl
(
    c_bigint
    bigint,
    c_bit
    bit(3),
    c_bool boolean,
    c_byte bytea,
    c_char char(10),
    c_varchar varchar(20),
    c_date date,
    c_double float8,
    c_int integer,
    c_json json,
    c_number decimal(8, 3),
    c_real real,
    c_small smallint,
    c_text text,
    c_ts timestamp,
    c_uuid uuid,
    c_xml xml,
    c_money money,
    c_inet inet,
    c_cidr cidr,
    c_macaddr macaddr
    );

insert into addax_tbl
values (999988887777,
        B '101',
        TRUE,
        '\xDEADBEEF',
        'hello',
        'hello, world',
        '2021-01-04',
        999888.9972,
        9876542,
        '{"bar": "baz", "balance": 7.77, "active": false}'::json,
        12345.123,
        123.123,
        126,
        'this is a long text ',
        '2020-01-04 12:13:14',
        'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid,
        '<foo>bar</foo>'::xml,
        '52093.89'::money,
        '192.168.1.1'::inet,
        '192.168.1/24'::cidr,
        '08002b:010203'::macaddr);
```

配置一个从PostgreSQL数据库同步抽取数据到本地的作业:

```json
{
  "job": {
    "setting": {
      "speed": {
        "byte": -1,
        "channel": 1
      }
    },
    "content": [
      {
        "reader": {
          "name": "postgresqlreader",
          "parameter": {
            "username": "pgtest",
            "password": "pgtest",
            "column": [
              "*"
            ],
            "connection": [
              {
                "table": [
                  "addax_tbl"
                ],
                "jdbcUrl": [
                  "jdbc:postgresql://127.0.0.1:5432/pgtest"
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

将上述配置文件保存为   `job/postgres2stream.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/postgres2stream.json
```

其输出信息如下（删除了非关键信息)

```
2021-01-07 10:15:12.295 [main] INFO  Engine -
{
	"content":[
		{
			"reader":{
				"parameter":{
					"password":"*****",
					"column":[
						"*"
					],
					"connection":[
						{
							"jdbcUrl":[
								"jdbc:postgresql://localhost:5432/pgtest"
							],
							"table":[
								"addax_tbl"
							]
						}
					],
					"username":"pgtest"
				},
				"name":"postgresqlreader"
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
		"speed":{
			"byte":-1,
			"channel":1
		}
	}
}

999988887777	101	true   	ޭ��	hello     	hello, world	2021-01-04	999888.99719999998	9876542	{"bar": "baz", "balance": 7.77, "active": false}	12345.123	123.123	126	this is a long text 	2020-01-04 12:13:14	a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11	<foo>bar</foo>	52093.89	192.168.1.1	192.168.1.0/24	08:00:2b:01:02:03

任务启动时刻                    : 2021-01-07 10:15:12
任务结束时刻                    : 2021-01-07 10:15:15
任务总计耗时                    :                  3s
任务平均流量                    :               90B/s
记录写入速度                    :              0rec/s
读出记录总数                    :                   1
读写失败总数                    :                   0
```

## 参数说明

| 配置项    | 是否必须 | 默认值 | 描述                                                                                                                                   |
| :-------- | :------: | ------ | -----------------------------------------------------------------------------------------------------------------------------------|
| jdbcUrl   |    是    | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接 [附件控制信息](http://jdbc.postgresql.org/documentation/93/connect.html)  |
| username  |    是    | 无     | 数据源的用户名                                                                                                                                 |
| password  |    否    | 无     | 数据源指定用户名的密码                                                                                                                         |
| table     |    是    | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构                                                    |
| column    |    是    | 无     | 所配置的表中需要同步的列名集合，详细描述见 [rdbmsreader](rdbmsreader)                                                                        |
| splitPk   |    否    | 无     | 使用splitPk代表的字段进行数据分片，Addax因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能                                         |
| autoPk    |    否    | false | 是否自动猜测分片主键，`3.2.6` 版本引入 |
| where     |    否    | 无     | 针对表的筛选条件                                                                                                                               |
| querySql  |    否    | 无     | 使用自定义的SQL而不是指定表来获取数据，当配置了这一项之后，Addax系统就会忽略 `table`，`column`这些配置项                                       |
| fetchSize |    否    | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 Addax 出现OOM                                                                 |

## 类型转换


| Addax 内部类型 | PostgreSQL 数据类型                          |
| -------------- | -------------------------------------------- |
| Long           | bigint, bigserial, integer, smallint, serial |
| Double         | double precision, money, numeric, real       |
| String         | varchar, char, text, bit(>1), inet, cidr, macaddr, array,uuid,json,xml    |
| Date           | date, time, timestamp                        |
| Boolean        | bool,bit(1)                                   |
| Bytes          | bytea                                        |

## 已知限制

除上述罗列字段类型外，其他类型均不支持; 
