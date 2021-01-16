# InfluxDBReader

InfluxDBReader 插件实现了从 [InfluxDB](https://www.influxdata.com) 读取数据。底层实现上，是通过调用 InfluQL 语言查询表，然后获得返回数据。

## 示例

以下示例用来演示该插件如何从指定表(即指标)上读取数据并输出到终端

### 创建需要的库表和数据

通过以下命令来创建需要读取的表以及数据

```bash
# create database
influx --execute "CREATE DATABASE NOAA_water_database"
# download sample data
curl https://s3.amazonaws.com/noaa.water-database/NOAA_data.txt -o NOAA_data.txt
# import data via influx-cli
influx -import -path=NOAA_data.txt -precision=s -database=NOAA_water_database
```

### 创建 job 文件

创建 `job/influxdb2stream.json` 文件，内容如下：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "influxdbreader",
          "parameter": {
            "column": [
              "*"
            ],
            "connection": [
              {
                "endpoint": "http://localhost:8086",
                "database": "NOAA_water_database",
                "table": "h2o_feet",
                "where": "1=1"
              }
            ],
            "username": "influx",
            "password": "influx123"
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": "true"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "bytes": -1,
        "channel": 1
      }
    }
  }
}
```

### 运行

执行下面的命令进行数据采集

```bash
bin/datax.py job/influxdb2stream.json
```

##  参数说明

| 配置项          | 是否必须 |  数据类型   |默认值 |         描述   |
| :-------------- | :------: | ------ |-------|-------------- |
| endpoint         |    是   | string | | 无     | InfluxDB 连接串 ｜
| username        |    是    | string | 无     | 数据源的用户名 |
| password        |    否    | string | 无     | 数据源指定用户名的密码 |
| database        |  是      | string |  无      | 数据源指定的数据库  |
| table           |    是    | string |无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column          |    是    | list  | 无     |  所配置的表中需要同步的列名集合，详细描述[rdbmreader](rdbmsreader.md) ｜
| where           |    否    | 无     | 针对表的筛选条件 |
| querySql        |    否    | 无     | 使用自定义的SQL而不是指定表来获取数据，当配置了这一项之后，DataX系统就会忽略 `table`，`column`这些配置项 |

##  类型转换

当前实现是将所有字段当作字符串处理


## 限制

1. 当前插件仅支持 1.x 版本，2.0 及以上并不支持