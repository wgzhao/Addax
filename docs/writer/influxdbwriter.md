# InfluxDB Writer

InfluxDBWriter 插件实现了将数据写入 [InfluxDB][1] 读取数据的功能。
底层实现上，是通过调用 InfluQL 语言接口，构建插入语句，然后进行数据插入。

## 示例

以下示例用来演示该插件从内存读取数据并写入到指定表

### 创建需要的库

通过以下命令来创建需要写入的库

```bash
# create database
influx --execute "CREATE DATABASE addax"
```

### 创建 job 文件

创建 `job/stream2influxdb.json` 文件，内容如下：

=== "job/stream2influxdb.json"

  ```json
  --8<-- "jobs/influxdbwriter.json"
  ```

### 运行

执行下面的命令进行数据采集

```bash
bin/addax.sh job/stream2influxdb.json
```

## 参数说明

| 配置项          | 是否必须 | 数据类型 | 默认值 | 描述                               |
| :-------------- | :------: | -------- | ------ | ---------------------------------- |
| endpoint        |    是    | string   | 无     | InfluxDB 连接串                    |
| username        |    是    | string   | 无     | 数据源的用户名                     |
| password        |    否    | string   | 无     | 数据源指定用户名的密码             |
| database        |    是    | string   | 无     | 数据源指定的数据库                 |
| table           |    是    | string   | 无     | 要写入的表（指标）                 |
| column          |    是    | list     | 无     | 所配置的表中需要同步的列名集合     |
| connTimeout     |    否    | int      | 15     | 设置连接超时值，单位为秒           |
| readTimeout     |    否    | int      | 20     | 设置读取超时值，单位为秒           |
| writeTimeout    |    否    | int      | 20     | 设置写入超时值，单位为秒           |
| preSql          |    否    | list     | 无     | 插入数据前执行的SQL语句            |
| postSql         |    否    | list     | 无     | 数据插入完毕后需要执行的语句       |
| retentionPolicy |    否    | dict     | 无     | 设置数据库的 Retention Policy 策略 |

### column

InfluxDB 作为时序数据库，需要每条记录都有时间戳字段，因此这里会把 `column` 配置的第一个字段默认当作时间戳

### retentionPolicy

设定数据库的 `Retention Policy` 策略，依据给定的配置，在指定数据库上创建一条 `Retention Policy` 信息。
有关 `Retention Policy` 更详细的信息，可以参考 [官方文档][2]

## 类型转换

当前支持 InfluxDB 的基本类型

## 限制

1. 当前插件仅支持 1.x 版本，2.0 及以上并不支持

[1]: https://www.influxdata.com
[1]: https://docs.influxdata.com/influxdb/v1.8/query_language/manage-database/#create-retention-policies-with-create-retention-policy