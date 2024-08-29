# 任务结果上报

## 快速介绍

主要用于将定时任务的结果上报给指定服务器，Addax 使用 HTTP 协议以 POST 方式把上报数据以 JSON 格式发送给指定的服务器。

发送的数据样例如下：

```json
{
  "jobName": "test",
  "startTimeStamp": 1587971621,
  "endTimeStamp": 1587971621,
  "totalCosts": 10,
  "byteSpeedPerSecond": 33,
  "recordSpeedPerSecond": 1,
  "totalReadRecords": 6,
  "totalErrorRecords": 0,
  "jobContent": {
    ...
  }
}
```
上述参数说明如下：

| 参数                 | 描述               | 必选 | 默认值 |
|----------------------|-------------------|----|------|
| jobName              | 任务名             | 是   | jobName    |
| startTimeStamp       | 任务执行的开始时间   | 是   | 无     |
| endTimeStamp         | 任务执行的结束时间   | 是   | 无     |
| totalCosts           | 任务总计耗时       | 是   | 无     |
| byteSpeedPerSecond   | 任务平均流量       | 是   | 无     |
| recordSpeedPerSecond | 记录写入速度       | 是   | 无     |
| totalReadRecords     | 读出记录总数       | 是   | 0      |
| totalErrorRecords    | 读写失败总数       | 是   | 0      |
| jobContent           | 本次任务的json文件 | 是   | 无     |

`jobName` 的设置规则如下:

1. 在命令行通过传递 `-P"-DjobName=xxxx"` 方式指定,否则
2. 配置文件的 `writer.parameters.path` 值按 `/` 分割后取第2，3列用点(.)拼接而成，其含义是为库名及表名,否则
3. 否则设置为 `jobName`
