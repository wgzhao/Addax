# 任务结果上报服务器功能 说明

## 快速介绍

主要用于将定时任务的结果上报给指定服务器

## 功能与限制

1. 支付http协议，JSON格式。
2. 接口地址配置在core.json文件下的core.dataXServer.address下。
3. 异步发送。
4. 需要引入httpclient-4.5.2.jar,httpcore-4.4.5.jar,httpcore-nio-4.4.5.jar,httpasyncclient-4.1.2.jar 相关jar包

## 功能说明

### 配置样例

```json
{
    "jobName": "test",
    "startTimeStamp": 1587971621,
    "endTimeStamp": 1587971621,
    "totalCosts": 10,
    "byteSpeedPerSecond": 33,
    "recordSpeedPerSecond": 1,
    "totalReadRecords": 6,
    "totalErrorRecords": 0
}
```

### 参数说明

| 参数          |  描述    | 必选   | 默认值  |
|---------------|----------|--------|-------|
| jobName        | 任务名 | 是 | job |
| startTimeStamp | 任务执行的开始时间 | 是  | 无 |
| endTimeStamp   | 任务执行的结束时间 | 是 | 无 |
| totalCosts   | 任务总计耗时 | 是 | 无 |
| byteSpeedPerSecond | 任务平均流量 | 是 | 无 |
| recordSpeedPerSecond | 记录写入速度 | 是 | 无 |
| totalReadRecords | 读出记录总数 | 是 | 0 |
| totalErrorRecords | 读写失败总数 | 是 | 0 |

**说明** 

`jobName` 的取值来自于配置文件的 `writer.parameters.path`值按 `/` 分割后取第2，3列用点(.)拼接而成，其含义是为库名及表名
