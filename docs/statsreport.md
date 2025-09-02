# 任务结果上报

## 快速介绍

主要用于将任务执行的结果上报给指定服务器，Addax 使用 HTTP 协议以 POST 方式把上报数据以 JSON 格式发送给指定的服务接口。

发送的数据类似如下：

```json
{
  "jobName": "test",
  "startTimeStamp": 1587971621,
  "endTimeStamp": 1587971621,
  "totalCosts": 10,
  "totalBytes": 330,
  "byteSpeedPerSecond": 33,
  "recordSpeedPerSecond": 1,
  "totalReadRecords": 6,
  "totalErrorRecords": 0,
  "jobContent": {
    "配置内容省略": "此处为实际任务配置"
  }
}
```

服务接口在 `$ADDAX/conf/core.json` 文件中的 `core.server.address` 中定义，比如：

```json
{
  "core": {
    "server": {
      "address": "http://localhost:9090/api/v1/addax/jobReport",
      "timeout": 5
    }
  }
}
```

这里的 <http://localhost:9090/api/v1/addax/jobReport> 接口服务需要自行开发，我们可以使用 Python 的 `flask` 快速开发这样的一个接口服务：

```python
#!/bin/env python3
# pip install flask
from flask import Flask, request, jsonify

app = Flask(__name__)

# 定义 POST 接口
@app.route('/api/v1/addax/jobReport', methods=['POST'])
def process_job():
    # 检查请求是否为 JSON 格式
    if not request.is_json:
        return jsonify({"error": "Invalid request. JSON data is expected."}), 400
    
    data = request.get_json()  # 获取 JSON 数据

    # 打印接收到的 JSON 数据
    print("Received JSON data:", data)

    # 可以在这里添加具体的数据处理逻辑
    # 比如保存到数据库表

    # 返回成功响应
    return jsonify({"message": "Job data received successfully.", "received_data": data}), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=9090, debug=True)
```

Java 代码示例可以参考 [AddaxReportController.java](https://github.com/wgzhao/addax-admin/blob/master/src/main/java/com/wgzhao/addax/admin/controller/AddaxReportController.java)

上述参数说明如下：

| 参数                    | 描述          | 必选 | 默认值      |
|-----------------------|-------------|----|----------|
| jobName               | 任务名         | 是  | jobName  |
| startTimeStamp        | 任务执行的开始时间   | 是  | 无        |
| endTimeStamp          | 任务执行的结束时间   | 是  | 无        |
| totalCosts            | 任务总计耗时(s)   | 是  | 无        |
| totalBytes            | 任务读写总字节数    | 是  | 无        |
| byteSpeedPerSecond    | 任务平均流量      | 是  | 无        |
| recordSpeedPerSecond  | 记录写入速度      | 是  | 无        |
| totalReadRecords      | 读出记录总数      | 是  | 0        |
| totalErrorRecords     | 读写失败总数      | 是  | 0        |
| jobContent            | 本次任务的json文件 | 是  | 无        |

上述参数只有 `jobName` 可以通过自行传递参数的时候设定，当你以

```shell
bin/addax.sh -p "-DjobName=test" job/job.json
```

执行采集任务时，POST 传递给接口的 `jobName` 就是上述指定的 `test` 值。

如果不指定，则 `Addax` 程序内部会生成 `jobName` 值，但生成的逻辑是假定你的采集任务是写数据到 Hadoop HDFS 文件系统上。
具体逻辑如下：

1. json 文件是否 `writer` 插件是否有 `writer.parameters.path` 值，如果没有，则设定值为 `jobName`，否则
2. 取 `writer.parameters.path` 值，按 `/` 分割后取第2，3列用点(.)拼接而成，其含义是为库名及表名

假定你的 json 任务文件配置如下：

```json
{
  "job": {
    "setting": {
      "speed": {
        "byte": -1,
        "channel": 1
      }
    },
    "content": {
      "reader": {
        "name": "mysqlreader",
        "parameter": {
          "username": "username",
          "password": "password",
          "column": [
            "*"
          ],
          "autoPk": "true",
          "connection": {
            "table": [
              "tbl"
            ],
            "jdbcUrl": "jdbc:mysql://example.com:3306/example_db"
          },
          "where": ""
        }
      },
      "writer": {
        "name": "hdfswriter",
        "parameter": {
          "defaultFS": "hdfs://yytz",
          "fileType": "orc",
          "path": "/ods/odstl/tbl/logdate=${logdate}",
          "fileName": "addax",
          "column": [
            "省略的字段配置"
          ],
          "writeMode": "overwrite",
          "fieldDelimiter": "\u0001",
          "compress": "lz4",
          "haveKerberos": "false"
        }
      }
    }
  }
}
```

那么先取出 `/ods/odstl/tbl/logdate=${logdate}`，然后按照 `/` 切分，获取第二项 `odstl`，第三项 `tbl`，然后拼接成 `odstl.tbl` 这个值就是 `jobName` 值
