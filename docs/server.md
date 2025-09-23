# Server模块

Server模块用于通过HTTP接口提交和管理数据采集任务。用户可通过POST方式提交JSON任务配置，服务端异步执行采集任务并返回唯一任务ID，随后可通过任务ID查询任务进度和结果。

## 功能简介

- 提供RESTful接口，支持任务提交与状态查询
- 支持最大并发任务数限制（默认30，可配置）
- 集成Addax核心Engine，直接执行采集任务
- 支持命令行和环境变量设置并发数
- 提供启动/停止脚本，支持后台运行

## HTTP接口说明

### 1. 提交任务
- URL: `/api/submit?k1=v1&k2=v2`
- 方法: POST
- 请求体示例：
```shell
curl 'http://localhost:10601/api/submit?jobName=example-job' \
-H 'Content-Type: application/json' \
-d @job/job.json
```
- 返回示例：
```json
{
    "taskId": "xxxx-xxxx-xxxx"
}
```
- 当并发数达到上限时：
```json
{
    "error": "ERROR: Maximum number of concurrent tasks reached."
}
```

### 2. 查询任务状态
- URL: `/api/status?taskId={taskId}`
- 方法: GET
- 返回示例：
```json
{
    "taskId": "xxxx-xxxx-xxxx",
    "status": "SUCCESS",
    "result": "Job example-job executed.",
    "error": null
}
```

## 启动与停止

推荐使用脚本 `core/src/main/bin/addax-server.sh` 启动和停止服务。

### 启动服务
```bash
./addax-server.sh start
```

### 设置最大并发数（如50）并后台运行
```bash
./addax-server.sh start -p 50 --daemon
```

### 停止服务
```bash
./addax-server.sh stop
```

## 并发数配置
- 命令行参数 `-p` 或 `--parallel` 优先
- 环境变量 `ADDAX_SERVER_PARALLEL` 其次
- 默认并发数为30

## 依赖说明
- 仅依赖Spring Boot最小化Web组件
- 依赖core模块的Engine类

## 注意事项
- 任务提交时请确保job参数为合法的Addax任务JSON
- 并发数过高可能影响系统性能

---
如需更多帮助，请参考其他文档或联系项目维护者。

