# Server Module

The Server module provides HTTP interfaces for submitting and managing data collection tasks. Users can submit JSON job configurations via POST, and the server executes tasks asynchronously, returning a unique task ID. Progress and results can be queried using this ID.

## Features
- RESTful API for task submission and status query
- Configurable maximum concurrent tasks (default: 30)
- Integrated with Addax core Engine for direct job execution
- Supports command-line and environment variable concurrency settings
- Startup/shutdown script with background mode support

## HTTP API

### 1. Submit Task
- URL: `/api/submit?k1=v1&k2=v2`
- Method: POST
- Request Example:
```shell
curl 'http://localhost:10601/api/submit?jobName=example-job' \
-H 'Content-Type: application/json' \
-d @job/job.json
```
- Response Example:
```json
{
    "taskId": "xxxx-xxxx-xxxx"
}
```
- If concurrency limit is reached:
```json
{
    "error": "ERROR: Maximum number of concurrent tasks reached."
}
```

### 2. Query Task Status
- URL: `/api/status?taskId={taskId}`
- Method: GET
- Response Example:
```json
{
    "taskId": "xxxx-xxxx-xxxx",
    "status": "SUCCESS",
    "result": "Job example-job executed.",
    "error": null
}
```

## Start and Stop

Use the script `core/src/main/bin/addax-server.sh` to start and stop the service.

### Start Service
```bash
./addax-server.sh start
```

### Set Max Concurrency (e.g., 50) and Run in Background
```bash
./addax-server.sh start -p 50 --daemon
```

### Stop Service
```bash
./addax-server.sh stop
```

## Concurrency Configuration
- Command-line `-p` or `--parallel` has the highest priority
- Environment variable `ADDAX_SERVER_PARALLEL` is next
- Default concurrency is 30

## Dependencies
- Minimal Spring Boot Web components
- Depends on core module Engine class

## Notes
- Ensure the job parameter is a valid Addax job JSON
- High concurrency may impact system performance

---
For more help, refer to other documentation or contact the project maintainers.

