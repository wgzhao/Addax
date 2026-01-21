# SKILL: Addax 项目知识

## 1. 项目整体认识

### 1.1 项目定位

- **名称**：Addax  
- **类型**：通用开源 ETL 工具（Extract–Transform–Load）  
- **起源**：基于阿里巴巴 DataX 的 fork 与演进  
- **目标**：在多种异构数据源之间，提供稳定、高效、可扩展的“离线数据同步”能力

### 1.2 核心价值

- 支持 **20+ SQL/NoSQL/文件/时序/大数据** 数据源
- 使用 **JSON 任务配置** 即可完成复杂同步，无需写代码
- 插件化架构，Reader / Writer / Transformer 解耦，可自由扩展
- 提供 **数据质量监控、速率控制、错误容忍、脏数据探测** 等生产级能力
- 既可命令行运行，也可通过 **Server 模块 HTTP 接口** 异步提交和管理任务
- 有配套的 **addax-admin / addax-ui** 项目做 Web 管控

---

## 2. 概念与架构模型

### 2.1 核心业务概念

在与用户讨论 / 理解需求时，应优先按以下抽象模型理解：

- **Job（作业）**
  - 一次完整的数据同步任务，从一个源到一个目标
  - 通过一个 JSON 文件描述：数据源 reader、目标端 writer、变换规则、速率控制、错误阈值等
  - Job 是业务上的最小单位，如 “从 MySQL 表 A 同步到 PostgreSQL 表 B”

- **Task（子任务）**
  - 为提升性能，将一个 Job 拆分为多个 Task 并发执行
  - 每个 Task 负责同步一部分数据（如若干分表、某一范围分片）

- **TaskGroup**
  - 一组 Task 的集合，由框架统一调度执行
  - 每个 TaskGroup 内有若干通道（channel），每个 channel 负责一条 `Reader → Channel → Writer` 流水线

- **Reader 插件**
  - 数据采集模块，负责从“源数据源”读取数据，发送给框架
  - 只关心“如何正确读”，不关注类型转换、指标统计等通用问题

- **Writer 插件**
  - 数据写入模块，负责从框架拿数据写入“目标端”
  - 只关心“如何正确写”，通用逻辑由框架处理

- **Transformer（数据转换）**
  - 可选模块，在 Reader 和 Writer 之间对数据进行转换
  - 支持内置 UDF：`dx_substr` / `dx_pad` / `dx_replace` / `dx_filter` / `dx_groovy`
  - 可以做脱敏、字段裁剪、补全、过滤、自定义 Groovy 脚本转换等

- **Channel（通道）**
  - Reader 到 Writer 之间的数据通路和缓冲队列
  - 决定并发度 & 流量控制（基于字节数、记录数、通道数）

### 2.2 架构概览

- **整体框架**：Framework + 插件（Reader / Writer / Transformer）
- 数据通路（简化）：

  - **源端 → Reader → Framework(Channel) → Writer → 目标端**

- 作业生命周期（JobContainer 内部）：
  1. `preHandler()` – 作业前置处理
  2. `init()` – 初始化 reader/writer 插件
  3. `prepare()` – 源端和目标端的准备工作
  4. `split()` – 按并发度拆分成多个 Task
  5. `schedule()` – 将 Task 组织为 TaskGroup，并发执行
  6. `post()` – 全局后置收尾（如 rename 影子表）
  7. `postHandler()` – 作业后置处理

- Task 执行：
  - 每个 Task 固定以 `Reader → Channel → Writer` 的线程模型执行
  - Channel 内以 Record/Column 为单位传输数据

---

## 3. SKILL：与用户交互时的“领域语言”

### 3.1 如何理解/解释一个 Job JSON

Job JSON 顶层结构：

```json
{
  "job": {
    "settings": {},
    "content": {
      "reader": {},
      "writer": {},
      "transformer": []
    }
  }
}
```

- `job.settings`
  - 控制本次任务的全局行为
  - 重点字段：
    - `speed.byte`：每秒允许的最大字节数（Bps），`-1` 表示不限制
    - `speed.record`：每秒允许的最大记录数
    - `speed.channel`：通道数（影响 Task 数量）
    - `errorLimit.record`：允许错误记录总数
    - `errorLimit.percentage`：允许错误记录占比

- `job.content.reader`
  - 必填，描述数据源及其读取方式
  - 核心字段（以关系型数据库为例）：
    ```json
    {
      "name": "mysqlreader",
      "parameter": {
        "username": "",
        "password": "",
        "column": [],
        "autoPk": false,
        "splitPk": "",
        "connection": [
          {
            "jdbcUrl": [],
            "table": []
          }
        ],
        "where": ""
      }
    }
    ```

- `job.content.writer`
  - 必填，描述落地目标及写入策略
  - 核心字段（以关系库为例）：
    ```json
    {
      "name": "mysqlwriter",
      "parameter": {
        "username": "",
        "password": "",
        "writeMode": "",
        "column": [],
        "session": [],
        "preSql": [],
        "postSql": [],
        "connection": [
          {
            "jdbcUrl": "",
            "table": []
          }
        ]
      }
    }
    ```

- `job.content.transformer`
  - 可选，列表形式，每个元素为一个转换规则
  - 典型片段（内置函数）：
    ```json
    {
      "transformer": [
        {
          "name": "dx_substr",
          "parameter": { "idx": 1, "pos": 0, "length": 3 }
        }
      ]
    }
    ```

AI 在阅读/生成 Job 时，应显式区分：
- 全局控制（settings） vs 数据流定义（reader/writer） vs 转换规则（transformer）

### 3.2 任务拆分与并发推导逻辑

- 用户设定并发度：`job.settings.speed.channel = N`
- 框架内部：
  1. Reader 的 `split()` 按源端特性拆成若干 Task（如按分表、分片、主键范围等）
  2. Writer 的 `split()` 需与 Reader 的 Task 数量 **1:1 对齐**
  3. Scheduler 根据 `taskGroup.channel`（`conf/core.json` 中配置）决定 TaskGroup 数量：
     - `taskGroupCount = speed.channel / taskGroup.channel`
- 与用户讨论“为什么任务这么慢/这么多连接”时，应从：
  - `speed.channel`
  - Reader 拆分策略（是否按 `splitPk` / 分区表）
  - 目标端写入瓶颈（Writer 能力、批量大小等）
  入手解释。

### 3.3 数据质量与错误处理

Addax 在数据质量方面的关键点：

- **类型不丢失/不失真**
  - 内部抽象了统一的 Column 类型：`Long / Double / String / Date / Timestamp / Bool / Bytes`
  - 每个插件有自己的类型转换策略，保证最小损失

- **错误控制**
  - 通过 `errorLimit.record` 和 `errorLimit.percentage` 控制“可容忍错误”
  - 超过阈值即认为任务失败

- **脏数据（Dirty Data）**
  - 概念：传输过程中因各种原因（例如类型不匹配）导致出错的记录
  - 能够过滤、识别、收集与展示脏数据，并统计数量和字节数
  - Transformer 层若抛出异常/返回 null，也会影响成功/失败/过滤计数

AI 在帮助用户排错时：
- 应主动询问/检查：错误是否集中在类型转换、特定列、特定插件
- 建议合理设置 `errorLimit`，在保障数据质量和任务稳定之间平衡

---

## 4. 使用方式与运行环境

### 4.1 安装与运行

- **运行时环境**
  - Java：JDK 17
  - Python 2.7+ / 3.7+（仅 Windows 使用本地脚本时需要）

- **三种典型使用方式**

1）Docker 运行示例：
```bash
docker pull quay.io/wgzhao/addax:latest
docker run -ti --rm --name addax \
  quay.io/wgzhao/addax:latest \
  /opt/addax/bin/addax.sh /opt/addax/job/job.json
```

2）一键安装脚本（Linux / macOS）：
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/wgzhao/Addax/master/install.sh)"
```
- 安装目录：
  - macOS：`/usr/local/addax`
  - Linux：`/opt/addax`

3）源码编译：
```bash
git clone https://github.com/wgzhao/addax.git
cd addax
mvn clean package
mvn package -Pdistribution        # 或 assembly:single
# 产出目录示例：target/addax-<version>
```

- **首次运行任务**
  ```bash
  bin/addax.sh job/job.json
  ```

### 4.2 命令行工具 addax.sh

基本用法：
```bash
bin/addax.sh <job_file> [options]
```

关键参数（AI 在给终端命令建议时要正确使用）：

- `-h, --help`：帮助
- `-v, --version`：版本
- `-l, --log`：指定日志文件路径
- `-d, --debug`：开启调试模式（IDEA 远程调试会用到）
- `-L, --log-level`：`DEBUG | INFO | WARN | ERROR`
- `-j, --jvm`：追加 JVM 参数
- `-p, --params`：向 Job 传入动态参数（`-Dkey=value` 形式）

示例（动态参数）：
```bash
bin/addax.sh job/test.json \
  -p "-Dusername=root -Dpassword=123456 -Dparam1=value1 -Dparam2=value2"
```

Job JSON 中可以通过 `${param}` 访问这些值，比如 `${username}`。

内置时间变量（示例时间 `2025-07-16 12:13:14`）：
- `${curr_date_short}` → `20250716`
- `${curr_date_dash}` → `2025-07-16`
- `${curr_datetime_short}` → `20250716121314`
- `${curr_datetime_dash}` → `2025-07-16 12:13:14`
- `${biz_date_short}` / `${biz_date_dash}` / `${biz_datetime_*}` 等

### 4.3 Server 模块（HTTP 提交任务）

- 用途：通过 HTTP 提交 Job JSON 并异步执行，可查询进度和结果
- 启动脚本：`core/src/main/bin/addax-server.sh`

启动示例：
```bash
./addax-server.sh start               # 默认并发上限 30
./addax-server.sh start -p 50 --daemon  # 最大并发 50，后台运行
./addax-server.sh stop
```

并发配置优先级：
1. 命令行 `-p` / `--parallel`
2. 环境变量 `ADDAX_SERVER_PARALLEL`
3. 默认 30

HTTP 接口：

1）提交任务
- URL: `/api/submit?k1=v1&k2=v2`
- Method: POST
- Body: Job JSON

示例：
```bash
curl 'http://localhost:10601/api/submit?jobName=example-job' \
  -H 'Content-Type: application/json' \
  -d @job/job.json
```

响应：
```json
{ "taskId": "xxxx-xxxx-xxxx" }
```
或并发达到上限时：
```json
{ "error": "ERROR: Maximum number of concurrent tasks reached." }
```

2）查询任务状态
- URL: `/api/status?taskId={taskId}`
- Method: GET
- 响应：
```json
{
  "taskId": "xxxx-xxxx-xxxx",
  "status": "SUCCESS",
  "result": "Job example-job executed.",
  "error": null
}
```

AI 在设计自动化系统（如调度、工作流）时，可建议用户使用 Server 模块通过 HTTP 集成。

---

## 5. 插件系统与二次开发

### 5.1 支持的数据源（典型）

Reader / Writer 插件覆盖的常见系统包括但不限于：

- 关系库：MySQL, PostgreSQL, Oracle, SQLServer, SQLite, Greenplum, DB2, Sybase, Doris, StarRocks 等
- NoSQL / KV：Cassandra, Redis, MongoDB, HBase（1.x, 2.x，多种模式）
- 大数据 / 文件：HDFS, Hive, Kudu, Iceberg, Paimon, S3/MinIO, FTP, 本地文件（txt/dbf/excel/json）
- 时序 / 流：InfluxDB/InfluxDB2, TDengine, Kafka, streamreader/streamwriter
- 其它：Access, SAP HANA, ClickHouse, Databend 等

当用户问“是否支持 XXX 数据源”时，应：
- 优先在 docs/reader 与 docs/writer 列表中查找对应 `xxxreader`/`xxxwriter`
- 若暂不支持，可建议走 **插件开发** 路线，说明难度和接口模型

### 5.2 插件开发模型

- 插件 = 一个 Java 模块 + `plugin.json` 描述 + 若干 jar 依赖
- 入口类需继承：
  - `Reader` 或 `Writer` 抽象类
  - 内部包含两个静态内部类：`Job` 和 `Task`
- 插件目录结构（约定）：
  ```text
  ${ADDAX_HOME}/plugin
    ├── reader
    │   └── <plugin_name>
    │       ├── <plugin_name>-<version>.jar
    │       ├── libs/ -> 指向 shared 依赖目录的符号链接
    │       ├── plugin.json
    │       └── plugin_job_template.json
    └── writer
        └── ...
  ```

`plugin.json` 示例：
```json
{
  "name": "mysqlwriter",
  "class": "com.wgzhao.addax.plugin.writer.mysqlwriter.MysqlWriter",
  "description": "Use Jdbc connect to database, execute insert sql.",
  "developer": "wgzhao"
}
```
注意：
- 目录名必须与 `plugin.json` 中的 `name` 一致
- 框架通过 `name` 找插件，通过 `class`（完全限定名）反射加载

### 5.3 Job / Task 接口职责（用于代码分析/生成）

以 Reader 为例：

```java
public class SomeReader extends Reader {
    public static class Job extends Reader.Job {
        public void init() { }
        public void prepare() { }
        public List<Configuration> split(int adviceNumber) { return null; }
        public void post() { }
        public void destroy() { }
    }

    public static class Task extends Reader.Task {
        public void init() { }
        public void prepare() { }
        public void startRead(RecordSender recordSender) { }
        public void post() { }
        public void destroy() { }
    }
}
```

- `Job` 级别：
  - 负责读取插件配置：`super.getPluginJobConf()`
  - 全局准备/收尾：如建表/清表、校验权限等
  - `split(adviceNumber)`：按并发建议数拆分 Configuration 列表（每个对应一个 Task）

- `Task` 级别：
  - 用 `super.getPluginJobConf()` 获取本 Task 的配置
  - 在 `startRead()` / `startWrite()` 中进行实际 I/O
  - 使用 `RecordSender` / `RecordReceiver` 和 `Record`/`Column` 抽象进行传输

重要约束：
- Job 和 Task 之间禁止使用共享变量，只能通过配置（Configuration）传递信息
- `prepare` / `post` 在 Job 和 Task 层都有，需根据场景选择合适层级实现

### 5.4 Configuration DSL

Addax 提供 `Configuration` 类和路径 DSL 来读取 JSON 配置：

- 路径规则：
  - 子对象：`a.b.c`
  - 数组元素：`a.f[2].g`
- 示例 JSON：
  ```json
  {
    "a": {
      "b": { "c": 2 },
      "f": [1, 2, { "g": true }]
    },
    "x": 4
  }
  ```
- 示例路径与结果：
  - `x` → `4`
  - `a.b.c` → `2`
  - `a.b.f[2].g` → `true`

AI 在帮用户写插件代码时，可直接给出 `Configuration.get("a.b.c")` 等示例。

---

## 6. 调试、监控与结果上报

### 6.1 运行日志与调试

- 本地/远程调试模式：`bin/addax.sh -d job/job.json`
  - 程序会以 `Listening for transport dt_socket at address: 9999` 形式暴露 JVM 调试端口
  - 可用 IntelliJ IDEA 的 Remote JVM Debug 挂载到指定 host:port

本地调试典型配置：
- Main class：`com.wgzhao.addax.core.Engine`
- VM Options：
  - `-Daddax.home=/opt/app/addax/4.0.3`
  - 如需加载本地 lib 依赖：`-classpath .:/opt/app/addax/4.0.3/lib/*`
- Program arguments: `-job job/job.json`
- Working directory: `/opt/app/addax/4.0.3`

### 6.2 任务运行统计和 Transformer 计量

Addax 会在日志中输出类似：

```text
Total 1000000 records, 22000000 bytes | 
Transform 100000 records(in), 10000 records(out) | 
Speed 2.10MB/s, 100000 records/s | 
Error 0 records, 0 bytes | Percentage 100.00%
```

以及最终汇总：

```text
任务启动时刻                    : 2015-03-10 17:34:21
任务结束时刻                    : 2015-03-10 17:34:31
任务总计耗时                    :                 10s
任务平均流量                    :            2.10MB/s
记录写入速度                    :         100000rec/s
转换输入总数                    :             1000000
转换输出总数                    :             1000000
读取出记录总数                  :             1000000
同步失败总数                    :                   0
```

Transformer 维度统计：
- 输入记录数/字节数
- 输出记录数/字节数
- 脏数据记录数/字节数
- 总耗时

AI 在分析性能问题 / 瓶颈时，应从：
- 读/写 QPS
- Transform 前后记录量变化
- 错误/脏数据数量
- 任务耗时与并发度
这些维度提示用户。

### 6.3 任务结果上报（Stats Report）

- 用途：将 Job 执行的统计结果通过 HTTP POST 报告到外部服务（如监控平台 / 管理端）
- 配置位置：`$ADDAX_HOME/conf/core.json` → `core.server.address`

示例：
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

上报数据结构（JSON）：
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
  "jobContent": { "配置内容省略": "此处为实际任务配置" }
}
```

AI 可建议用户：
- 使用简单 Flask/Node/Java 服务接收并入库统计数据
- 或与现有监控系统集成

---

## 7. 与 Addax 相关的生态项目

- **addax-admin**（后端）
  - 仓库：`https://github.com/wgzhao/addax-admin`
  - 提供 Web 管理界面和 API，用于管理 Addax 任务

- **addax-ui**（前端）
  - 仓库：`https://github.com/wgzhao/addax-ui`
  - 为 addax-admin 提供前端展示

AI 在回答“有没有 Web 管理界面 / 调度平台”时，可推荐这两个项目。

---

## 8. 版本与贡献

- **版本规范**：遵循 SemVer (`x.y.z`)
  - `z` Patch：兼容修复、性能优化
  - `y` Minor：新增特性或兼容性风险较小的修改
  - `x` Major：重大变更，通常不向后兼容

- **开发规范（概略）**
  - 使用 IntelliJ + Airlift 代码风格
  - 异常使用 `AddaxException`，并区分错误类型
  - 谨慎使用 Stream API，避免在性能敏感路径中滥用
  - 避免复杂三元表达式
  - 所有文件需包含 Apache 2.0 许可证头
  - Commit message 参考 <https://cbea.ms/git-commit/>

AI 在生成 PR/代码建议时，应尽量贴合以上风格。
