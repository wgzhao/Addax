# AGENTS.md

## 概述

该项目是一个离线的 ETL 工具，通过插件化架构支持多种数据源和目标。核心组件包括 Engine、JobContainer，以及各种 Reader/Writer 插件。项目使用 Maven 管理构建和依赖。

## 通用偏好

- 用中文回复，代码注释用英文，注释写 why 不写 how
- 简洁直接，不要多余总结和解释
- 直接写代码，不需要每次确认后再生成

## 编译

### 整体编译

```bash
mvn clean package -T1C -DskipTests
```

### 打包发行版（产出可运行的目录树）

```bash
mvn package -Pdistribution -T1C -DskipTests
```

只有带 `-Pdistribution` 才会在 `target/addax-<version>/` 下组装出 `lib/` 和 `plugin/`，`bin/addax.sh` 依赖该目录结构，源码树里的脚本无法直接运行。

### 单模块编译（以 dorisreader 为例）

```bash
mvn clean package -pl :dorisreader -am
```

## 禁止
- `core` 为核心模块，包含 Engine 和 JobContainer 等核心类。修改此模块需谨慎，确保不破坏核心运行逻辑。
- `addax-rdbms` 包含多个 JDBC/SQL 相关的工具类
- `addax-lib` 包含一些通用工具类和依赖管理, 若无必要，避免修改核心工具类。
- 尽可能不要引入新的依赖库，尤其是核心模块和公共库。新增依赖可能引入版本冲突或增加维护负担。
- 考虑到兼容各类 RDBMS 的最低版本，因此各依赖库的版本请勿修改，除非确实需要修复安全漏洞或兼容性问题，并且在修改前先评估对现有插件的影响。
- 不需要写单元测试，所有的测试都会在目前特定构建的环境下手工执行

## 运行流程

1. 编辑一个 `json` 格式或者 `yaml` 格式的 Job 配置文件，指定 Reader、Writer 以及相关参数，可以参考 `core/src/main/job` 下的例子
2. 执行发行版目录里的 `bin/addax.sh` 脚本，job 文件是位置参数（`-j` 是 JVM 参数，勿写成 `-job`），例如：

```bash
sh target/addax-<version>/bin/addax.sh /path/to/job.json
```

程序运行的内部流程和新增插件的开发流程可以参考 [plugin development 文档](https://addax.wgzhao.com/plugin-development.html)

## 关键运行时约定

以下约定跨模块、不直观，改代码时最容易踩坑；细节以源码为准。

- **配置优先级**：Job 文件 > `conf/core.json` > 插件 `plugin.json`（后两者只补缺失键，不覆盖）。`${name}` 在解析前从 JVM system property 替换，未知占位符原样保留；`-p` 经 `sh -c` 展开且不加引号，值里不能有空格、`&`、`;`、`$` 等 shell 元字符。`curr_*`/`biz_*` 日期变量由 `addax.sh` 注入。
- **split 必须 1:1**：Reader `split(adviceNumber)` 的数量是建议值，Writer `split(mandatoryNumber)` 必须返回同样数量；`JobContainer` 对不一致直接报 `CONFIG_ERROR`，不补齐、不重采样。多表 RDBMS writer 要求表数 == 并发数。
- **并发推导**：配置了 `speed.byte`/`speed.record` 时按“总速率/单通道速率”算 channel 数，`speed.channel` 被忽略；否则用 `speed.channel`（默认 1）。channel 数最终会被 task 数截断，taskGroup 数 = `ceil(channel / core.container.taskGroup.channel)`（默认 5）。
- **一次 task = 一个 channel + 一对 reader/writer 线程**，writer 先启动。失败重试仅当 writer `supportFailOver()` 为 true 且未超过 `maxRetryTimes`（默认 1，即默认不重试）；重试间隔 10s、等待预算 60s，且必须等上一次线程真正退出后才允许重试，避免重复写入。
- **状态机**：task 的 FAILED 不可逆，首个失败原因保留；只有 reader/writer 两个 worker 都终止且 `Communication` 进入终态，task 才算结束。
- **errorLimit 陷阱**：`errorLimit.record` 优先于 `percentage`；两者都未配置时**不做任何错误检查**，即使所有行写入失败也可能退出 0。E2E 的 job 都显式设置 `"errorLimit": {"record": 0}`。
- **脏数据**：reader/writer 转换或写入失败通过 `TaskPluginCollector.collectDirtyRecord` 计入 readFailed/writeFailed，默认最多打印 128 条（负数=不限）。Transformer 返回 null 表示过滤（不计错误），抛异常才计脏数据。
- **RDBMS**：`splitPk`/`autoPk` 决定主键区间切分，单表默认切分数再 ×5；`querySql` 模式每个 SQL 一个 task。writeMode 支持 insert/replace/update(key)：MySQL `ON DUPLICATE KEY`、PostgreSQL/DuckDB `ON CONFLICT`、Oracle/SQL Server `MERGE`。批量失败会回滚并逐行重试，坏行记脏数据。连接池默认 maxTotal=32、borrow 等待 30s。
- **文件/HDFS**：writeMode 为 truncate/append/nonConflict/overwrite。hdfswriter 先写隐藏 `.<uuid>` staging 目录，post 时 rename 到目标且拒绝覆盖同名文件，destroy 时清理 staging；压缩文件不支持 append。
- **插件隔离**：每个插件由独立 `JarLoader` 加载 `plugin/<type>/<name>` 及其 `libs/`，Job/Task 调用期间切换线程 context classloader；入口类必须是 public、非抽象、静态嵌套的 `<class>$Job` / `<class>$Task`。
- **发行版构建**：必须先 `mvn clean package` 再 `mvn package -Pdistribution`；第二遍不能 clean，因为 root assembly 读取各模块 `target/` 下的产物。
- **验证方式**：无单元测试；改动的最终验证是 `e2e/` 的“打包发行版”端到端套件（真实 `bin/addax.sh` + `plugin/` + 容器数据库）。case 契约见 `e2e/README.md`；HDFS case 用 `file:///`，无需真实集群。

## 架构与设计宗旨

- 从第一性原理解构问题 一先明确什么是必须的，再决定怎么做
- 警惕 XY 问题-多角度审视方案，先确认真正要解决的是什么，主动提出替代方案
- 解決根本问题，不要 workaround -如果现有架构不支持，重构它
- 质疑不合理的需求和方向—发现问题立刻指出，不要等我问才说，不要奉承或无脑赞同
- 架构设计时参考 ddia-principles 和 software-design-philosophy 规则

## Git / PR 标准流程

用户明确要求"提交并创建 PR"时，按 `.claude/skills/git-pr-flow/SKILL.md` 执行（分支命名、commit 语言、`gh pr create`、PR 模板与 Ready for review 等要求都在其中）。

## Commit Message 规范

格式 `<type>(<scope>)!: <subject>` 由 `.github/scripts/lint-commit-msg.sh` 强制校验，本地已通过 `core.hooksPath=.githooks` 生效；校验失败时会打印全部机械规则，此处不再重复。以下是脚本查不出、需要自行判断的部分：

- 标题必须使用英文；`subject` 用祈使句现在时。
- 单个 commit 只做一件事，禁止混入无关改动。
- `feat`/`fix`/`refactor`/`perf` 必须写 `why:` / `what:` / `impact:` 三行，描述要紧跟在冒号后同一行内（脚本只检查行首前缀，换行不会报错）。

### Scope 约束

优先使用以下 scope：

- `core`、`server`、`docs`、`build`、`ci`、`deps`、`release`、`script`
- `lib-rdbms`、`lib-storage`
- `plugin-<name>`（例如：`plugin-hdfswriter`、`plugin-mongodbreader`）

### Footer 规则

- 关联 issue：`Refs: #123` 或 `Closes: #123`。
- 破坏性变更必须使用 `!` 或 `BREAKING CHANGE:`，并明确迁移方式。

### 历史风格映射（统一口径）

- `feature` -> `feat`
- `bugfix` -> `fix`
- `update` -> `chore`（仅版本/依赖更新）或 `fix`（修复问题）
- `improve` -> `refactor` / `perf` / `feat`（按语义选择）
- `[chore][3rd]` -> `chore(deps)`
- `[chore][action]` / `[chore][github][action]` -> `ci(github-actions)`

本地启用校验（仅需一次）：`git config core.hooksPath .githooks`；`chmod +x .githooks/commit-msg .github/scripts/lint-commit-msg.sh`
