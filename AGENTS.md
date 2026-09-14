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
2. 执行 `addax.sh` 脚本，传入 Job 配置文件路径，例如：

```bash
sh addax.sh -job /path/to/job.json
```

程序运行的内部流程可以参考[这个文档](https://github.com/wgzhao/addax-docs/raw/refs/heads/master/docs/plugin-development.md)

## 插件开发

新增插件的开发流程可以参考[plugin development 文档](https://github.com/wgzhao/addax-docs/raw/refs/heads/master/docs/plugin-development.md)


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
