# AGENTS.md

## 概述

该项目是一个离线的 ETL 工具，通过插件化架构支持多种数据源和目标。核心组件包括 Engine、JobContainer，以及各种 Reader/Writer 插件。项目使用 Maven 管理构建和依赖。

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


## Git / PR 标准流程

当用户明确提出“提交并创建 PR”时，默认按以下流程执行（除非用户另有说明）：

1. 创建新分支后再提交，分支名建议使用 `feat/<topic>` 或 `fix/<topic>` 格式，根据本次修改的性质选择 `feat`（新功能）或 `fix`（修复）。例如：`feat/add-protobuf-dependency`。
2. 使用英文编写 commit message：
   - `title` 简洁明确（建议 Conventional Commits 风格）。
   - `description/body` 说明动机、核心改动、验证情况。
3. 使用 `gh` 命令创建 PR，不只推送分支：
   - 示例：`gh pr create --base master --head <branch> --title "<english title>" --body-file <file>`
4. PR 内容必须使用英文，遵循 [PR 模板](.github/pull_request_template.md) 进行填写
5. 若无特别要求，PR 设为 Ready for review（非 Draft）。

以上流程可由一句 "提交并创建 PR" 触发，不需要用户重复描述细节格式要求。
