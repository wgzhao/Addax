---
name: git-pr-flow
description: Standard branch, commit, and pull-request workflow for this repo. Use when the user says "提交并创建 PR" or otherwise asks to commit changes and open a PR, instead of only pushing a branch.
---

# Git / PR 标准流程

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

commit message 的格式与 scope 约定见 `AGENTS.md` 的「Commit Message 规范」一节。
