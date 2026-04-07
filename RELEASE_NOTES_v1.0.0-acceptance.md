# Release Notes - v1.0.0-acceptance

发布日期：2026-04-07  
基线标签：v1.0.0-acceptance

## 版本结论

本版本为 AI 模块 V1 的验收基线版本，已完成导出、上传转换、打标、可观测闭环与文档固化，满足当前可演示与可回归要求。

## 验收范围

- 导出链路：MySQL -> CSV / JSON / TSV
- 上传转换链路：
  - CSV -> JSON
  - CSV -> TSV
  - JSON -> CSV
  - JSON -> TSV
  - TSV -> CSV
  - TSV -> JSON
- 打标链路：manual-table 编辑保存 -> tagged_output 产物
- 可观测：traceId + STARTED/SUCCEEDED/FAILED + durationMs
- 前端展示：上传转换结果支持三行清晰展示
  - 成功信息
  - 上传文件路径
  - 转换文件路径

## 本版本关键改动

- 新增并稳定上传转换 6 条独立路径
- 导出支持 where 条件透传并完成分流验证
- 完成上传无表头场景兜底（含 columns 参数）
- 统一响应观测字段并通过中间件输出结构化日志
- 需求与验收文档重构为执行版并补齐回填记录
- 根目录补充 README 与仓库纳管（.gitignore）

## 关联提交（按时间近到远）

- 460b3dd chore(git): ignore local scratch file ai.txt
- 97312cc docs(repo): track execution checklist and daily progress docs
- bd41c5a chore(repo): track core backend/frontend source and configs
- 2254204 chore(git): add .gitignore to reduce noisy pending changes
- a9577cf docs: add root README with quick start and acceptance status
- 01e7af5 feat(v1): close acceptance gate and improve upload path visibility

## 已知边界

- 当前基线以本地运行和既有目录约定为主，后续 NiFi 迁移为增量改造。
- 若进入生产部署，需要补齐环境隔离、权限与自动化回归策略。

## 回滚指引

若后续版本出现回归，可回退到本验收基线：

```bash
git checkout v1.0.0-acceptance
```

或基于该标签创建修复分支：

```bash
git checkout -b hotfix/from-v1.0.0-acceptance v1.0.0-acceptance
```
