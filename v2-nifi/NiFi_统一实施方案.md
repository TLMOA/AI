# NiFi 统一实施方案（Manual / Semi-auto / Full-auto）

版本：1.0
作者：xy（由 Copilot 协助生成）
日期：2026-05-08

说明：本文件为合并版，替代 `V1-NiFi统一实施可执行方案.md` 与 `NiFi后端生产化实施方案.md`（这两份文件已标记为弃用）。文档明确三条部署路径：Manual（人工）、Semi-auto（半自动，推荐短期生产过渡）、Full-auto（全自动，长期目标）。

## 1 目标与范围
- 目标：把当前 `v1-backend` 的 NiFi 整合能力，升级为可生产、可观测、可运维、可回滚的 NiFi 后端子系统；并提供清晰的迁移路径从 Manual → Semi-auto → Full-auto。
- 范围：受控 NiFi 镜像、NiFi Registry、后端 `nifi_orchestrator` 的 K8s 与 Docker 适配、CI/CD（Flow + 镜像 + manifest）、监控/告警、备份/恢复、Runbooks 与演练。

## 2 三条部署路径（定义与推荐）
- Manual：运维手工管理 NiFi/Registry，后端只负责提交任务到既有目录（`/home/yhz/iot/real_nifi_data`）。适用于快速实验与应急回退，不建议长期生产化。
- Semi-auto（推荐短期转生产）：自动化 Flow 打包、导入 Registry 与 contract-tests；容器/manifest 的启动/升级由运维或 CI/CD 触发（非后端自动启动）。优点：降低风险、易回滚、可以较快落地生产验证。
- Full-auto（长期目标）：`nifi_orchestrator` 完整控制 NiFi 容器生命周期、自动部署/启动 Flow、分布式锁、审计与自动 promote/回滚。优点：一键部署与更高可操作性；缺点：工程量大、需要更严格的演练与监控。

建议：以 `Semi-auto` 为默认生产过渡路线，逐步在后续迭代中补齐 Full-auto 能力。

## 3 核心约定（跨路径一致）
- 接口契约：统一响应壳 `{ code, message, data, traceId }`，文件元数据字段 `fileId,fileName,fileFormat,fileSize,storageType,storagePath,createdAt,jobId`。
- 目录约定：NiFi 后端统一使用 `/home/yhz/iot/real_nifi_data`；逻辑目录集合：`output_csv|output_json|output_tsv|inbox_csv|inbox_json|inbox_tsv|tagged_output|export_jobs`。
- 环境变量：`NIFI_API_BASE`、`NIFI_REGISTRY_URL`、`NIFI_MODE`（`manual|semi|full`）等由部署/CI 提供。
- 回滚与备份：Flow 版本化优先使用 NiFi Registry；per-resource last-applied 保存为 ConfigMap（K8s）或对象存储快照，回滚流程须在 Runbook 中明确。

## 4 Orchestrator 改造要点（优先级）
1. 支持 `NIFI_MODE`：`manual|semi|full`（接口不变，对前端透明）。
2. 幂等与锁：短期使用 DB 锁（而非文件锁）；Full-auto 阶段强化分布式锁与事务保证。
3. 部署方式：Semi-auto 下仅提供 Flow 导入/验证 API；Full-auto 下实现 `ensure_nifi_container()`、`wait_nifi_ready()`、`deploy_export_flow()`、`start_export_flow()`、`submit_export_job()`、`sync_export_result()` 等。
4. 审计与可观测：关键操作生成 traceId，输出到审计日志并上报指标（`nifi.flow.execution.count`、`nifi.flow.failure.count` 等）。

## 5 CI/CD 与 contract-tests
- Semi-auto 流程（必须实现并验证）：
  1) CI 生成 Flow bundle (`artifacts/flow-bundle.zip`)；
  2) 在 staging 导入 NiFi Registry（使用 `v2-nifi/scripts/import_flow_to_registry.sh`）；
  3) 运行 contract-tests（`newman`）；
  4) 人工/受控自动 promote 到 Production Registry。 
- Full-auto 目标：在合格后自动 promote 并触发 orchestrator 对 Production 环境的部署/更新（需审计与 review gate）。

## 6 迁移与实施路线（迭代）
- 阶段 0（准备）：准备私有 Registry、对象存储、测试命名空间、Secrets（`REGISTRY_URL` 等）。
- 阶段 1（Semi-auto 上线，1–2 周）：实现 CI 的 Flow 打包→导入→contract-tests，验证 staging，通过后在小范围工厂开启 `nifi` 模式。
- 阶段 2（强化，2–4 周）：实现分布式锁、规范化 manifests、完善 Runbooks、监控与告警、回滚演练。
- 阶段 3（Full-auto，2–4 周）：实现 orchestrator 完整自动化（容器生命周期控制、自动回滚、演练与 SLA 承诺）。

## 7 运行手册要点（Runbooks 精简）
- 启动 NiFi（K8s）：`kubectl apply -f v2-nifi/k8s/`。
- 检查 NiFi 健康：`curl ${NIFI_API_BASE%/}/flow/about` 或 `kubectl -n <ns> get pods -l app=nifi`。
- 回滚 Flow：从 NiFi Registry 回退到上一版本并通过 orchestrator/手工验证。
- 备份恢复：见 `v2-nifi/backup_restore.md`。

## 8 验收标准（合并版精简）
- CI contract-tests 在 staging 通过。
- 在 sample 工厂开启 `nifi` 模式，样本一致率 ≥ 99.9%。
- 回滚 runbook 在实验室演练通过一次。
- 监控/告警与审计链路就绪。

## 9 实施缺口与下一步（快速清单）
- 优先：私有 Registry 推拉验证、CI staging E2E、Runbook 演练。
- 中期：manifest canonicalize、分布式锁、监控面板与告警规则。
- 长期：Full-auto orchestrator、HA NiFi 部署、SLA 验证。

## 10 弃用说明
- 原文件 `V1-NiFi统一实施可执行方案.md` 与 `NiFi后端生产化实施方案.md` 已被合并为本文件并在仓库中标注为 DEPRECATED，请以本文件为准并在 PR 合并后更新 README 指向。

---
说明：如需我把本文件提交到 `doc/merge-nifi` 分支并创建 PR，我会一并将旧文件在仓库中标记为弃用并发起 PR。 
