# V1 NiFi 统一实施总方案（精简可执行版）

版本：v2.1
作者：xy
日期：2026-04-09

**执行摘要**

本文件为 V1 阶段的主线实施方案（精简可执行版）。核心目标：同时交付“本机后端（local）”与“NiFi 后端（nifi）”两条生产路径，保持接口与产物语义一致，并在上线前补齐 P0（阻断风险）与 P1（稳定性）事项以保障可回滚、可观测与安全合规。

本文档结构：执行摘要 → 范围与决策 → 关键契约 → 优先级行动清单（P0/P1/P2）→ 可交付物与负责人 → 快速验收与演练 → 附录（契约测试与回滚 runbook 模板定位）。

## 1. 参考与优先级来源

1. 优先级：`智慧数据平台模块详细开发方案.pdf` > 本文件 > 历史文档（`nifi-migration/PLAN.md` 等）。
2. 本文件为交付与运维主线，所有变更须归档并在每日/周报中记录。

## 2. 总体决策（已锁定）

1. 双生产：并行交付 `backend-local` 与 `backend-nifi`，功能对等。  
2. 前端透明切换：同一页面支持两条后端适配层（adapter 模式）。  
3. 目录隔离：两条路径物理隔离但逻辑目录一致（见第 4 节）。  
4. 验收口径：同一批次仅使用一种后端口径进行验收。

## 2.5 交付路径与目标（从本地实现到双后端生产）

本项目核心目标是从当前已存在的本地后端实现（`backend-local`）平滑推进到 `backend-nifi` 并最终实现双生产可交付。交付路径需可回溯、可回滚并由契约测试保证双路径语义一致。推荐步骤：

- Step A（现状→本地稳定）
	- 把 `backend-local` 的基本功能（上传/导出/转换/打标/回调）覆盖完整并通过本地契约测试。建立 `contract-tests` 并纳入 CI。验收：本地路径在实验室通过全部契约测试。
- Step B（适配层与适配器）
	- 在前端与后端之间加入 adapter 层，前端通过逻辑目录请求，不暴露物理实现。实现 `nifi_adapter`/`nifi_connector` 的代码样板，使前端切换仅需翻转配置。验收：adapter 层在本地模拟 NiFi 行为且通过契约测试。
- Step C（NiFi 实验室验证）
	- 在实验室环境部署 NiFi（Registry+Provenance 配置），将 adapter 切换到真实 NiFi 流程，运行完整契约测试与故障注入（回退、DLQ）。验收：NiFi 路径在实验室通过契约测试、DLQ 行为符合规范、回滚 runbook 经演练通过。
- Step D（灰度与小范围生产）
	- 在受控工厂或灰度环境放量，监控一致率/延迟/失败率，若达到量化目标方可放量。验收：样本一致率与 SLA 达标，告警合格。
- Step E（双生产切换）
	- 完成小范围验收后，逐步在更多工厂开启 NiFi 路径，保持 `backend-local` 为回退路径，最终达到双路径并行生产能力。

在以上每一步，安全（鉴权/证书/密钥管理）、监控与回滚演练为必须项，详见第 10 节与仓库补充文件。

## 3. 双生产版本定义（简明）

- Local：适用于网络受限或运维能力弱的工厂；单机后端处理；本地文件系统为主。  
- NiFi：适用于复杂流程与高并发场景；NiFi（单机或集群）为处理引擎；建议集群部署并映射到 `NIFI_BACKEND_BASE_DIR`。

## 4. 功能对等与契约要点

必须对等的功能域：任务管理、上传/转换、文件中心、标签（人工/自动）、回调/结果回写、审计与日志追踪。  

契约要点（必须冻结并被契约测试覆盖）：

- 响应壳：`{ code, message, data, traceId }`。  
- 文件元数据：`fileId,fileName,fileFormat,fileSize,storageType,storagePath,createdAt,jobId`。  
- 可观测字段：`operation,sourcePath,targetPath,status,errorCode,errorMessage,durationMs`。  
- 错误码基线（例）：`1002401`（参数非法）、`1002402`（解析失败）、`1002404`（fileId 不存在）、`1005001`（数据源连接失败）。

## 附：Local / NiFi 切换需求补充

为支持在生产线上按需在 `backend-local` 与 `backend-nifi` 之间切换执行路径，补充以下可执行需求与实现要点：

- 概要：在前端不改动页面结构的前提下，新增一个全局切换开关（Local ↔ NiFi），切换后同一接口请求由服务端路由到 Local 或 NiFi 实现，NiFi 能力优先使用，缺失或不可用时回退 Local。

- 前端行为：
	- 增加全局切换按钮（放置于页面工具栏或顶部），显示当前模式（Local / NiFi）。
	- 切换粒度推荐按工厂（tenant）级别持久化，也可支持 session/user 级作为调试或临时切换手段。
	- 前端请求路径和参数保持不变；可选暴露调试用请求头 `X-Backend-Mode`（仅允许运维/开发使用）。

- 后端路由（Adapter）设计：
	- 在网关/服务层实现 Adapter/Router：读取工厂级配置（或 session），决定路由。优先尝试 NiFi：若存在对应 Flow 且健康可用，则调用 NiFi；否则回退到 Local 实现。
	- 能力映射表维护每个逻辑操作到 NiFi flow id（例：`export_mysql -> nifi_flow_export_mysql_v1`）。
	- 路由决策、执行结果须记录到日志与监控（包含 `traceId`、chosen_backend、flow_id、duration、error）。

- 错误、回退与幂等性：
	- NiFi 执行失败应返回统一响应壳并触发回退或人工干预流程；对非幂等操作回退需谨慎并写入审计记录。
	- 建议所有写产物采用原子写（临时文件 + 重命名）并使用 `jobId+targetPath` 作为幂等键。

- 权限与治理：
	- 切换按钮应受限（系统管理员或工厂运维），切换操作须有审计记录（操作者、时间、旧值/新值、traceId）。

- 契约测试与验收：
	- 必须保证 Local 与 NiFi 在相同输入下返回相同响应 schema 与关键字段（契约测试覆盖）。
	- 样本一致率与关键字段哈希一致率须满足总体量化目标（见第 8 节）。

- 监控与告警：
	- 增加指标：`operation.route.choice`, `nifi.flow.execution.count`, `nifi.flow.failure.count`, `local.execution.count`, `flow.latency.p95`。
	- 当 NiFi 连续失败或不可用时触发告警并建议自动或人工切回 Local。

- 回滚与演练：
	- 在阶段 2 中完成回滚 runbook 演练：暂停 NiFi 流 → 切换 adapter 到 Local → 校验样本一致性 → 恢复或回放 DLQ。

- 可交付物（补充）：
	1. `nifi_adapter` 服务端模块与能力映射表示例（代码样板）。
	2. 前端切换按钮组件（含权限判断与持久化示例）。
	3. 契约测试用例（导出/上传/打标 场景）与演练记录。

以上补充为本方案对“切换能力”的具体要求，需纳入 Phase B（适配层）与 Phase C（NiFi 实验室验证）的实施与验收条目中。

### 5.1 基于需求基线的对等范围（强制）

以下条目以 `V1执行清单/需求规格-NiFi-文件输出.md` 为基线，要求 `backend-local` 与 `backend-nifi` 双路径均实现：

1. MySQL -> CSV 导出
	- 逻辑目录：`output_csv`
	- 格式要求：UTF-8、逗号分隔、LF、含表头
	- 命名要求：`<table>_export_YYYYMMDD_HHMMSS.csv`
2. MySQL -> JSON 导出
	- 逻辑目录：`output_json`
	- 格式要求：NDJSON（每行一条 JSON）
	- 命名要求：`<table>_export_YYYYMMDD_HHMMSS.json`
3. 上传 CSV -> 转换
	- 逻辑目录：`inbox_csv` -> `csv_to_json` 或 `csv_to_tsv`
	- 命名要求：`uploaded_<user>_csv_YYYYMMDD_HHMMSS.<ext>`
4. 上传 JSON -> 转换
	- 逻辑目录：`inbox_json` -> `json_to_csv` 或 `json_to_tsv`
	- 输入支持：NDJSON / JSON 数组 / 单对象
	- 命名要求：`uploaded_<user>_json_YYYYMMDD_HHMMSS.<ext>`
5. 上传 TSV -> 转换
	- 逻辑目录：`inbox_tsv` -> `tsv_to_json` 或 `tsv_to_csv`
	- 命名要求：`uploaded_<user>_tsv_YYYYMMDD_HHMMSS.<ext>`
6. 打标产物输出
	- 逻辑目录：`tagged_output`
	- 命名要求：`<source>_tagged_YYYYMMDD_HHMMSS.<ext>`
	- 写入要求：原子写（临时文件 + 重命名）

7. MySQL -> TSV 导出
	- 逻辑目录：`output_tsv`
	- 格式要求：UTF-8、制表符分隔、LF、含表头
	- 命名要求：`<table>_export_YYYYMMDD_HHMMSS.tsv`

8. 定时导出（Scheduled export）
	- 逻辑目录：`export`
	- 说明：支持基于调度器的定时导出（例如每日/每小时），包括 CSV/JSON/TSV 格式，需同样遵守命名与原子写入规则
	- 命名建议：`<table>_scheduled_export_YYYYMMDD_HHMMSS.<ext>`

### 5.2 接口与响应对等（强制）

1. 双路径必须返回统一响应壳：`{ code, message, data, traceId }`。
2. 文件元数据字段保持一致：`fileId, fileName, fileFormat, fileSize, storageType, storagePath, createdAt, jobId`。
3. 冻结错误码语义保持一致：
	- `1002401` 参数或输入非法
	- `1002402` 文件解析失败
	- `1002404` 文件不存在
	- `1005001` 数据源连接失败
4. 禁止在双路径中出现“同类错误不同 code”或“同 code 不同语义”。

### 5.3 latest 与追加策略（口径一致）

1. 导出链路支持 `append_to_latest` 语义。
2. 若某工厂未启用 latest 追加，必须在工厂参数包中显式关闭，并在验收单记录。
3. 双路径在同一工厂配置下，latest 行为必须一致。

### 5.4 可观测字段对等（强制）

1. 双路径均输出：`operation, sourcePath, targetPath, status, errorCode, errorMessage, durationMs`。
2. 双路径均要求 `STARTED` 与 `SUCCEEDED/FAILED` 成对日志。
3. 任一失败请求必须可用 `traceId` 完整追溯。

## 5. 关键接口（示例）

常用接口示例（详见 `V1执行清单/后端V1接口字段级清单.md`）：

- `POST /api/v1/export/mysql`（导出）  
- `POST /api/v1/upload/inbox_csv`、`/inbox_json`、`/inbox_tsv`（上传转换）  
- `POST /api/v1/tags/manual-table`（手动打标）  
- 统一响应与错误码策略如第 4 节所述。

## 6. 目录与存储规范（要点）

- 逻辑目录集合（必须统一）：`output_csv|output_json|output_tsv|inbox_csv|inbox_json|inbox_tsv|*_to_*|tagged_output`。  
- 物理映射：Local 与 NiFi 分别映射到不同根目录，前端仅识别逻辑目录。  
- 写入原则：临时文件 -> 完整性校验 -> 原子重命名；幂等键为 `jobId+targetPath`。
 - 物理映射：Local 与 NiFi 分别映射到不同根目录，前端仅识别逻辑目录。为避免混淆，建议本地后端使用物理目录 `nifi_data`，NiFi 写入使用同一路径下的 `real_nifi_data`（例如同一挂载点下的并列目录），两者在物理上隔离但逻辑目录一致。
 - 写入原则：临时文件 -> 完整性校验 -> 原子重命名；幂等键为 `jobId+targetPath`。

## 7. 三阶段执行路线（简明）

阶段 1（Local 验证）→ 阶段 2（NiFi 实验室验证：含故障注入/回滚与安全校验）→ 阶段 3（工厂灰度与交付）。

阶段细化与验收门（示例）
- 阶段 1（Local 验证，目标 0-2 天）
	- 目标：`backend-local` 功能完整、契约测试覆盖、基础监控到位。验收门：CI contract-tests 通过。
- 阶段 2（NiFi 实验室，目标 3-7 天）
	- 目标：NiFi 流程接入、回滚 runbook 演练（参见 `V1执行清单nifi/rollback_runbook.md`）、故障注入与 DLQ 验证、鉴权（mTLS/Vault）校验。验收门：实验室端到端契约测试通过、一次回滚演练记录。
- 阶段 3（灰度与交付，目标 7-21 天）
	- 目标：小范围灰度放量，监控样本一致率 ≥ 99.9%、延迟/失败率满足量化目标。验收门：业务方签字、切换计划与最终回滚窗口确认。

每阶段完成前务必执行：契约测试、回滚演练、监控与告警验证、鉴权与审计检查。

## 8. 量化目标（关键）

- 一致性：样本一致率 ≥ 99.9%；关键字段哈希一致率 ≥ 99.99%。  
- 可用性：服务可用性 ≥ 99.9%。  
- 延迟：上传→产物→回调 P95 < 3s（可签字调整）。  
- 失败率：NiFi 处理失败率 < 0.1%。

## 9. JetLinks 融合边界

- 服务化对接，不修改 JetLinks 核心。  
- 联调必须走契约测试与接口白盒校验，放量前完成 end-to-end 验收。

## 10. 运维与安全要点

运维与安全为项目可交付的核心要素，必须与每一阶段并行验证，以下为融合进主线的可执行要点：

1) 鉴权与密钥管理（必须在阶段 1-2 固化）
	- 边界鉴权：建议 OAuth2/JWT，access token 有效期短（5-30 分钟），配合 refresh token。前端请求必须携带 `Authorization` header。错误语义：401/403 对应响应壳 `{ code, message, data, traceId }`。
	- 服务间：生产环境建议 mTLS；所有敏感凭据与证书通过 Vault 管理并用短期凭证访问。详见补充文件：`V1执行清单nifi/auth_and_security_spec.md`。

2) 回滚与演练（必须在阶段 2 演练通过）
	- 回滚 Runbook 必须在仓库中，并在实验室演练成功一次（参见 `V1执行清单nifi/rollback_runbook.md`）。
	- 回滚流程应包含：暂停 NiFi 流、切换 adapter 到 `backend-local`、样本一致性校验、恢复/重放 DLQ。演练记录为验收门之一。

3) 契约测试与 CI（持续保障）
	- 所有接口契约（响应壳、元数据字段、错误码、latest 行为）必须由自动化测试覆盖并作为 PR 阻断条件。CI 应能模拟或 mock NiFi 行为以验证 `backend-local` 与 adapter。示例路径：`V1执行清单nifi/contract-tests/`。

4) 数据完整性与原子写
	- 所有产物写入遵循：临时文件 -> fsync -> 计算 checksum -> 原子重命名 -> 写入元数据索引。并发采用 `jobId+targetPath` 作为幂等键，必要时借助分布式锁。

5) 错误处理、重试与 DLQ
	- 定义统一策略：最大重试 3 次、指数退避、失败写入 `dlq/<flow>/`，并生成可回放的元数据与日志。

6) 监控、告警与审计
	- 指标（建议）：`flow.processed.count`, `flow.success.rate`, `flow.failure.count`, `flow.latency.p95`, `processor.cpu.usage`, `disk.util`, `provenance.storage.used`。
	- 告警分级：P0（Pager/电话）、P1（Slack+邮件）、P2（邮件日报）。告警规则与告警接收人清单需在阶段 1 完成并演练。
	- 审计日志需记录关键操作（谁/何时/何操作/traceId），保留期建议 90 天。

7) 备份与恢复
	- MySQL：每日增量/周全量，定期演练恢复。
	- 产物目录：定期同步到对象存储并验证可读性；恢复过程纳入回滚演练。

8) NiFi 运行要点（配置建议）
	- 使用 NiFi Registry 管理 flow 版本；生产环境至少 3 节点（HA）；Provenance 短期保留 7 天、长期摘要 90 天；Backpressure 阈值根据流量调整（示例值见补充详细方案）。

量化验收（每阶段）
- 一致性：样本一致率 ≥ 99.9%；关键字段哈希一致率 ≥ 99.99%。
- 可用性：服务可用性 ≥ 99.9%。
- 延迟：上传→产物→回调 P95 < 3s（如有调整需变更记录）。

注：以上运维/安全项已在仓库中补充为可执行的文档与脚本（鉴权规范、回滚 runbook、原子写示例、契约测试模板），请参见仓库相应文件并在阶段 1-2 中完成演练与 CI 集成。


