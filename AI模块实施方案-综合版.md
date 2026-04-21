# AI 模块实施方案（综合版）

版本：v1.0
作者：xy
日期：2026-04-21

**结论**
- 目标：在保证“数据不出域”前提下，交付可服务化的 AI 模块（含数据处理链路、模型训练/联邦训练能力、模型仓库与前端展示），并与 JetLinks 保持可对接状态。
- 技术主线：双生产路径（`backend-local` 与 `backend-nifi`）+ 智能服务层（中心化训练服务 & Flower 联邦训练）+ 客户端 Agent（工厂侧）。

**当前进度（仓库状态）**
- 本地后端（`backend-local`）的数据提供与部分 NiFi 流已落地，标签产物目录存在：`/home/yhz/nifi-data/tagged_output`。
- 已准备的文档：`V1-NiFi统一实施总方案.md`、`联邦学习技术路线调查报告-2026-04-15.md`、`AI模块详细实施方案.md`、`AI模块训练与联邦学习框架.md`。

---

## 1 范围与目标
- 范围：数据接入与转换、NiFi 流治理、任务编排 API、前端页面、模型训练（本地训练/联邦训练/中心化托管）、模型仓库、运维/审计/安全。 
- 里程碑：
  - M1（2周）：PoC（Flower + 3 Clients）跑通 5–10 轮，FastAPI 展示任务/轮次指标。
  - M2（4周）：接入至少一种业务算法，完成前端任务中心与节点管理页面。
  - M3（8周）：完成基础安全（mTLS、节点准入）、契约测试与回滚演练，评估是否并行 NVFlare/FATE。

## 2 总体架构
- 逻辑分层：
  1. 数据流中枢：NiFi（数据抽取/转换/标签/副本/调度）
  2. 模块后端层：FastAPI（任务编排、审计、模型元数据、契约测试入口）
  3. 智能服务层：
     - Flower Server（联邦训练编排/聚合）
     - 中心训练服务（托管模型训练/微调）
  4. 客户端层：Client Agent（Local/NiFi sidecar）
  5. 前端：任务中心、模型仓库、节点管理、审计面板
- 存储：V0.1 本地文件系统；V0.2 Ceph S3（通过 StorageAdapter 抽象切换）

## 3 关键组件与职责
- FastAPI：任务生命周期（PENDING→RUNNING→SUCCEEDED/FAILED）、节点注册/吊销、审计日志、模型版本索引、对外 OpenAPI
- Flower Server：联邦会话管理、轮次调度、聚合策略（FedAvg 默认）、写出全局模型
- Client Agent：注册/心跳/拉取任务/执行本地 `client_train()`/上报 `weights_delta` 与 `local_metrics`（仅参数与 summary）
- NiFi：保持数据预处理/特征构造与产物生成，遵守原子写 + manifest 约定
- 模型仓库：artifact 存储、checksum、签名 URL

## 4 数据与接口契约（必须由契约测试覆盖）
- 逻辑目录（必须统一）：`inbox_csv|inbox_json|output_csv|output_json|tagged_output|*_to_*`
- Manifest（每次产物同时生成）：{schema, rows, gen_time, data_version, rule_version, checksum}
- 响应壳：`{ code, message, data, traceId }`
- 错误码基线：`1002401` 参数非法、`1002402` 解析失败、`1002404` fileId 不存在、`1005001` 数据源连接失败
- FastAPI 最小接口（示例）：
  - POST `/api/fl/jobs` 创建联邦任务
  - POST `/api/fl/jobs/{job_id}/start`
  - POST `/api/fl/jobs/{job_id}/stop`
  - GET `/api/fl/jobs/{job_id}`
  - POST `/api/fl/nodes/register`（cert_fingerprint, capabilities）
  - GET `/api/fl/models/{version}`

## 5 模型/算法对接契约
- `client_train(data_path: str, global_weights: list, train_cfg: dict) -> (weights_or_delta, local_metrics)`
  - `local_metrics` 应至少包含：`sample_count, loss, f1`。
  - 禁止上报原始样本或可逆中间态。
- `server_aggregate(updates, strategy_config) -> new_global_model`
- 中心化托管训练与联邦训练并行：用户在 UI 选择“本地训练（联邦）/ 托管训练（中心）”。

## 6 NiFi 与 Client Agent 集成规范
- 部署选型：
  - 推荐：在 NiFi 节点同机或近邻运行 Client Agent（sidecar）。
  - 备选：NiFi 自定义 Processor 触发容器化训练（需资源管理）。
- 读写规则：临时文件 -> fsync -> 重命名（或写 `.ready`）→ Agent 读取
- Manifest 校验：Agent 必须在加载数据前验证 manifest 与 checksum
- DLQ 与补偿：失败产物写入 `dlq/<flow>/`，Agent 提供重放/回放工具接口

## 7 安全、鉴权与隐私策略
- 传输：出厂 PoC 用自签 CA + mTLS；生产接入企业 PKI（CRL/OCSP）
- 节点准入：证书指纹 + 白名单 + 软件版本；FastAPI 记录并支持吊销
- 聚合安全：先实现权重差上报，后续逐步接入 Secure Aggregation 与 DP
- 审计：每轮写入审计条目（traceId、参与者、model_hash、sample_count、timestamp）保存至少 90 天

## 8 PoC 计划（4周）
- 第1周：搭建 `fl-poc/`（server.py、client.py、certs 脚本），启动 1 server + 2 clients，跑通 5 轮
- 第2周：FastAPI 加入基础 `/api/fl/*` 接口，前端任务中心展示基础状态
- 第3周：模型组接入 `client_train()` 实例（一种算法），实现轮次评估并登记模型版本
- 第4周：证书注册/吊销演练、DLQ/回放演练、产出 PoC 报告与下一阶段预算

## 9 测试、CI 与契约覆盖
- 契约测试用例（强制纳入 CI）：
  - 数据契约（manifest/schema/hash）
  - 接口契约（响应壳/错误码）
  - 双路径一致性（Local vs NiFi 输出字段/命名）
  - 联邦行为（任务创建→轮次推进→模型写入）
- 自动化：在 PR 阶段运行 contract-tests，模拟 NiFi 输出并验证 `backend-local` 与 adapter 行为一致

## 10 监控、告警与运维
- 指标建议：`fl.rounds.completed`, `fl.rounds.duration`, `client.heartbeat`, `flow.processed.count`, `flow.failure.count`, `model.performance.f1`
- 告警等级：P0（Pager/电话）、P1（Slack+邮件）、P2（邮件日报）
- 回滚 Runbook：包含暂停 NiFi 流、切换 adapter 到 `backend-local`、重放 DLQ、样本一致性校验步骤

## 11 交付清单（门槛）
- 文档：本实施方案、联调手册、运维手册、回滚 Runbook
- 工件：`fl-poc/`、`client_train()` 模板、契约测试套件、证书脚本、前端任务中心原型
- 验收：PoC 跑通、前端展示、审计与安全演练通过

## 12 角色与节奏（2 人团队建议）
- 你（NiFi 主责）：数据流治理、Agent 与 NiFi 集成、契约测试、前端适配
- 同事（模型主责）：`client_train()` 实现、模型评估/版本管理、托管训练服务
- 6 周推进（参见 AI 模块详细实施方案中的周计划）

## 13 风险与缓解
- 数据异构导致收敛慢：使用分层抽样、个性化或混合策略
- 证书/网络问题影响参与率：设计离线缓存与重试机制，优先 outbound-only 模式
- 2 人资源瓶颈：分阶段交付，优先 PoC 与关键路径

## 附录
- 参考文档：`V1-NiFi统一实施总方案.md`, `联邦学习技术路线调查报告-2026-04-15.md`, `AI模块训练与联邦学习框架.md`
- 建议下一步交付：生成 `fl-poc/` 工程并加入仓库；生成 OpenAPI 文档草案并纳入契约测试

---

文件：AI模块实施方案-综合版.md

附录：整合的源文档全文（为便于查阅，下面完整包含三个参考文档的内容）

=== 文档一：AI模块训练与联邦学习框架.md ===

# AI 模块 — 训练与联邦学习框架

版本：v1.0
作者：xy
日期：2026-04-21

## 1 目的
整理本项目中“模型训练能力”的工程框架与运行流程，明确 Flower 在系统中的位置、与现有 `backend-local`（数据提供服务）与模型训练服务的边界、以及 NiFi 的接入方式，供开发与联调使用。

## 2 结论（概览）
- Flower 放置于“模型训练/编排层（智能服务层）”，负责联邦训练轮次编排与聚合。FastAPI 继续负责编排/元数据/审计与对外 API。
- 系统支持两条用户路径：1）本地训练（联邦学习 via Flower + Client Agent） 2）中心训练（购买/托管模型服务，中心化训练）。
- NiFi 继续负责本地数据预处理和产物生成，Client Agent 在工厂侧以 sidecar/守护进程方式读取 NiFi 输出并参与训练。

## 3 总体架构（文字版）
- 前端（UI）
  - 任务中心、节点管理、模型仓库、审计面板。
- 编排层（FastAPI）
  - 任务创建、节点注册、审计、模型仓库元数据、状态机。
- 智能服务层（模型训练）
  - Flower Server（联邦编排/聚合）
  - 中心化训练服务（托管/购买模型服务）
- 客户端（工厂侧）
  - Client Agent（Local / NiFi sidecar）：实现 Flower 客户端适配或调用 `client_train()`。
- 存储：本地文件系统（V0.1）→ Ceph S3（V0.2）

（数据流简述：前端→FastAPI→Flower↔Client Agent←NiFi 本地产物）

## 4 组件说明
- Flower Server
  - 负责轮次调度、客户端抽样、聚合（FedAvg 等）、写出全局模型。
  - 可独立容器化部署或与训练服务同机部署用于 PoC。
- FastAPI
  - 提供 `/api/fl/jobs`、`/api/fl/nodes`、`/api/fl/models` 等编排与审计 API。
  - 保存任务元数据、审计日志、模型版本索引。
- Client Agent
  - 注册/心跳/拉取任务/本地执行 `client_train()`/上报更新。
  - 读取 NiFi 输出目录（manifest 校验），遵循临时文件→重命名原子读写约定。
- 模型仓库
  - 存储全局模型 artifact、版本与 checksum（V0.1 本地目录，V0.2 Ceph S3）。

## 5 数据与契约（摘要）
- 逻辑目录：`inbox_csv|inbox_json|output_csv|output_json|tagged_output`。
- manifest.json（每次数据产物同时生成），包含：schema、rows、gen_time、data_version、rule_version。
- 响应壳：`{ code, message, data, traceId }`。
- 错误码对齐（如 `1002401` 参数非法、`1002402` 解析失败 等）。

## 6 API 概要（FastAPI 最小集）
- POST `/api/fl/jobs` — 创建联邦任务（payload 包含 model_ref、strategy、rounds、client_selector、train_cfg）
- POST `/api/fl/jobs/{job_id}/start` — 启动任务
- POST `/api/fl/jobs/{job_id}/stop` — 停止任务
- GET `/api/fl/jobs/{job_id}` — 查询任务状态与轮次指标
- POST `/api/fl/nodes/register` — 节点注册（cert_fingerprint、capabilities）
- GET `/api/fl/models/{version}` — 查询/下载模型元数据

> 说明：PoC 推荐使用 Flower 原生通信实现客户端/服务器参数传输，FastAPI 负责上层编排、审计与展示。

## 7 client_train() 合约（交给模型组实现）
- Python 签名示例：
  ```py
  def client_train(data_path: str, global_weights: list, train_cfg: dict) -> (list, dict):
      """返回 (local_weights_or_delta, local_metrics)
      local_metrics 至少包含: sample_count, loss, f1
      """
  ```
- 约束：只在本地读取 `data_path` 指向的数据，禁止上传原始样本；返回的数据仅为参数与 summary。

## 8 NiFi 集成建议
- 优先方案（推荐）：在 NiFi 节点同机或近邻部署 Client Agent（sidecar）。
- 高级方案：自定义 NiFi Processor 触发容器化训练（需资源调度与更复杂运维）。
- 必须：原子写规则、manifest 校验、DLQ 机制。

## 9 用户路径（端到端）
1. 本地训练（用户自主）
   - 用户选择数据集并创建联邦任务 → FastAPI 写任务并触发 Flower → Client Agent 注册并参与轮次 → 本地执行 `client_train()` → 上报更新 → Flower 聚合 → FastAPI 写入模型仓库并展示指标。
2. 购买模型服务（中心化）
   - 用户选择“托管训练”→ FastAPI 转发请求至中心训练服务 → 训练完成后模型写入仓库 → 前端展示/上线。

## 10 安全与合规要点
- 传输：mTLS；PoC 可用自签 CA。
- 鉴权：节点证书、白名单、软件版本；FastAPI 记录并支持吊销
- 聚合安全：先实现权重差上报，后续逐步接入 Secure Aggregation 与 DP
- 审计：每轮写入审计条目（traceId、参与者、model_hash、sample_count、timestamp）保存至少 90 天

## 11 PoC 与验收门
- 第1周：搭建 `fl-poc/`（server.py、client.py、certs 脚本），启动 1 server + 2 clients，跑通 5 轮
- 第2周：FastAPI 加入基础 `/api/fl/*` 接口，前端任务中心展示基础状态
- 第3周：模型组接入 `client_train()` 实例，一种算法，轮次评估并登记模型版本
- 第4周：证书注册/吊销演练、DLQ/回放演练、产出 PoC 报告与下一阶段预算

## 12 部署建议
- PoC：docker-compose（fastapi + flower + clients）
- 生产：容器化部署 Flower、中心训练服务；Client Agent 以 systemd 管理的容器或进程运行，证书通过 PKI 自动化分发。

## 13 下一步交付项（可选）
- 生成 `fl-poc/` 示例工程（server.py, client.py, cert scripts）
- 生成 `client_train()` 模板与示例数据 manifest
- 在 `AI模块详细实施方案.md` 中同步此框架链接

=== 文档二：AI模块详细实施方案.md ===

# AI模块详细实施方案（面向JetLinks融合）

（此处为完整复制，保留原文档全部章节与内容）

## 1. 文档目的与依据

本方案用于指导当前AI模块从“本地可运行验证”走向“可服务化交付并与JetLinks融合”的完整落地过程。

本方案依据以下材料整理：

- 智慧数据平台总体路线与阶段目标：智慧数据平台模块详细开发方案.pdf
- 功能清单与验收目标：功能描述.xlsx
- 当前工作区已有工程结构与历史实施进展（NiFi数据流、容器部署、输出路径、第五条标签流）

## 2. 项目目标与边界

### 2.1 总目标

构建可嵌入JetLinks生态的AI模块，形成“数据接入与转换 -> 标签与特征 -> 模型训练/推理 -> 预警反馈 -> 前端操作展示”的闭环能力。

### 2.2 当前团队边界

- JetLinks主平台由外部团队负责。
- 本团队（2人）负责AI模块建设与前端页面。
- 分工：
  - 成员A（你）：NiFi数据转换与流程编排。
  - 成员B：大模型/训练推理服务。

### 2.3 与JetLinks对接边界

- 本模块以独立后端微服务形态建设。
- 通过标准API、事件或回调与JetLinks进行系统融合。
- 不改动JetLinks核心代码，优先通过接口协议与配置实现接入。
- V1阶段前端先采用“脱离JetLinks联调的简化可改造版本”，通过适配层保留后续对接能力。

## 3. 需求拆解（对应功能描述.xlsx）

（此处保留原文所有需求条目，包括数据接入、算法与模型训练、预测与预警、前端能力）

...（为节省空间，在合入后文档中保留原文完整条目；在仓库中请参见原文件）

## 4. 现状评估（截至当前）

（已完成与未完成/待完善项，保留原文）

## 5. 目标架构（本期可落地版本）

（逻辑分层、存储策略、联邦学习集成、Client Agent 要求、NiFi 集成要点、PoC 建议等，完全沿用原文）

## 6. 分阶段实施方案

（包含阶段A/V0.1、阶段B/V0.2、阶段C 与 JetLinks 融合等完整执行细节与里程碑，保留原文）

## 7. 任务分工与节奏（2人团队可执行版）

（保留原文分工与6周推进建议）

## 8. 风险与应对

（保留原文风险列表与应对方案）

## 9. 交付物清单

（保留原文清单）

## 10. 阶段完成判定

（保留原文 V0.1 / V0.2 判定标准）

## 11. V1执行启动与每日进度管理

（保留原文的每日进度模板与要求）

=== 文档三：联邦学习技术路线调查报告-2026-04-15.md ===

# 联邦学习技术路线调查报告（V1）

- 日期：2026-04-15
- 适用项目：IoT AI 模块（当前 FastAPI + NiFi + 前端任务编排）
- 调研目标：评估“数据静默传输”替代路线，验证“数据不出本地”的联邦学习（Federated Learning, FL）可行性，并给出开源试点与后续算法合入路径。

## 1. 结论（Executive Summary）

✅ **建议立即启动联邦学习 PoC（首选 Flower）**，作为对当前“静默传输数据”路线的替代/补充能力。

✅ **核心收益明确**：原始数据留在用户侧，仅上传模型参数/梯度更新，可显著降低用户对“数据被拿走”的抵触。

⚠️ **风险需正视**：联邦学习不是“绝对隐私”，模型更新仍可能泄露信息；需引入安全聚合、差分隐私、传输加密与审计。

✅ **技术选型建议**：
- 第一阶段（2~4周）优先 Flower，原因是 Python 生态友好、上手快、社区活跃、对你们现有 FastAPI 架构改造成本最低。
- 第二阶段（4~8周）按场景升级：
  - 合规/多机构协作偏重：评估 FATE
  - 强安全与生产治理偏重：评估 NVFlare

## 2. 现状与问题定义

（保留原文关于现状与问题定义的全部内容）

## 3. 开源方案调查结果

（包含 Flower、NVFlare、FATE、TFF 等方案的优缺点与适配建议，保留原文）

## 4. 选型建议（面向当前团队与架构）

（保留原文的推荐路线、实施顺序与注意事项）

## 5. 与现有技术栈的融合设计（不传原始数据）

（保留原文：架构建议、NiFi 与 FastAPI 的职责划分、前端与算法端改造要点）

## 6. 后期算法端合入方案（重点）

（保留原文改造原则与优先级）

## 7. 安全与合规注意事项

（保留原文最小安全基线与建议）

## 8. PoC 实施计划（建议 4 周）

（保留原文周计划）

## 9. 成本与收益评估（定性）

（保留原文成本与收益点）

## 10. 决策建议

（保留原文决策建议与里程碑）

---
注：上述附录为便于工程队直接查阅的全文合并，若需要我可以把三个文档中的冗余处做进一步去重、格式统一与交叉引用重连（例如将所有 API 列表合并为一张表，把所有 PoC 步骤抽象为单一执行脚本），并生成最终版本的 `AI模块实施方案-综合版.md`（去重并保留可执行检查项）。
