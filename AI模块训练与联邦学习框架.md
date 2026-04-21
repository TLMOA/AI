# AI 模块 — 训练与联邦学习框架

版本：v1.0
作者：xy
日期：2026-04-21

## 1 目的
整理本项目中“模型训练能力”的工程框架与运行流程，明确 Flower 在系统中的位置、与现有 `backend-local`（数据提供服务）与模型训练服务的边界、以及 NiFi 的接入方式，供开发与联调使用。

## 2 结论（概览）
- Flower 放置于“模型训练/编排层（智能服务层）”，负责联邦训练轮次编排与聚合。FastAPI 继续负责编排/元数据/审计与对外 API。
- 系统支持两条用户路径：1）本地训练（联邦学习 via Flower + Client Agent） 2）中心训练（购买/托管模型服务，中心化训练）。
- NiFi 继续负责本地数据预处理和产物输出，Client Agent 在工厂侧以 sidecar/守护进程方式读取 NiFi 输出并参与训练。

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
- 优先方案（推荐）：在 NiFi 节点同机或近邻部署 Client Agent（sidecar），NiFi 将数据产物写到约定目录，Agent 读取并训练。
- 高级方案：自定义 NiFi Processor 触发容器化训练（需资源调度与更复杂运维）。
- 必须：原子写规则、manifest 校验、DLQ 机制。

## 9 用户路径（端到端）
1. 本地训练（用户自主）
   - 用户选择数据集并创建联邦任务 → FastAPI 写任务并触发 Flower → Client Agent 注册并参与轮次 → 本地执行 `client_train()` → 上报更新 → Flower 聚合 → FastAPI 写入模型仓库并展示指标。
2. 购买模型服务（中心化）
   - 用户选择“托管训练”→ FastAPI 转发请求至中心训练服务 → 训练完成后模型写入仓库 → 前端展示/上线。

## 10 安全与合规要点
- 传输：mTLS；PoC 可用自签 CA。
- 鉴权：节点证书、白名单、任务签名。
- 隐私增强：逐步引入 Secure Aggregation / 差分隐私。
- 审计：每轮记录参与者、样本数、模型 hash、traceId。

## 11 PoC 与验收门
- PoC 最小交付：1 x FastAPI（编排） + 1 x Flower Server + 2~3 Client Agent（Local/NiFi 模拟），完成 5–10 轮训练并展示指标。
- 验收要点：Client 未上传原始样本、FastAPI 能显示任务/轮次/参与节点、证书注册与心跳功能。

## 12 部署建议
- PoC：docker-compose（fastapi + flower + clients）
- 生产：容器化部署 Flower、中心训练服务；Client Agent 以 systemd 管理的容器或进程运行，证书通过 PKI 自动化分发。

## 13 下一步交付项（可选）
- 生成 `fl-poc/` 示例工程（server.py, client.py, cert scripts）
- 生成 `client_train()` 模板与示例数据 manifest
- 在 `AI模块详细实施方案.md` 中同步此框架链接

---
文件：AI模块训练与联邦学习框架.md
