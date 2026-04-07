# Day3封板验收记录

## 1. 基本信息

- 验收日期：2026-03-31
- 验收人：xy
- 环境：本地 Linux + Docker NiFi
- 后端地址：http://127.0.0.1:8082/api/v1
- 执行模式：nifi

## 2. 接口与能力验收

- [x] 流程映射已配置5类任务
- [x] CONVERT 任务成功
- [x] TAG_AUTO 任务成功
- [x] COPY_MULTI_FORMAT 任务成功
- [x] IMPORT_EXTERNAL 任务成功
- [x] TAG_MANUAL 任务成功
- [x] 失败策略验证通过（错误流程ID触发失败）
- [x] 错误分级生效（AUTH/NETWORK/FLOW_NOT_FOUND/EXEC）
- [x] 产物绑定路径正确

## 3. 前端验收

- [x] 任务类型下拉含5类任务
- [x] 失败任务展示错误码与中文提示
- [x] 任务详情展示 flowId/retry 信息
- [x] 文件中心可预览与下载产物

## 4. 关键证据

### 4.1 任务结果摘要

| 任务类型 | jobId | 状态 | errorCode | nifiFlowId |
|---|---|---|---|---|
| CONVERT | job_20260331_104440_7b8d | SUCCEEDED |  | 2d06d224-019d-1000-2be8-56312552a547 |
| TAG_AUTO | job_20260331_104443_5167 | SUCCEEDED |  | 3ce997bf-019d-1000-5dfc-c83dd845bf44 |
| COPY_MULTI_FORMAT | job_20260331_104447_b42e | SUCCEEDED |  | 2d06d27c-019d-1000-a047-d1463163099e |
| IMPORT_EXTERNAL | job_20260331_104450_f607 | SUCCEEDED |  | 2d06d25d-019d-1000-6786-42cd007ad891 |
| TAG_MANUAL | job_20260331_104453_4391 | SUCCEEDED |  | 3ce997bf-019d-1000-5dfc-c83dd845bf44 |

### 4.2 日志/截图清单

- 后端日志：
- 任务详情截图：
- 文件产物截图：
- 失败策略截图：

## 5. 压测摘要（短时）

- 脚本：v1-backend/scripts/nifi_batch_smoke.py
- 命令：
  - python scripts/nifi_batch_smoke.py --base-url http://127.0.0.1:8082/api/v1 --rounds 2 --json-out ./data/generated/day3-batch-report.json
- 结果：已执行 2 轮验证，10/10 任务成功。
- 报告文件：v1-backend/data/generated/day3-batch-report-round2.json
- 失败明细：无

## 6. 结论

- 验收结论：通过
- 风险项：NiFi 流程ID变更时需同步映射文件。
- 后续动作：补齐截图证据并归档到联调记录。
