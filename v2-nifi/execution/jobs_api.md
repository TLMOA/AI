# 任务状态与回调（/jobs, /jobs/{id}）

目标
- 提供统一的任务查询与回调机制，后端负责从 NiFi 写入的 done/error 中同步任务状态并触发回调。

接口
- `GET /api/v1/jobs`（列表）
- `GET /api/v1/jobs/{id}`（详情）
- 回调：后端在任务完成时向 `callbackUrl` 发起 `POST`，payload 包含 `{ jobId, status, artifacts, traceId }`。

运行时工作流
1. 后端 `sync_export_result()` 定期或事件驱动扫描 `export_jobs/done|error`，解析状态与 artifacts。 
2. 更新任务表并触发回调（幂等实现，记录 `callbackId`）。
3. 若回调失败，按指数退避重试 N 次，最后写告警并人工处理。 

幂等与去重
- 回调必须包含 `callbackId` 或使用 `jobId` + `attempt` 做去重，后端记录已成功回调的 `callbackId`。

测试
- 覆盖重复回调、网络故障与重试策略，验证幂等性。 

监控
- `job.pending.count`, `job.succeeded.count`, `job.failed.count`, `callback.failure.count`。