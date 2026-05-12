# 定时导出（Scheduled Export）

目标
- 支持按调度执行数据库导出，调度器/后端触发相同的导出任务并投递给 NiFi。

接口
- `POST /api/v1/schedules`（创建/更新调度）
- 调度触发点（后端内部 scheduler 或外部系统如 cron/airflow 调用）：`POST /api/v1/export/mysql`（内部调用）

任务 JSON
- 与导出一致，并增加 `scheduleId` 与 `runAt` 字段：
  ```json
  { "jobId":"s-01","scheduleId":"daily-01","sql":"...","runAt":"2026-05-09T00:00:00Z" }
  ```

运行时工作流
1. 调度器（后端）触发构造任务并调用导出接口或直接写入 inbox。 
2. NiFi 消费、执行并写入 done/error。 
3. 后端同步结果并更新 schedule 状态（如失败次数、上次成功时间）。

CI / 测试
- 在 CI 中模拟定时触发（调用内部 API 或脚本），验证重试与 DLQ 行为。

监控
- `schedule.run.count`, `schedule.success.rate`, `schedule.failure.count`。

回退
- 若 NiFi 不可用，记录失败并根据策略重试或标记为失败，通知运维。