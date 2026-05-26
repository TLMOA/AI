# 数据库导出（POST /api/v1/export/mysql）

目标
- 前端发起导出请求，后端在 `backend_mode=nifi` 时将任务投递给 NiFi，返回 `PENDING` 并异步同步结果。

API / 契约
- 请求：`POST /api/v1/export/mysql`
  - Body (JSON schema 简要)：
    - `jobId` (string, optional server生成)
    - `factoryId` (string)
    - `sql` (string)
    - `format` ("csv"|"json"|"tsv")
    - `targetDir` (string, 可选，逻辑目录)
    - `callbackUrl` (string, 可选)
- 响应壳：`{ code, message, data: { jobId }, traceId }`

任务 JSON（写入 inbox 示例）
- 文件路径：`/home/yhz/real_nifi_data/export_jobs/inbox/{jobId}.json`
- 示例：
  ```json
  {
    "jobId": "e-20260508-0001",
    "factoryId": "factory-001",
    "sql": "SELECT * FROM orders WHERE created_at > ?",
    "format": "csv",
    "targetDir": "output_csv",
    "submittedBy": "user-01",
    "submittedAt": "2026-05-08T12:00:00Z"
  }
  ```

`nifi_orchestrator` 建议函数签名
- `check_nifi_ready() -> bool`  # ping NiFi API
- `ensure_export_flow(process_group_id: str) -> bool`  # 验证 Flow 存在
- `start_existing_flow(process_group_id: str) -> None`
- `stop_existing_flow(process_group_id: str) -> None`
- `submit_export_job(job_json: dict) -> None`  # 写入 inbox（原子写）
- `sync_export_result()`  # 扫描 done/error，更新状态并回调

运行时工作流（后端）
1. Adapter 接收请求，生成 `jobId`、`traceId`。记录审计记录。 
2. 调用 `check_nifi_ready()`；若 false：按 tenant 策略回退或返回 503。
3. 调用 `ensure_export_flow()`；若不存在：返回 409 + 审计记录。
4. 可选 `start_existing_flow()`（仅启用已存在 Flow）。
5. 调用 `submit_export_job()`（原子写入 `/export_jobs/inbox`）。
6. 返回 `PENDING` 给前端。
7. 后端周期性/事件驱动 `sync_export_result()`：检测 `/export_jobs/done|error`，读取结果，注册文件并回调 `callbackUrl`。
8. 任务结束后按策略 `stop_existing_flow()`。

CI / GitHub Actions 示例（场景：contract-tests）
- job 流程：build -> spin-nifi-smoke -> import test flow -> run newman contract-tests -> cleanup
- spin-nifi-smoke 示例（Actions shell）:
  ```bash
  docker run -d --name ci-nifi -p 8080:8080 \
    -v $GITHUB_WORKSPACE/ci-fixtures:/opt/nifi/nifi-current/data/iot apache/nifi:latest
  # poll /nifi-api/flow/about until ready
  ```
- 导入 bundle：`v2-nifi/scripts/import_flow_to_registry.sh` 或直接把 flow XML 放到 volume。

测试清单
- 单元：`nifi_orchestrator` 各函数 mock NiFi API。 
- 契约：Local vs NiFi 对同一请求返回相同字段与 error code。 
- 集成：在 Actions 中使用 ephemeral NiFi，跑完整的 contract-tests（包含 SQL 源准备）。

监控 / 指标
- `export.count`, `export.success.count`, `export.failure.count`, `export.latency.p95`, `export.queue.depth`
- 告警：连续 5 次失败触发 P1，NiFi 不可用触发 P0。 

故障与回退策略
- NiFi 不可用：返回 503 或按 tenant 配置回退到 `local`（须审计）。
- Flow 启动失败：写入 `export_jobs/error/{jobId}.json` 并通知运维。

验收要点
- 请求能产生 inbox 文件且被 NiFi 消费并写入 done 文件。 
- Local/NiFi 在样例输入下输出 metadata 字段一致。