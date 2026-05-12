# 上传转换（CSV/JSON/TSV）

目标
- 前端上传文件；在 `backend_mode=nifi` 时后端把文件或 metadata 投递给 NiFi，NiFi 完成转换并写回输出目录。

API
- `POST /api/v1/upload/inbox_csv`（同理 `/inbox_json`, `/inbox_tsv`）
- 请求包含文件 multipart/form-data 或前端先上传到对象存储后传 metadata。
- 响应：`{ code,message,data: { jobId }, traceId }`

任务 JSON 与存储
- 文件保存路径（示例）：`/home/yhz/iot/real_nifi_data/inbox_csv/{jobId}.csv`
- metadata 写入：`/home/yhz/iot/real_nifi_data/inbox_csv/{jobId}.json`
- metadata 示例：
  ```json
  { "jobId":"u-0001","uploader":"user-1","storagePath":"inbox_csv/u-0001.csv","format":"csv","options":{} }
  ```

`nifi_orchestrator` 签名（补充）
- `submit_upload_job(job_meta: dict) -> None`
- `check_nifi_ready()`、`sync_upload_result()`（与导出同模式）

运行时工作流
1. 后端接收文件并保存到后端存储（本地或对象存储），生成 `jobId`。 
2. 在 `nifi` 模式下，写入 metadata 到 `inbox_*`（原子写）。
3. 返回 `PENDING`。 NiFi Flow 读取文件并执行转换，写入 `output_*` 并生成 done/error。
4. 后端 `sync_upload_result()` 更新任务状态并注册 fileId。

CI / 测试工作流
- 使用 CI 启动 NiFi smoke 容器，并 mount `test-data/` 到 `inbox_*`。
- 运行转换 Flow 并断言输出文件内容与 Local 路径下转换结果一致（样本比对）。

失败与回退
- 文件写入失败返回 5xx；NiFi 处理失败写 error 并进入 DLQ（按策略人工补跑）。

监控
- `upload.count`, `upload.failure.count`, `upload.latency`。