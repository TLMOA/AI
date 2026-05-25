# 文件预览与下载（GET /files/{id}/preview, /download）

目标
- 统一返回由 Local 或 NiFi 生成的文件，支持流式预览与签名下载。

接口与行为
- `GET /files/{id}` 返回文件元数据（包含 `storageType, storagePath, fileId`）
- `GET /files/{id}/preview`：若文件存在返回 200 + 流；若在生成中返回 202 + `PENDING`。
- `GET /files/{id}/download`：可直接流式返回或返回短期签名 URL（如果对象存储）。

运行时工作流
1. 查询文件 registry（后端数据库）。
2. 根据 `storageType`：
   - `nifi_local`：直接读取 `/home/yhz/iot/real_nifi_data/...` 并流式返回（支持 Range）。
   - `object_store`：生成 signed URL 并返回。
3. 记录下载审计（user, jobId, time）。

测试
- 验证 Range 请求、Content-Type、断点续传。

监控
- `download.count`, `preview.latency`, `download.failure.count`。

回退
- 文件丢失返回 404 并记录审计；若文件在生成中返回 202 并提供 `jobId`。