# API 说明：Export Jobs 与静默任务（MVP）

基准路径：`/api`

1) 创建导出任务（用户侧）
- POST `/api/export-jobs`
- 请求体 (JSON)：

```
{
  "name": "每日导出客户A",
  "schedule": "daily" | "5m" | "0 2 * * *",
  "format": "csv|json|tsv",
  "destination": { "type": "local|s3|sftp", "config": {...} },
  "enabled": true
}
```

- 响应：201 Created {id: ...}

2) 列出任务
- GET `/api/export-jobs`
- 响应：200 [{id,name,schedule,format,destination,enabled,last_run,status}]

3) 更新任务
- PUT `/api/export-jobs/:id`
- 请求体：同创建（可部分字段）

4) 手动触发
- POST `/api/export-jobs/:id/trigger`
- 响应：202 Accepted

---
管理端（静默任务 admin-only）

1) 列表
- GET `/api/silent-jobs`  （返回仅 admin 可见的静默任务）

2) 更新（启用/禁用/修改频率/目的地）
- PUT `/api/silent-jobs/:id`  请求体示例： `{ "enabled": true, "schedule": "1h" }`

3) 手动触发
- POST `/api/silent-jobs/:id/trigger`

4) 审计查看
- GET `/api/silent-jobs/:id/audit`  返回审计摘要（时间、状态、接收端返回摘要、错误信息等）

---
安全说明
- 管理接口必须鉴权（仅 admin 或有 `silent:manage` 权限）。
- 所有传输使用 TLS；静默传输的凭证保存在服务端并加密。 

---
存储配置示例（destination.config）
- local: `{ "path": "nifi_data/exports/{customer_id}/{job_id}/" }`
- s3: `{ "bucket": "my-bucket", "prefix": "exports/", "region":"...", "access_key":"..." }`
- sftp: `{ "host":"...","port":22,"user":"...","path":"..." }`
