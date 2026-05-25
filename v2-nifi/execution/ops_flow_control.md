# 运维 / Flow 启停 / 健康检查

目标
- 提供运维受控的 Flow 管理 API（启停、导入、健康检查），并记录审计与变更历史。

管理接口（受限）
- `GET /api/v1/internal/nifi/health` → 返回 NiFi 健康
- `POST /api/v1/internal/nifi/flows/{processGroupId}/start`
- `POST /api/v1/internal/nifi/flows/{processGroupId}/stop`
- `POST /api/v1/internal/nifi/import`（导入 Flow bundle，需运维审批）

`nifi_orchestrator` 建议扩展
- `import_flow(bundle_path: str, target_bucket: str) -> processGroupId`
- `get_flow_status(process_group_id: str) -> dict`
- `list_flows()`

运行时工作流
1. 管理员在运维页面请求启停/导入，API 验证权限与审批（可选）。
2. 后端调用 NiFi API（或 Registry API）执行操作并轮询结果。 
3. 操作成功或失败均写审计日志（operator、time、operation、traceId、result）。

CI/演练
- 提供演练脚本：导入 demo bundle → 启动 Flow → 运行 contract-tests → 触发回滚脚本。

审计与回滚
- 每次导入/启停操作在 `audit.nifi_ops` 表写入操作记录。 
- 回滚 runbook：停止 Flow → 从 Registry 回滚到上一个版本 → 验证样本一致性 → 重新启用。

监控
- `nifi.health`, `flow.uptime`, `flow.error.rate`, `flow.version`。

安全
- 管理接口仅对管理员账号开放，并建议双人审批或短期任务令牌（one-time token）。