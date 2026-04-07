# Day3详细任务单（真实NiFi接入日）

## 1. 当日目标

完成从占位执行器到真实 NiFi REST 触发的切换，并在不破坏前端演示链路前提下实现可观测状态回填。

## 2. 任务拆分

1. 配置真实流程映射
   - 编辑 `v1-backend/app/nifi_flow_mapping.template.json`，将5类任务映射为真实流程ID。
2. 配置NiFi连接参数
   - `NIFI_BASE_URL`
   - `NIFI_USERNAME`
   - `NIFI_PASSWORD`
   - `NIFI_VERIFY_SSL`
   - `NIFI_OUTPUT_DIR`
3. 启动 nifi 模式后端
   - `export APP_EXECUTOR_MODE=nifi`
4. 真实链路验证
   - 创建 `CONVERT` 任务，确认状态不再是 `NIFI_FLOW_UNMAPPED`。
   - 查询 `/api/v1/jobs/{jobId}`，确认 `nifiFlowId`、`nifiActiveThreadCount` 变化。
   - 查询 `/api/v1/jobs/{jobId}/outputs`，确认产物已绑定。
5. 失败策略验证
   - 故意配置错误流程ID，确认错误码 `NIFI_EXEC_ERROR` 可读。

## 3. 完成判定

1. 至少1条任务能通过真实NiFi执行成功。
2. 任务详情可看到真实NiFi字段。
3. 前端任务页可无改动显示执行结果。

## 3.1 验证结果（2026-03-31）

1. ✅ 已完成5类任务真实流程ID映射。
2. ✅ 已完成 `CONVERT`、`TAG_AUTO`、`COPY_MULTI_FORMAT`、`IMPORT_EXTERNAL` 真实链路执行，状态均为 `SUCCEEDED`。
3. ✅ 已完成 `TAG_MANUAL` 正常映射真实链路执行，状态为 `SUCCEEDED`。
4. ✅ 任务详情已回填 `nifiFlowId`、`nifiRequestId`、`nifiActiveThreadCount`。
5. ✅ `/api/v1/jobs/{jobId}/outputs` 已绑定真实产物文件路径。
6. ✅ 失败策略验证通过：故意配置错误流程ID后返回 `NIFI_EXEC_ERROR`（404 group not found）。
7. ✅ 已增加 NiFi 调用重试与轮询参数：`NIFI_RETRY_COUNT`、`NIFI_RETRY_DELAY_MS`、`NIFI_STATUS_POLL_STEPS`、`NIFI_STATUS_POLL_INTERVAL_MS`。
8. ✅ 已增加错误分级映射：`NIFI_AUTH_ERROR`、`NIFI_NETWORK_ERROR`、`NIFI_FLOW_NOT_FOUND`、`NIFI_EXEC_ERROR`。
9. ✅ 已在前端任务详情增加错误码提示映射，失败时展示可读排查建议。
10. ✅ 已新增 nifi 模式批量回归脚本：`v1-backend/scripts/nifi_batch_smoke.py`。
11. ✅ 已新增封板验收模板：`V1执行清单/Day3封板验收记录.md`。

## 4. 注意事项

1. 先在非生产流程ID进行验证。
2. 若NiFi证书为自签，保持 `NIFI_VERIFY_SSL=false`。
3. 真实产物目录要与NiFi输出目录一致。
