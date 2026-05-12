# 自动/手动打标（Tagging）

目标
- 支持自动和手动打标流程，NiFi 负责可编排的标注处理并产出 `tagged_output`。

接口
- 自动：`POST /api/v1/tags/auto`（提交数据源或 fileId）
- 手动：`POST /api/v1/tags/manual-table`（含 operator 信息）

任务 JSON
- 示例：
  ```json
  {"jobId":"t-0001","fileId":"f-001","rules":["ruleA"],"outputDir":"tagged_output","submittedBy":"op-1"}
  ```

运行时工作流
1. Adapter 接收请求并生成 `jobId`。写入 `tagging_jobs/inbox/{jobId}.json`。
2. NiFi Flow 执行标注逻辑，写 `tagged_output/{...}`，并在 done/error 写状态文件。
3. 后端 `sync` 更新文件注册与 metadata，通知前端。 

CI/测试
- Contract-tests 包含标注字段一致性检查。集成使用 sample files 验证行为。 

监控
- `tagging.count`, `tagging.failure.count`, `tagging.quality.sample`（抽样比对）。

回退与补跑
- 标注失败写入 DLQ，提供手动补跑接口（受审计）。