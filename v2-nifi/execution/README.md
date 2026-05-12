# NiFi 接入执行文档目录

本目录存放 V1 阶段各项功能的执行文档。当前默认口径已经收敛为“预置 NiFi 容器和 Flow，后端只做接入”，因此这里按当前必用、后续待用、运维演练三层来管理。

## 当前必用

- `export_mysql_full_flow_plan.md`：数据库导出完整流程主方案
- `export_mysql.md`：数据库导出切片说明，可作为主方案的简版参考

## 后续待用

- `upload_conversion.md`：上传转换（CSV/JSON/TSV）
- `scheduled_export.md`：定时导出
- `tagging.md`：自动/手动打标
- `file_access.md`：文件预览与下载
- `scan_index.md`：目录扫描与索引
- `jobs_api.md`：任务状态与回调

## 运维演练待用

- `ops_flow_control.md`：运维 / Flow 启停 / 健康检查
- `ci_workflows.md`：CI / GitHub Actions 示例

## 使用说明

- 当前推进数据库导出时，优先读 `export_mysql_full_flow_plan.md`。
- 每个后续功能文档都按“目标 → 接口/契约 → 运行时工作流 → nifi_orchestrator 签名 → CI/测试 → 监控/审计 → 回退/故障演练”结构撰写。
- 如果某个功能还没开始实现，只需要先保留文档，不必强行把它放进主链路。
