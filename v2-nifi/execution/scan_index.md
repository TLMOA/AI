# 目录扫描与索引（Scan / Index）

目标
- 支持增量/全量目录扫描并把结果索引到后端数据库，供前端文件中心展示。

接口
- `POST /api/v1/scan`（管理员或调度触发）
- 可选参数：`pathPrefix`, `fullScan`（bool）

运行时工作流
1. Adapter 接收触发请求，生成 `jobId` 并写入 `scan_jobs/inbox/{jobId}.json`（或直接调用 NiFi API）。
2. NiFi Flow（预置）扫描目标目录，输出索引文件到 `output_index/{jobId}.json`。
3. 后端读取索引文件，合并到 DB（去重、比对时间戳），更新文件中心。
4. 写 done/error 并回调。

CI/测试
- 集成测试在隔离容器中运行 NiFi 扫描 Flow，校验索引文件结构与去重逻辑。

监控
- `scan.count`, `scan.duration`, `scan.new_files.count`。

回退
- 扫描中断：记录失败并支持增量重试或人工补跑。