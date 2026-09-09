# 需求规格：NiFi 与网站间的文件导出/上传/转换/打标输出

## 1. 文档目的
- 统一网站与 NiFi 之间的文件目录、命名、格式与处理规则。
- 给出当前实现状态、差异收口清单和后续可迁移到 NiFi 的结构目标。
- 本文为执行版规格，替代历史对话式记录。

## 2. 决策快照（最新确认）
- 管理根目录保持不变：`/home/yhz/nifi-data`。
- 打标目录策略保持当前实现：`/home/yhz/nifi-data/tagged_output`（不改到 `output_csv/tagged_output`）。
- 差异清单中的第 4 项（上传链路 latest 追加）暂不实施。
- 差异清单中的第 6 项（样例回归集）暂后置，迁 NiFi 前再推进。

## 3. 范围与目录约定
### 3.1 管理根目录
- `/home/yhz/nifi-data`

### 3.2 业务目录
- `output_csv`
- `output_json`
- `output_tsv`
- `inbox_csv`
- `inbox_json`
- `inbox_tsv`
- `csv_to_json`
- `json_to_csv`
- `csv_to_tsv`
- `json_to_tsv`
- `tsv_to_json`
- `tsv_to_csv`
- `tagged_output`

## 4. 需求基线
### 4.1 MySQL -> CSV
- 输出目录：`/home/yhz/nifi-data/output_csv`
- 格式：CSV，UTF-8，逗号分隔，LF，含表头
- 命名：`<table>_export_YYYYMMDD_HHMMSS.csv`
- latest 策略：支持写入 `*_export_latest.csv`，并可追加

### 4.2 MySQL -> JSON
- 输出目录：`/home/yhz/nifi-data/output_json`
- 格式：NDJSON（每行一条 JSON）
- 命名：`<table>_export_YYYYMMDD_HHMMSS.json`
- latest 策略：支持写入 `*_export_latest.json`，并可追加

### 4.3 用户上传 CSV -> 转换
- 上传目录：`/home/yhz/nifi-data/inbox_csv`
- 转换输出：`csv_to_json` 或 `csv_to_tsv`（单次只走一个方向）
- 命名：`uploaded_<user>_csv_YYYYMMDD_HHMMSS.<ext>`

### 4.4 用户上传 JSON -> 转换
- 上传目录：`/home/yhz/nifi-data/inbox_json`
- 输入支持：NDJSON / JSON 数组 / 单对象
- 转换输出：`json_to_csv` 或 `json_to_tsv`（单次只走一个方向）
- 命名：`uploaded_<user>_json_YYYYMMDD_HHMMSS.<ext>`

### 4.5 用户上传 TSV -> 转换
- 上传目录：`/home/yhz/nifi-data/inbox_tsv`
- 转换输出：`tsv_to_json` 或 `tsv_to_csv`（单次只走一个方向）
- 命名：`uploaded_<user>_tsv_YYYYMMDD_HHMMSS.<ext>`

### 4.6 打标产物
- 输出目录：`/home/yhz/nifi-data/tagged_output`
- 输出格式：按源文件格式输出（CSV/JSON/TSV）
- 命名：`<source>_tagged_YYYYMMDD_HHMMSS.<ext>`
- 写入要求：原子写（临时文件 + 重命名）

## 5. 实现状态核对（截至 2026-04-07）
- MySQL -> CSV：已实现
- MySQL -> JSON：已实现
- MySQL -> TSV：已实现
- 上传 CSV 单路转换：已实现
- 上传 JSON 单路转换：已实现
- 上传 TSV 单路转换：已实现
- 文件预览 + 表格编辑保存：已实现
- 新增字段后重命名保存：已修复（前端）
- 打标输出目录：已实现为 `tagged_output`（与本文当前决策一致）

## 6. 差异收口清单（当前版本）
### 6.1 P0（优先实施）
1. 前端补 where 条件输入并透传后端导出接口
- 目标：导出时可传 where 条件
- 验收：同一表在不同 where 下导出记录数可区分
- 状态：已完成（2026-04-07）

2. 上传无表头兜底
- 目标：CSV/TSV 无表头时不再静默失败
- 验收：出现明确提示，并允许补列名后再提交
- 状态：已完成（2026-04-07）

### 6.2 暂缓项（按当前决策）
4. 上传链路 latest 追加策略
- 状态：暂不实施
- 当前行为：上传转换链路以时间戳文件为主

6. 样例回归集（自动化）
- 状态：后置
- 启动时机：转 NiFi 前或进入联调稳定期

### 6.3 中期项（P1）
5. 接口契约冻结
- 目标：固定关键字段与错误码，降低前后端/迁移联调成本
- 当前进度：已完成契约草案（本文件 6.4），待代码侧按版本策略执行冻结

7. 最小可观测闭环
- 目标：统一 traceId、输入文件、输出文件关联信息，提升排障效率
- 当前进度：已完成规范草案（本文件 6.5）；第一步“响应字段补齐”、第二步“结构化日志”、第三步“统一中间件封装”已完成（2026-04-07）

### 6.4 接口契约冻结（V1 草案）
#### 6.4.1 通用响应结构（冻结）
- 结构：`{ code, message, data, traceId }`
- 冻结约束：
	- `code`：`0` 表示成功，非 `0` 表示失败
	- `message`：简短可读错误描述
	- `data`：成功时返回业务对象，失败时允许为 `null`
	- `traceId`：始终返回，便于排障

#### 6.4.2 核心接口契约（冻结）
1. `POST /api/v1/export/mysql`
- 请求字段（冻结）：`host, port, user, password, db, table, where, format, append_to_latest`
- 成功响应 `data.file`（冻结）：`fileId, fileName, fileFormat, fileSize, storageType, storagePath, createdAt, jobId`

2. `POST /api/v1/upload/inbox_csv`
- Query（冻结）：`username, convertType, columns`
- `convertType` 可选值（冻结）：`csv_to_json | csv_to_tsv`
- 成功响应 `data`（冻结）：文件元数据对象（同上）

3. `POST /api/v1/upload/inbox_json`
- Query（冻结）：`username, convertType`
- `convertType` 可选值（冻结）：`json_to_csv | json_to_tsv`
- 成功响应 `data`（冻结）：文件元数据对象（同上）

4. `POST /api/v1/upload/inbox_tsv`
- Query（冻结）：`username, convertType, columns`
- `convertType` 可选值（冻结）：`tsv_to_json | tsv_to_csv`
- 成功响应 `data`（冻结）：文件元数据对象（同上）

5. `POST /api/v1/tags/manual-table`
- 请求字段（冻结）：`fileId, operator, changes[], renameColumns[]`
- `changes[]` 元素（冻结）：`rowId, column, value`
- `renameColumns[]` 元素（冻结）：`old, new`
- 成功响应 `data`（冻结）：`updatedCells, renamedColumns, operator, fileId, file`

#### 6.4.3 错误码基线（冻结）
- `1002401`：参数或输入内容非法（如无有效改动、无表头且未提供 columns）
- `1002402`：文件内容解析失败
- `1002404`：fileId 不存在
- `1005001`：MySQL 连接失败

#### 6.4.4 版本策略（冻结）
- 当前契约版本：`contract-v1`
- 兼容性规则：
	- 允许新增可选字段，不允许删除或重命名已冻结字段
	- 若必须破坏兼容，新增 `v2` 接口路径，不在 `v1` 上直接变更
	- 前端仅依赖冻结字段，忽略未知字段

#### 6.4.5 执行与验收
- 执行要求：前后端改动涉及冻结字段时，必须同步更新本文档并注明版本变更
- 验收标准：
	- 核心接口在同一输入下，字段名与字段类型保持稳定
	- 错误码语义不漂移（同类错误返回同类 code）

### 6.5 最小可观测闭环（V1 草案）
#### 6.5.1 目标
- 将一次业务操作在接口层、文件层、错误层统一关联，做到“可查、可定位、可复盘”。

#### 6.5.2 统一观测字段（冻结）
- `traceId`：请求级唯一标识（已有）
- `operation`：操作类型（如 `export_mysql`, `upload_csv_to_json`, `manual_table_edit`）
- `sourcePath`：输入文件路径（无输入文件时可为空）
- `targetPath`：输出文件路径（失败时可为空）
- `status`：`SUCCEEDED | FAILED`
- `errorCode`：失败时返回标准错误码
- `errorMessage`：失败时返回可读错误信息
- `durationMs`：请求处理耗时（毫秒）

#### 6.5.3 覆盖范围（第一阶段）
1. `POST /api/v1/export/mysql`
2. `POST /api/v1/upload/inbox_csv`
3. `POST /api/v1/upload/inbox_json`
4. `POST /api/v1/upload/inbox_tsv`
5. `POST /api/v1/tags/manual-table`

#### 6.5.4 落地规则
- 响应规则：
	- 成功时：`data` 中必须包含 `targetPath`（或 `file.storagePath`）
	- 失败时：必须包含 `traceId + errorCode + errorMessage`
- 日志规则：
	- 每次操作至少打印 2 条结构化日志：开始日志、结束日志
	- 结束日志必须带 `traceId, operation, status, durationMs`
	- 失败结束日志必须带 `errorCode, errorMessage`
- 路径规则：
	- 单输入单输出时直接记录 `sourcePath/targetPath`
	- 单输入多输出时，`targetPath` 记录主产物，附加产物记录到 `outputs[]`

#### 6.5.5 验收标准
- 任意一次失败请求，可通过 `traceId` 在日志中定位完整链路。
- 任意一次成功请求，可从响应中直接拿到产物路径并在文件系统验证存在。
- 5 个覆盖接口全部满足“开始/结束日志 + 耗时 + 状态”最小观测集。

#### 6.5.6 推进顺序
1. 先补响应侧字段规范（低风险）
2. 再补结构化日志（中风险）
3. 最后补统一中间件封装（提高一致性）

#### 6.5.7 实施状态（2026-04-07）
- 已完成：步骤 1（响应侧字段补齐）、步骤 2（结构化日志）、步骤 3（统一中间件封装）
	- 已覆盖接口：
		- `POST /api/v1/export/mysql`
		- `POST /api/v1/upload/inbox_csv`
		- `POST /api/v1/upload/inbox_json`
		- `POST /api/v1/upload/inbox_tsv`
		- `POST /api/v1/tags/manual-table`
	- 已返回字段：`operation, sourcePath, targetPath, status, errorCode, errorMessage, durationMs`
- 待完成：无

## 7. 可迁移结构目标（后续转 NiFi 的前置约束）
1. 稳定契约：导出/上传/打标接口字段保持稳定
2. 编排解耦：业务规则与触发/重试/路由分层
3. 目录配置化：目录通过环境变量注入，不写死路径
4. 可观测：关键步骤全链路可追踪（traceId + source + target）
5. 双模式兼容：支持“本地直跑”与“NiFi 拾取/触发”并存
6. 迁移验收：同一输入在两种模式下关键输出一致

## 8. 全局约束
- 时区：CST（UTC+8）
- 时间戳格式：`YYYYMMDD_HHMMSS`
- 命名要求：来源 + 操作 + 时间戳
- JSON 输出规范：统一 NDJSON（每行一条 JSON）
- 外部对象存储：当前不纳入（Ceph/S3 暂不实施）

## 9. 验收口径（执行时使用）
- 功能验收：每条流程都能生成预期目录和命名的产物
- 数据验收：关键字段、记录数、编码（UTF-8）符合预期
- 稳定性验收：重启后服务可恢复、目录可写、流程可重入
- 迁移验收：本地模式与 NiFi 模式输出一致性可比对

## 10. 10~15 分钟验收勾选表（按顺序执行）
### 10.1 执行前检查（1 分钟）
- [ ] 后端服务为运行态（`iot-backend.service`）
- [ ] 目录存在且可写：`output_* / inbox_* / *_to_* / tagged_output`
- [ ] 前端页面已刷新到最新版本

### 10.2 导出链路回归（3 分钟）
1. MySQL -> CSV（带 where）
- [ ] 执行一次 `where=A` 导出
- [ ] 执行一次 `where=B` 导出
- [ ] 两次结果记录数不同（或可解释地不同）
- [ ] 响应包含：`traceId, operation, targetPath, status`

2. MySQL -> JSON / TSV
- [ ] 各执行 1 次导出
- [ ] 输出文件在目标目录存在且可读

### 10.3 上传转换回归（6 条，6 分钟）
1. CSV -> JSON
- [ ] 产物落 `csv_to_json`

2. CSV -> TSV
- [ ] 产物落 `csv_to_tsv`

3. JSON -> CSV
- [ ] 产物落 `json_to_csv`

4. JSON -> TSV
- [ ] 产物落 `json_to_tsv`

5. TSV -> JSON
- [ ] 产物落 `tsv_to_json`

6. TSV -> CSV
- [ ] 产物落 `tsv_to_csv`

补充校验（无表头兜底）
- [ ] 上传无表头 CSV/TSV 时，前端出现明确提示
- [ ] 填写 `columns` 后可成功提交并产生产物

### 10.4 打标与编辑回归（2 分钟）
- [ ] 预览文件成功
- [ ] 表格修改并保存成功
- [ ] 新增字段重命名后保存成功
- [ ] 打标产物落 `tagged_output`

### 10.5 可观测回归（2 分钟）
- [ ] 日志可检索到同一 `traceId` 的 STARTED 与 SUCCEEDED/FAILED
- [ ] 结束日志包含：`operation, status, durationMs`
- [ ] 失败场景包含：`errorCode, errorMessage`

### 10.6 通过判定（Gate）
- 通过：以上勾选项全部完成，且无阻断性错误
- 有条件通过：存在非阻断问题，但已登记并给出修复时点
- 不通过：任一主链路（导出/上传6条/打标）失败且无临时绕行方案

### 10.7 验收记录模板（执行后填写）
- 执行时间：
- 执行人：
- 结果：通过 / 有条件通过 / 不通过
- 阻断问题：
- 非阻断问题：
- 关联日志关键字（traceId 或 operation）：
- 结论与下一步：

### 10.8 本次回填结果（2026-04-07）
- 执行时间：2026-04-07 10:56（CST）
- 执行人：xy（后端侧自动验收）
- 结果：有条件通过

执行项结果：
- [x] 后端服务运行态检查通过（`iot-backend.service` active/running，监听 `127.0.0.1:8081`）
- [x] 目录存在与可写检查通过（`/home/yhz/nifi-data` 下各业务目录可访问）
- [ ] MySQL 导出链路（CSV/JSON/TSV）未通过
	- 现象：`POST /api/v1/export/mysql` 返回 `code=1005001`
	- 错误：`mysql connect failed: (1049, "Unknown database 'test'")`
	- 影响：无法完成 where 条件分流记录数对比
- [x] 上传 6 条转换链路通过
	- `csv_to_json`、`csv_to_tsv`、`json_to_csv`、`json_to_tsv`、`tsv_to_json`、`tsv_to_csv` 均返回 `code=0`
	- 各自目标目录均有产物文件
- [x] 手工表格编辑/打标产物校验通过
	- `POST /api/v1/tags/manual-table` 返回 `code=0`
	- 产物写入 `tagged_output`
- [x] 可观测日志校验通过
	- 同一 `traceId` 可检索到 `STARTED` 与 `SUCCEEDED/FAILED`
	- 日志含 `operation/status/durationMs`
	- 失败场景含 `errorCode/errorMessage`（导出场景为 `1005001`）

关于“无表头兜底”本轮说明：
- [ ] 未完成前端提示链路验收（本轮仅后端 API 直调）
- 现象：后端直调上传无表头 CSV/TSV 仍可成功转换（返回 `code=0`）
- 结论：前端拦截提示需在页面侧人工验收，不应以后端直调结果替代

阻断问题：
- MySQL 测试库缺失（`test` 不存在）导致导出链路无法验收通过。

非阻断问题：
- 前端“无表头提示”未在本轮后端自动验收覆盖。

关联日志关键字（traceId / operation）：
- `acc-export-CSV`, `acc-export-JSON`, `acc-export-TSV`
- `acc-csv-json`, `acc-json-csv`, `acc-tsv-json`
- `acc-manual-table`
- `operation=export_mysql|upload_csv_to_json|upload_json_to_csv|upload_tsv_to_json|manual_table_edit`

结论与下一步：
- 当前可判定“上传/转换/打标/可观测”链路可用。
- 补齐 MySQL 可用测试库后，复跑导出与 where 分流验收，即可完成全项 Gate 判定。

### 10.9 复验回填结果（2026-04-07，继续执行）
- 执行时间：2026-04-07 10:59（CST）
- 执行人：xy（后端侧自动复验）
- 结果：有条件通过（导出阻断已解除）

复验动作：
- 初始化 MySQL 测试数据：创建 `test.sensor` 并写入 4 条样例记录。
- 复跑导出：`CSV/JSON/TSV` 各 1 次，均返回 `code=0`。
- 复跑 where 分流：
	- `where=id<=1` 导出后数据行数为 1
	- `where=id<=3` 导出后数据行数为 3
	- 结论：where 条件生效，记录数可区分。

关键证据：
- 导出成功 traceId：`acc2-export-CSV`, `acc2-export-JSON`, `acc2-export-TSV`
- where 对比 traceId：`acc2-export-where-id<=1`, `acc2-export-where-id<=3`
- 日志状态：同一 traceId 均存在 `STARTED` + `SUCCEEDED`

状态更新：
- 已关闭阻断项：MySQL 测试库缺失（`test` 不存在）
- 当前遗留：前端“无表头提示链路”仍需页面侧人工验收（后端直调不覆盖）

当前 Gate 结论：
- 后端链路（导出 + 上传6条 + 打标 + 可观测）已通过。
- 全项验收仍为“有条件通过”，待补前端无表头提示的人工验收记录后可转“通过”。

### 10.10 最终验收结论（2026-04-07）
- 执行时间：2026-04-07（CST）
- 执行人：xy + 用户（前端人工验收）
- 最终结果：通过

最终确认项：
- [x] 导出链路通过：`CSV/JSON/TSV` 成功，where 分流记录数可区分。
- [x] 上传转换链路通过：6 条转换均成功并落入对应目录。
- [x] 路径可见性通过：上传转换结果可清晰展示 3 行信息（成功信息、上传文件路径、转换文件路径）。
- [x] 打标链路通过：`manual-table` 保存成功并生成 `tagged_output` 产物。
- [x] 可观测通过：关键接口具备 `traceId + STARTED/SUCCEEDED/FAILED + durationMs`。

基线结论：
- V1 文件导出/上传/转换/打标闭环已达到可演示与可验收状态。
- 以本文当前实现作为验收基线，后续仅做缺陷修复与 NiFi 迁移增量改造。
