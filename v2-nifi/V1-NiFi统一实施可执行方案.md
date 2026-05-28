# V1-NiFi 统一实施可执行方案

> 适用范围：`v1-frontend`、`v1-backend`、`V2执行清单/V1-NiFi统一实施总方案.md`
>
> 目标：在不改动普通用户页面业务流程的前提下，先完成全站统一后端切换入口，再按并发风险优先级把高频功能逐步补齐到 Local / NiFi 双后端路径中。

## 一、必须先满足的实施约束

1. 全站必须只有一个统一的后端切换入口。
2. 切换按钮必须放在普通用户使用的 `index.html` 顶部区域。
3. 内部管理页只保留运维能力，不再放全局切换按钮。
4. 切换按钮只改变同一套前端请求的后端执行路径，不改变页面结构和业务逻辑。
5. 切换状态必须按 `tenant` 级别持久化。
6. 切换行为必须可审计，并受管理员权限控制。
7. Local 与 NiFi 两套后端返回结构必须统一，避免前端出现双份分支逻辑。

## 二、实施原则

### 1. 先统一入口，再补齐双路径功能
先做全局切换，再补齐业务功能的 Local / NiFi 双路径实现。否则每个页面都会出现自己的判断逻辑，后续维护成本会很高。

### 2. 先异步化高并发链路
优先补齐导出、上传转换、定时任务、自动打标等容易形成队列和背压的功能在两条路径上的实现。

### 3. 先统一契约，再改实现
Local 和 NiFi 必须共用同一套响应壳、错误码、文件元数据和任务状态字段。

### 4. 先 tenant 持久化，再做权限控制
切换不是个人偏好，而是工厂级运行模式，需要可追踪、可回滚、可审计。

## 三、分阶段执行

### P0：先把全站切换打通
- 在 `index.html` 顶部加入 Local / NiFi 切换按钮
- 在前端封装统一 API adapter，所有普通用户页面请求必须走同一入口
- 切换结果写入 tenant 级配置，并在后端持久化
- 提供当前模式查询接口与保存接口
- 加入切换审计记录
- 默认切换结果必须影响后续页面请求的路由选择

### P1：优先补齐高并发功能的双路径实现
- 数据库导出
- 用户上传自动转换
- 定时导出
- 自动打标 / 手动保存生成结果

### P2：优化文件与任务链路
- 目录树扫描
- 文件中心刷新同步
- 任务状态回调
- 失败重试与幂等控制
- 回滚与故障演练

## 四、功能双路径补齐优先级

### 1. 数据库导出
并发风险最高，适合第一批补齐 Local / NiFi 双路径实现。

原因：
- 用户同时导出时容易形成请求洪峰
- 导出任务常常耗时较长
- 适合队列化、异步化、限流、重试

### 2. 用户上传自动转换
适合流式处理。

原因：
- 上传量大时容易出现瞬时并发
- 文件解析和格式转换会占用资源
- NiFi 能更好地处理流控与背压

### 3. 定时导出
适合批处理和调度编排。

原因：
- 多工厂任务会集中触发
- 需要统一的调度、状态管理和失败重试

### 4. 自动打标 / 手动保存结果
适合放入可编排的后端流程。

原因：
- 单次不一定很重，但批量积压时会成为瓶颈
- 需要统一生成 tagged_output 结果文件

### 5. 文件中心目录扫描
适合做增量扫描和批处理。

原因：
- 文件量增大后，前端或后端直接扫全目录会越来越慢
- 适合转成后端索引更新流程

### 6. 任务状态与回调
适合异步任务链路。

原因：
- 任务多时，状态更新频繁
- 适合统一落库、轮询、回调、重试

## 五、接口映射建议

### 全局切换
- `GET/POST /api/v1/internal/backend-mode`
- 作用：读取和保存当前 tenant 的后端模式

### 任务链路
- `/jobs`
- `/jobs/{id}`
- `/jobs/{id}/outputs`

### 文件中心
- `/files`
- `/files/{id}/preview`
- `/files/{id}/download`

### 导出
- `/export`
- `/export-jobs`

### 上传转换
- `/upload/*`

### 打标
- `/tags/auto`
- `/tags/manual-table`

要求：
- 不同后端必须返回一致的字段
- 不同页面必须走统一 adapter
- 不允许每个页面单独拼接后端地址

## 六、存储落地约定

1. NiFi 后端相关文件统一落到 `/home/yhz/real_nifi_data`。
2. Local 后端相关文件统一落到 `/home/yhz/nifi-data`。
3. 前端不得直接依赖物理路径，只能识别逻辑目录与统一接口返回。
4. 任何新增的 NiFi 输出、转换、打标、导出结果，都应优先写入 `/home/yhz/real_nifi_data` 下的对应子目录。
5. 若后续需要目录扩展，必须保持与 `/home/yhz/nifi-data` 的逻辑目录映射一致。

## 七、主要风险

1. 接口散落
   - 如果页面各自拼接 API，切换按钮会失效。

2. 同步阻塞
   - 大文件导出或转换会卡住普通用户页面。

3. 状态不一致
   - Local/NiFi 返回结构不同会导致前端维护成本暴涨。

4. 无审计
   - 无法回答谁在什么时候切到了哪个后端。

5. 无回滚
   - NiFi 异常时无法快速退回 Local。

## 七、推荐实施顺序

1. 先在 `index.html` 上做全局切换按钮。
2. 再把前端请求收敛到统一 API adapter。
3. 然后实现后端 NiFi 一键接入能力：前端操作保持不变，后端只对接运维预置好的容器与已运行的处理器，不在运行时自动部署新处理器。
4. 再补齐数据库导出、上传转换、定时导出、自动打标四条高并发链路的 Local / NiFi 双路径实现。
5. 再补目录扫描、任务回调、审计、回滚与演练。
6. 最后再考虑更复杂的数据接入和扩展场景。

## 七点五、NiFi 后端一键接入方案

本项目目标采用“NiFi 容器与 Flow 由后端麻烦地预先配置好，前端保持和 Local 一样一键触发”的方式实现 `backend-nifi`：用户在 `nifi` 模式下点击数据库导出时，前端不感知 NiFi 细节；后端只确认 NiFi 容器和处理器已就绪，然后投递任务到共享目录，由 NiFi 自动消费并完成导出。

这种模式适合阶段一和生产初版：前端体验和 Local 一样是一键完成，两个处理器始终保持 RUNNING 状态，后端只需写入任务 JSON 并轮询结果，复杂度最低。

### 7.5.1 基本原则

1. 预置优先：`backend_mode=nifi` 时，NiFi 容器和处理器由运维/脚本提前准备好并保持运行。
2. 一键优先：前端点击导出后，体验应和 Local 一样，不要求用户手工进入 NiFi 页面。
3. 后端只做接入：后端负责检查 NiFi 是否可用、确认处理器已就绪、投递任务、同步结果、记录审计。
4. 不自动部署：后端不创建新处理器，不在运行时做复杂编排。
5. 单机 NiFi MVP 优先：先跑通 Docker 单机 NiFi 和 2 个固定处理器，再考虑集群、HA。
6. Local 与 NiFi 共享逻辑目录，但物理目录必须隔离。
7. NiFi 后端统一使用 `/home/yhz/real_nifi_data` 作为宿主机数据根目录。
8. 若 NiFi 不可用，明确返回错误或按配置回退 Local，不做静默自动修复。

### 7.5.2 NiFi 容器与目录前置准备

需要先在宿主机创建以下目录，并由运维或脚本提前启动 NiFi 容器及预置处理器：

```bash
mkdir -p /home/yhz/real_nifi_data/export_jobs/inbox
mkdir -p /home/yhz/real_nifi_data/export_jobs/done
mkdir -p /home/yhz/real_nifi_data/export_jobs/error
mkdir -p /home/yhz/real_nifi_data/output_csv
mkdir -p /home/yhz/real_nifi_data/output_json
mkdir -p /home/yhz/real_nifi_data/output_tsv
mkdir -p /home/yhz/real_nifi_data/inbox_csv
mkdir -p /home/yhz/real_nifi_data/inbox_json
mkdir -p /home/yhz/real_nifi_data/inbox_tsv
mkdir -p /home/yhz/real_nifi_data/tagged_output
mkdir -p /home/yhz/real_nifi_data/csv_to_json
mkdir -p /home/yhz/real_nifi_data/csv_to_tsv
mkdir -p /home/yhz/real_nifi_data/json_to_csv
mkdir -p /home/yhz/real_nifi_data/json_to_tsv
mkdir -p /home/yhz/real_nifi_data/tsv_to_csv
mkdir -p /home/yhz/real_nifi_data/tsv_to_json
```

### 7.5.3 后端只做 NiFi 就绪检查与 Flow 控制

当前阶段推荐优先使用 Docker 单机 NiFi，但容器由运维或脚本预置启动，后端不负责拉起容器。

后端在 `backend_mode=nifi` 且用户点击数据库导出时，应执行以下就绪检查逻辑：

1. 检查 NiFi 容器或服务是否已经在运行。
2. 检查 NiFi API 是否可访问。
3. 检查目标处理器（GetFile + ExecuteStreamCommand）是否已存在且处于 RUNNING 状态。
4. 若未就绪则返回明确错误，或按配置回退 Local，并记录审计。

默认策略应是“只做后端对接，不在运行时自动创建容器或自动部署处理器”；如确需演示或联调，可通过显式环境变量单独开启自动化能力，并保持审计可追踪。

运维或脚本预置容器时的等价命令如下：

```bash
docker run -d \
  --name iot-nifi \
  -p 8080:8080 \
   -v /home/yhz/real_nifi_data:/opt/nifi/nifi-current/data/iot \
  apache/nifi:latest
```

后端实现时不应执行上述命令，而是把它作为运维预置参考。

后续正式化时，应改为 `docker-compose.yml` 或 Docker SDK 管理，并补充：

- NiFi 数据库/Flow 仓库持久化 volume
- Python pymysql 依赖预装（Worker 脚本需要，见 7.5.4）
- 用户认证配置
- 时区配置
- 日志目录挂载
- NiFi Registry（可选）

### 7.5.3.1 后端 NiFi 接入模块建议

建议新增后端模块：`nifi_orchestrator`，职责包括：

- `check_nifi_ready()`：检查 NiFi API 是否可访问，确认容器/服务已预置。
- `ensure_export_flow()`：确认 GetFile + ExecuteStreamCommand 两个处理器已存在且 RUNNING，pymysql 已安装，Worker 脚本就位。
- `submit_export_job()`：写入导出任务 JSON 到 `export_jobs/inbox`（密码来自前端，原样传递）。
- `sync_export_result()`：扫描 `done/error` 并注册文件资源。

后端导出接口不得直接散落 NiFi HTTP 调用，必须通过该接入模块统一封装。

### 7.5.4 Python Worker 与 pymysql（替代 JDBC + DBCP）

> 本方案不使用 JDBC / DBCPConnectionPool / ExecuteSQLRecord。
> 原因：DBCP 的密码需要在 NiFi Controller Service 中**静态配置**，无法满足"前端用户填写不同数据库凭据，每次导出各用各的密码"这一核心需求。

数据库连接由 **Python Worker 脚本**直接从任务 JSON 读取动态参数（dbType / host / port / user / password / database），根据 `dbType` 字段自动选择对应驱动（pymysql、psycopg2、pymssql、oracledb、sqlite3、pyhive、hdfs、happybase）连接。

密码传递链路：**前端表单 → 后端任务 JSON → NiFi GetFile → ExecuteStreamCommand stdin → Worker → 驱动 connect()**。每一步都不对密码做加工，前端填什么，目标数据库就收到什么。

最小要求：

1. NiFi 容器中安装所需数据库驱动：
   ```bash
   docker exec iot-nifi pip3 install pymysql psycopg2-binary pymssql
   # 可选：oracledb pyhive pyarrow hdfs happybase
   ```
2. Worker 脚本部署到共享目录：
   ```bash
   cp v1-backend/scripts/nifi_db_export_worker.py \
      /home/yhz/iot/real_nifi_data/bin/nifi_db_export_worker.py
   ```
3. 容器内可见：`/opt/nifi/nifi-current/data/iot/bin/nifi_db_export_worker.py`
4. Worker 从 stdin 读取任务 JSON，根据 `dbType` 选择驱动，`password` 字段直接传给驱动的 connect 函数。
5. 无需配置任何 NiFi Controller Service（0 个 CS）。

详见 `02-db-export-flow.md` 第三章"完整数据流"。

### 7.5.5 数据库导出 Flow MVP（仅 2 个处理器，8 种数据源通用）

```
GetFile (Root Group) ──success──┐
                                │
                                ▼
                 ExecuteStreamCommand (Root Group)
                         │
                         │ 调用: python3 /opt/nifi/.../bin/nifi_db_export_worker.py
                         │ 传参: stdin ← 任务 JSON（含 dbType / host/port/user/password/database/table）
                         │
                         ▼
              nifi_db_export_worker.py（通用 Worker）
              ├── json.load(sys.stdin) → 动态凭据
              ├── 根据 dbType 选择驱动（mysql→pymysql, postgres→psycopg2, ...）
              ├── output_{format}/{jobId}_{timestamp}.{csv|json|tsv}
              └── export_jobs/done|error/{jobId}.json
```

**2 个处理器，0 个 Controller Service，无需任何 JDBC jar。支持全部 8 种数据源：MySQL、PostgreSQL、SQLServer、Oracle、SQLite、Hive、HDFS、HBase。**

1. `GetFile`：监听 `/opt/nifi/nifi-current/data/iot/export_jobs/inbox`，匹配 `*.json`，消费后删除。
2. `ExecuteStreamCommand`：通过 stdin 将任务 JSON 全文传给 Python Worker，Worker 负责解析、驱动选择、连接、查询（或 HDFS/HBase 读取）、格式转换、写入结果和状态文件。

创建方式：通过 NiFi REST API 一键部署（Python 脚本），详见 `02-db-export-flow.md` 第五章。

### 7.5.5.1 Flow 预置与复用要求

后端不负责自动部署 Flow，而是按以下规则处理：

1. NiFi 侧提前在 Root Group 准备好 `GetFile` + `ExecuteStreamCommand` 两个处理器（通过 `02-db-export-flow.md` 第五章的 Python 一键部署脚本创建）。
2. 后端通过 NiFi API 检查两个处理器是否存在且处于 RUNNING 状态。
3. 后端只需确认 Worker 脚本部署到位、所需驱动已安装、inbox 目录存在即可。
4. 若不存在或不可用，则返回明确错误或回退 Local。
5. 部署和版本管理由运维、脚本或单独发布流程完成。
6. 处理器标识：
   - GetFile 名称：`iot_db_export_getfile_v1`
   - ExecuteStreamCommand 名称：`iot_db_export_command_v1`
7. 所有数据源、手动导出、定时导出任务复用同一组处理器，只通过 `export_jobs/inbox` 投递不同的任务 JSON。

### 7.5.6 后端接入执行顺序

用户在 `backend_mode=nifi` 下点击数据库导出时，后端必须按以下顺序执行：

1. 读取当前 tenant 的 `backend_mode`，确认是 `nifi`。
2. 调用 `check_nifi_ready()`，确认 NiFi API 可访问。
3. 调用 `ensure_export_flow()`，确认 GetFile + ExecuteStreamCommand 两个处理器已存在且 Worker 脚本就位。
4. 生成数据库导出任务 JSON（密码来自前端，原样写入）。
5. 写入 `/home/yhz/real_nifi_data/export_jobs/inbox/{jobId}.json`。
6. 返回前端 `PENDING` / `已提交到 NiFi` 状态。
7. 后端扫描 `done/error`，或由定时任务同步结果。
8. 若 NiFi 成功输出文件，则注册 fileId，并供前端预览、下载、追踪。

> 注：新方案中两个处理器始终处于 RUNNING 状态，无需 `start_existing_flow()` / `stop_existing_flow()` 步骤。

### 7.5.6.1 接入失败处理

后端接入失败时，必须按统一策略处理：

1. Docker/服务不可用：返回明确错误或按配置回退 Local。
2. NiFi API 不可访问：记录审计，并返回 NiFi 不可用。
3. 目标处理器不存在或不可用：记录审计，并返回 Flow 不可用。
4. 任务 JSON 写入失败：返回任务提交失败。
5. 任何回退 Local 的行为都必须写入审计，不允许静默回退。

### 7.5.6.2 与后端联调顺序

1. 先由运维或脚本确保 NiFi 容器已启动。
2. 后端能检测 NiFi API 是否可访问。
3. 后端能检查 GetFile + ExecuteStreamCommand 两个处理器是否存在且 RUNNING。
4. 后端能写入导出任务 JSON。
5. NiFi 能消费任务并生成 CSV/JSON/TSV。
6. NiFi 能写入 `done/error` 状态文件。
7. 后端能扫描结果并注册 fileId。

### 7.5.7 数据库导出点击行为要求

当用户点击"数据库导出"时：

- `backend_mode=local`：后端本地执行导出。
- `backend_mode=nifi`：后端必须先确认 NiFi 容器/服务可用、GetFile + ExecuteStreamCommand 两个处理器已预置且 RUNNING、Worker 脚本就位，然后提交 NiFi 任务，由 Worker 完成 SQL 查询、格式转换与文件落盘。
- NiFi 可用且任务提交成功时，不允许后端本地直接生成导出文件。
- NiFi 不可用、处理器不存在或不可用、任务提交失败时，才允许根据降级策略回退 Local，并必须记录审计。
- 前端不需要知道容器启动、Worker 脚本等细节，只接收统一响应：`PENDING`、`SUCCEEDED`、`FAILED` 或明确错误。

### 7.5.8 上传自动转换 NiFi 方案

> 详细配置步骤见 `03-upload-convert-flow.md`

与数据库导出同模式——前端上传文件后，后端将源文件保存到 `inbox_csv|inbox_json|inbox_tsv`，并投递转换任务 JSON，NiFi 消费后调用 Worker 完成格式转换。

**处理器数量**：3（ListFile + FetchFile + ExecuteStreamCommand），0 个 Controller Service。

**ListFile**：持续扫描 `/opt/nifi/nifi-current/data/iot/convert_jobs/inbox/*.json`，`schedulingPeriod: 5 sec`。

**FetchFile**：读取 ListFile 发现的文件内容（即任务 JSON），传给下游。

**ExecuteStreamCommand**：调用 `python3 /opt/nifi/nifi-current/data/iot/bin/nifi_upload_convert_worker.py`，通过 stdin 传入任务 JSON。

**Worker 职责**：
- 解析任务 JSON（sourcePath / sourceFormat / targetFormats）
- 读取源文件（CSV / NDJSON / JSON 数组 / JSON 单对象 / TSV 自动检测）
- 转换为目标格式：CSV↔JSON↔TSV 六向转换
- 原子写输出到 `csv_to_json/`, `csv_to_tsv/`, `json_to_csv/`, `json_to_tsv/`, `tsv_to_csv/`, `tsv_to_json/`
- 写状态文件到 `convert_jobs/done|error/{jobId}.json`

**前提依赖**：
- Worker 脚本：`v1-backend/scripts/nifi_upload_convert_worker.py` 部署到 `real_nifi_data/bin/`
- 目录创建：`convert_jobs/`, `*_to_*` 六个转换输出目录
- 无外部 pip 依赖（纯标准库 csv / json）

**创建方式**：Python API 一键部署脚本，详见 `03-upload-convert-flow.md` 第四章。

**处理器标识建议**：
- ListFile: `iot_upload_convert_listfile_v1`
- FetchFile: `iot_upload_convert_fetchfile_v1`
- ExecuteStreamCommand: `iot_upload_convert_command_v1`

**后端接入要点**：
1. `nifi_orchestrator` 新增 `ensure_convert_flow()`：检查 ListFile + FetchFile + ExecuteStreamCommand 三级处理器是否 RUNNING。
2. 上传接口收到文件后，后端保存源文件到对应 `inbox_*` 目录。
3. 构建转换任务 JSON → 写入 `convert_jobs/inbox/{jobId}.json`。
4. 扫描 `convert_jobs/done|error` 同步结果。

**任务 JSON 结构**：
```json
{
  "jobId": "convert_abc123",
  "sourcePath": "/opt/nifi/nifi-current/data/iot/inbox_csv/uploaded_user_csv_20260528_120000.csv",
  "sourceFormat": "CSV",
  "targetFormats": ["JSON", "TSV"],
  "fileName": "uploaded_user_csv_20260528_120000",
  "ownerId": "user-001",
  "factoryId": "factory-001"
}
```

### 7.5.9 自动打标 NiFi 方案

> 详细配置步骤见 `04-auto-tagging-flow.md`

前端选择文件 + 配置打标规则后，后端投递打标任务 JSON，NiFi 消费后调用 Worker 应用标签并输出到 `tagged_output`。

**处理器数量**：2（GetFile + ExecuteStreamCommand），0 个 Controller Service。与数据库导出完全一致的架构。

**GetFile**：监听 `/opt/nifi/nifi-current/data/iot/tagging_jobs/inbox/*.json`，消费后删除。

**ExecuteStreamCommand**：调用 `python3 /opt/nifi/nifi-current/data/iot/bin/nifi_auto_tagging_worker.py`，通过 stdin 传入任务 JSON。

**Worker 职责**：
- 解析任务 JSON（sourcePath / tagType / tagConfig / targetFormat）
- 读取源文件（CSV / JSON / TSV）
- 根据 tagType 应用打标规则：
  - `manual-table`：逐列映射 `值→标签`
  - `auto-rule`：预定义规则引擎（正则 / 范围 / 条件表达式）
  - `ai-suggestion`：AI 服务推荐（预留接口）
- 原子写输出到 `tagged_output/<source>_tagged_YYYYMMDD_HHMMSS.<ext>`
- 写状态文件到 `tagging_jobs/done|error/{jobId}.json`

**前提依赖**：
- Worker 脚本：`v1-backend/scripts/nifi_auto_tagging_worker.py` 部署到 `real_nifi_data/bin/`
- 目录创建：`tagging_jobs/`, `tagged_output/`
- 无外部 pip 依赖（纯标准库 csv / json）

**创建方式**：Python API 一键部署脚本，详见 `04-auto-tagging-flow.md` 第四章。

**处理器标识建议**：
- GetFile: `iot_auto_tagging_getfile_v1`
- ExecuteStreamCommand: `iot_auto_tagging_command_v1`

**后端接入要点**：
1. `nifi_orchestrator` 新增 `ensure_tagging_flow()`：检查 GetFile + ExecuteStreamCommand 处理器是否 RUNNING。
2. 打标接口收到请求后，构建任务 JSON → 写入 `tagging_jobs/inbox/{jobId}.json`。
3. 扫描 `tagging_jobs/done|error` 同步结果。

**任务 JSON 结构**：
```json
{
  "jobId": "tag_abc123",
  "sourcePath": "/opt/nifi/nifi-current/data/iot/output_csv/sensor_data.csv",
  "sourceFormat": "CSV",
  "tagType": "manual-table",
  "tagConfig": {
    "columns": ["status"],
    "mappings": {
      "row_rules": [{ "column": "status", "mapping": {"0": "正常", "1": "告警"}}]
    }
  },
  "targetFormat": "CSV",
  "fileName": "sensor_data_export_20260528",
  "factoryId": "factory-001",
  "ownerId": "admin"
}
```

### 7.5.10 其余功能说明

以下功能不需要独立的 NiFi Flow，由后端直接处理或复用已有 Flow：

#### 7.5.10.1 定时导出

定时导出**不创建单独的 NiFi Flow**，而是复用 02 数据库导出 Flow（GetFile + ExecuteStreamCommand + MySQL Export Worker）。

**实现方式**：
- 后端通过 cron / APScheduler 按调度策略触发
- 到了调度时间点，后端生成与数据库导出完全一致的任务 JSON
- 写入同一个 `export_jobs/inbox` 目录
- NiFi 现有的 GetFile + ExecuteStreamCommand 处理器自动消费，Worker 正常执行
- 输出仍落 `output_csv|output_json|output_tsv`
- 命名加 `_scheduled` 前缀区分手动导出

**后端新增逻辑**：
- `schedule_export_job()`：按工厂调度配置定时生成任务 JSON 并投递
- 调度配置持久化到数据库（工厂级 `export_schedule` 字段）

#### 7.5.10.2 文件中心目录扫描

**不需要 NiFi 承接**。这是一个纯后端操作：

- 后端直接扫描 `/home/yhz/real_nifi_data` 和 `/home/yhz/nifi-data` 的目录树
- 建立文件索引（fileId, fileName, fileFormat, fileSize, storagePath, createdAt）
- 提供 `/api/v1/files` 列表接口
- 前端通过统一 adapter 获取文件列表

NiFi 模式下文件索引更新与 Local 模式完全一致，无额外 NiFi 配置。

#### 7.5.10.3 任务状态与回调

**不需要 NiFi 承接**。后端轮询机制统一处理：

- 所有 NiFi Flow（02/03/04）的 Worker 均写入 `done|error/{jobId}.json` 状态文件
- 后端 `nifi_orchestrator.sync_*_result()` 定期扫描各 `done/error` 目录
- 检测到新状态文件后：解析内容 → 注册 fileId → 更新任务状态 → 通知前端
- 轮询间隔建议 5 秒（与 ListFile schedulingPeriod 一致）

---

### 7.5.11 所有 Flow 全景图

```
┌──────────────────────────────────────────────────────────────────────┐
│                    NiFi Root Group                                     │
│                                                                        │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │ 02 数据库导出 + 定时导出复用（8 种数据源通用）                     │ │
│  │ GetFile ──→ ExecuteStreamCommand ──→ nifi_db_export_worker       │ │
│  │ inbox: export_jobs/inbox  │  输出: output_csv|output_json|output_tsv│
│  │ 处理器: iot_db_export_getfile_v1, iot_db_export_command_v1        │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                                                                        │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │ 03 上传自动转换                                                    │ │
│  │ ListFile → FetchFile → ExecuteStreamCommand → upload_convert_worker│
│  │ inbox: convert_jobs/inbox  │  输出: *_to_* (6个子目录)             │ │
│  │ 处理器: iot_upload_convert_listfile_v1, ...fetchfile_v1, ...cmd_v1│ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                                                                        │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │ 04 自动打标                                                        │ │
│  │ GetFile ──→ ExecuteStreamCommand ──→ nifi_auto_tagging_worker    │ │
│  │ inbox: tagging_jobs/inbox  │  输出: tagged_output                 │ │
│  │ 处理器: iot_auto_tagging_getfile_v1, iot_auto_tagging_command_v1 │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                                                                        │
│  ┌──────────────────────────────────────────────────────────────────┐│
│  │ 后端直接处理（无需 NiFi Flow）                                      ││
│  │ · 文件中心目录扫描 → 后端扫描 real_nifi_data + nifi-data           ││
│  │ · 任务状态回调   → 后端轮询各 Flow 的 done/error 目录              ││
│  │ · 定时导出       → 后端 cron 触发，复用 02 Flow 的 inbox           ││
│  └──────────────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────────┘
```

## 八、验收标准

- `index.html` 顶部能看到全局切换按钮
- 切换后所有普通用户页面请求会走对应后端
- tenant 级切换状态可持久化
- 切换行为可审计
- Local 和 NiFi 返回结构一致
- 数据库导出、上传转换、定时导出、自动打标能按模式正确路由
- 当 `backend_mode=nifi` 且 NiFi 容器未运行时，后端应返回明确的不可用错误或按部署配置回退到 `local`，不得默认自动创建新容器
- 当数据库导出处理器已存在时，后端不得重复创建，必须复用并确保其处于 RUNNING 状态
- 点击数据库导出后，任务 JSON 能自动进入 `/home/yhz/real_nifi_data/export_jobs/inbox`
- NiFi 能自动消费任务并将结果写入 `/home/yhz/real_nifi_data/output_csv|output_json|output_tsv`
- NiFi 能写入 `/home/yhz/real_nifi_data/export_jobs/done|error` 状态文件
- 后端能同步 NiFi 状态文件并注册 fileId
- 启动/停止既有处理器、任务提交、失败回退等关键操作必须记录审计日志

## 九、与现有文档的关系

本文件是实施版，配合以下文档使用：

**总方案**：
- `V2执行清单/V1-NiFi统一实施总方案.md` — 原则和边界

**NiFi Flow 详细配置（v2-nifi/ 目录）**：
| 文档 | 功能 | 处理器数量 |
|------|------|-----------|
| `01-cleanup-old-flow.md` | 清理旧方案遗留 | — |
| `02-db-export-flow.md` | 数据库导出（8 种数据源通用 + 定时导出复用） | 2（GetFile + ExecuteStreamCommand） |
| `03-upload-convert-flow.md` | 上传自动转换 | 3（ListFile + FetchFile + ExecuteStreamCommand） |
| `04-auto-tagging-flow.md` | 自动打标 | 2（GetFile + ExecuteStreamCommand） |
| 本文件 | 统一实施可执行方案（交叉索引总控） | — |

**后端对接**：
- `v1-backend/app/nifi_orchestrator.py` — NiFi 接入模块
- `v1-backend/scripts/nifi_db_export_worker.py` — 数据库导出 Worker（8 种数据源通用）
- `v1-backend/scripts/nifi_upload_convert_worker.py` — 上传转换 Worker（待创建）
- `v1-backend/scripts/nifi_auto_tagging_worker.py` — 自动打标 Worker（待创建）

使用方式：
- 总方案负责原则和边界
- 各 Flow md 负责对应功能的 NiFi 配置细化步骤
- 本文件负责落地顺序、交叉索引和验收


