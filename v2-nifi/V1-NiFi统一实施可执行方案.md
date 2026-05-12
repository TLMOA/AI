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

1. NiFi 后端相关文件统一落到 `/home/yhz/iot/real_nifi_data`。
2. Local 后端文件仍按本地实现目录管理，但对前端暴露时必须走统一逻辑目录。
3. 前端不得直接依赖物理路径，只能识别逻辑目录与统一接口返回。
4. 任何新增的 NiFi 输出、转换、打标、导出结果，都应优先写入 `/home/yhz/iot/real_nifi_data` 下的对应子目录。
5. 若后续需要目录扩展，必须保持与 `nifi_data` 的逻辑目录映射一致。

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
3. 然后实现后端 NiFi 一键接入能力：前端操作保持不变，后端只对接运维预置好的容器与 Flow，按任务需要启动/关闭既有 Flow，不在运行时自动部署新 Flow。
4. 再补齐数据库导出、上传转换、定时导出、自动打标四条高并发链路的 Local / NiFi 双路径实现。
5. 再补目录扫描、任务回调、审计、回滚与演练。
6. 最后再考虑更复杂的数据接入和扩展场景。

## 七点五、NiFi 后端一键接入方案

本项目目标采用“NiFi 容器与 Flow 由后端麻烦地预先配置好，前端保持和 Local 一样一键触发”的方式实现 `backend-nifi`：用户在 `nifi` 模式下点击数据库导出时，前端不感知 NiFi 细节；后端只接入已存在的 NiFi 容器与已建好的 Flow，在一次导出任务中负责启动既有 Flow、投递任务、等待完成、回收结果，任务结束后可按需要关闭该 Flow。

这种模式适合阶段一和生产初版：前端体验和 Local 一样是一键完成，后端复杂度被限制在预置配置与任务调度上，最容易先把主链路跑通。

### 7.5.1 基本原则

1. 预置优先：`backend_mode=nifi` 时，NiFi 容器和目标 Flow 由运维/脚本提前准备好。
2. 一键优先：前端点击导出后，体验应和 Local 一样，不要求用户手工进入 NiFi 页面。
3. 后端只做接入：后端负责检查 NiFi 是否可用、启动既有 Flow、提交任务、同步结果、记录审计。
4. 不自动部署：后端不创建新 Flow，不自动拉起容器，不在运行时做复杂编排。
5. 单机 NiFi MVP 优先：先跑通 Docker 单机 NiFi 和固定 Flow，再考虑 Registry、集群、HA。
6. Local 与 NiFi 共享逻辑目录，但物理目录必须隔离。
7. NiFi 后端统一使用 `/home/yhz/iot/real_nifi_data` 作为宿主机数据根目录。
8. 若 NiFi 不可用，明确返回错误或按配置回退 Local，不做静默自动修复。

### 7.5.2 NiFi 容器与目录前置准备

需要先在宿主机创建以下目录，并由运维或脚本提前启动 NiFi 容器及固定 Flow：

```bash
mkdir -p /home/yhz/iot/real_nifi_data/export_jobs/inbox
mkdir -p /home/yhz/iot/real_nifi_data/export_jobs/done
mkdir -p /home/yhz/iot/real_nifi_data/export_jobs/error
mkdir -p /home/yhz/iot/real_nifi_data/output_csv
mkdir -p /home/yhz/iot/real_nifi_data/output_json
mkdir -p /home/yhz/iot/real_nifi_data/output_tsv
mkdir -p /home/yhz/iot/real_nifi_data/inbox_csv
mkdir -p /home/yhz/iot/real_nifi_data/inbox_json
mkdir -p /home/yhz/iot/real_nifi_data/inbox_tsv
mkdir -p /home/yhz/iot/real_nifi_data/tagged_output
```

### 7.5.3 后端只做 NiFi 就绪检查与 Flow 控制

当前阶段推荐优先使用 Docker 单机 NiFi，但容器由运维或脚本预置启动，后端不负责拉起容器。

后端在 `backend_mode=nifi` 且用户点击数据库导出时，应执行以下就绪检查逻辑：

1. 检查 NiFi 容器或服务是否已经在运行。
2. 检查 NiFi API 是否可访问。
3. 检查目标 Flow 是否已存在且处于可用状态。
4. 需要时启动既有 Flow（启用/停止 Flow 的生命周期由后端控制，但不创建新 Flow）。
5. 任务执行完成后按需要关闭既有 Flow。
6. 若未就绪则返回明确错误，或按配置回退 Local，并记录审计。

默认策略应是“只做后端对接，不在运行时自动创建容器或自动部署 Flow”；如确需演示或联调，可通过显式环境变量单独开启自动化能力，并保持审计可追踪。

运维或脚本预置容器时的等价命令如下：

```bash
docker run -d \
  --name iot-nifi \
  -p 8080:8080 \
  -v /home/yhz/iot/real_nifi_data:/opt/nifi/nifi-current/data/iot \
  apache/nifi:latest
```

后端实现时不应执行上述命令，而是把它作为运维预置参考。

后续正式化时，应改为 `docker-compose.yml` 或 Docker SDK 管理，并补充：

- NiFi 数据库/Flow 仓库持久化 volume
- MySQL JDBC 驱动挂载
- 用户认证配置
- 时区配置
- 日志目录挂载
- NiFi Registry（可选）

### 7.5.3.1 后端 NiFi 接入模块建议

建议新增后端模块：`nifi_orchestrator`，职责包括：

- `check_nifi_ready()`：检查 NiFi API 是否可访问，确认容器/服务已预置。
- `start_existing_flow()`：启用既有数据库导出 Flow，开始处理任务。
- `stop_existing_flow()`：在任务完成后关闭既有 Flow。
- `ensure_export_flow()`：确认数据库导出 Flow 已存在且可用。
- `submit_export_job()`：写入导出任务 JSON 到 `export_jobs/inbox`。
- `sync_export_result()`：扫描 `done/error` 并注册文件资源。

后端导出接口不得直接散落 NiFi HTTP 调用，必须通过该接入模块统一封装。

### 7.5.4 MySQL JDBC 驱动

数据库导出 Flow 需要 NiFi 能连接 MySQL，因此必须准备 MySQL Connector/J，并在 NiFi 中配置 `DBCPConnectionPool`。

最小要求：

1. 将 MySQL JDBC jar 挂载到 NiFi 容器可访问目录。
2. 在 NiFi Controller Services 中新增 `DBCPConnectionPool`。
3. 配置 JDBC URL、驱动类、用户名、密码。
4. 在 `ExecuteSQL` 或 `ExecuteSQLRecord` 中引用该连接池。

### 7.5.5 数据库导出 Flow MVP

数据库导出最小 Flow 顺序：

1. `GetFile` 或 `ListFile + FetchFile` 监听 `/opt/nifi/nifi-current/data/iot/export_jobs/inbox`。
2. `EvaluateJsonPath` 解析任务 JSON。
3. `UpdateAttribute` 提取 `jobId、factoryId、host、port、database、table、where、format、targetDir`。
4. `ExecuteSQL` 或 `ExecuteSQLRecord` 执行 MySQL 查询。
5. `ConvertRecord` 或脚本处理器转换为 CSV / JSON / TSV。
6. `PutFile` 写入 `/opt/nifi/nifi-current/data/iot/output_csv|output_json|output_tsv`。
7. 成功后写 `/opt/nifi/nifi-current/data/iot/export_jobs/done/{jobId}.json`。
8. 失败后写 `/opt/nifi/nifi-current/data/iot/export_jobs/error/{jobId}.json`。

### 7.5.5.1 Flow 预置与复用要求

后端不负责自动部署 Flow，而是按以下规则处理：

1. NiFi 侧提前准备好数据库导出 Process Group。
2. 后端通过 NiFi API 检查该 Process Group 是否存在且可用。
3. 后端在执行任务前启用这个既有 Flow，任务完成后按需关闭。
4. 若已存在但不可用，则返回明确错误或回退 Local。
5. 部署和版本管理由运维、脚本或单独发布流程完成。
6. 建议记录 Flow 标识，例如 `processGroupId`、`flowName`、`version`。
7. 后续数据库导出任务复用同一个 Flow，只通过 `export_jobs/inbox` 投递不同任务 JSON。

Flow 标识建议：

- Flow 名称：`iot_mysql_export_flow_v1`
- Process Group 名称：`iot-mysql-export`
- 监听目录：`/opt/nifi/nifi-current/data/iot/export_jobs/inbox`
- 输出根目录：`/opt/nifi/nifi-current/data/iot`

### 7.5.6 后端半自动接入执行顺序

用户在 `backend_mode=nifi` 下点击数据库导出时，后端必须按以下顺序执行：

1. 读取当前 tenant 的 `backend_mode`，确认是 `nifi`。
2. 调用 `check_nifi_ready()`，确认 NiFi API 可访问且目标 Flow 已预置。
3. 调用 `ensure_export_flow()`，确认数据库导出 Flow 已存在且可用。
4. 调用 `start_existing_flow()`，启用该 Flow 开始处理。
5. 生成数据库导出任务 JSON。
6. 写入 `/home/yhz/iot/real_nifi_data/export_jobs/inbox/{jobId}.json`。
7. 返回前端 `PENDING` / `已提交到 NiFi` 状态。
8. 后端扫描 `done/error`，或由定时任务同步结果。
9. 若 NiFi 成功输出文件，则注册 fileId，并供前端预览、下载、追踪。
10. 任务结束后调用 `stop_existing_flow()`，关闭该 Flow。

### 7.5.6.1 半自动接入失败处理

后端接入失败时，必须按统一策略处理：

1. Docker/服务不可用：返回明确错误或按配置回退 Local。
2. NiFi API 不可访问：记录审计，并返回 NiFi 不可用。
3. 目标 Flow 不存在或不可用：记录审计，并返回 Flow 不可用。
4. Flow 启动失败或关闭失败：记录审计，并返回 Flow 不可用或任务失败。
5. 任务 JSON 写入失败：返回任务提交失败。
6. 任何回退 Local 的行为都必须写入审计，不允许静默回退。

### 7.5.6.2 与后端联调顺序

1. 先由运维或脚本确保 NiFi 容器已启动。
2. 后端能检测 NiFi API 是否可访问。
3. 后端能检查数据库导出 Flow 是否存在且可用。
4. 后端能启用和关闭既有 Flow。
5. 后端能写入导出任务 JSON。
6. NiFi 能消费任务并生成 CSV/JSON/TSV。
7. NiFi 能写入 `done/error` 状态文件。
8. 后端能扫描结果并注册 fileId。

### 7.5.7 数据库导出点击行为要求

当用户点击“数据库导出”时：

- `backend_mode=local`：后端本地执行导出。
- `backend_mode=nifi`：后端必须先确认 NiFi 容器/服务和数据库导出 Flow 已预置且可用，然后启用该 Flow、提交 NiFi 任务，由 NiFi 完成 SQL 查询、格式转换与文件落盘。
- NiFi 可用且任务提交成功时，不允许后端本地直接生成导出文件。
- NiFi 不可用、Flow 不存在或不可用、Flow 启动/关闭失败、任务提交失败时，才允许根据降级策略回退 Local，并必须记录审计。
- 前端不需要知道容器启动、Flow 部署、Flow 启动等细节，只接收统一响应：`PENDING`、`SUCCEEDED`、`FAILED` 或明确错误。

## 八、验收标准

- `index.html` 顶部能看到全局切换按钮
- 切换后所有普通用户页面请求会走对应后端
- tenant 级切换状态可持久化
- 切换行为可审计
- Local 和 NiFi 返回结构一致
- 数据库导出、上传转换、定时导出、自动打标能按模式正确路由
- 当 `backend_mode=nifi` 且 NiFi 容器未运行时，后端应返回明确的不可用错误或按部署配置回退到 `local`，不得默认自动创建新容器
- 当数据库导出 Flow 不存在时，后端应记录审计并返回 Flow 不可用；Flow 的部署由运维/发布流程负责，后端不应默认自动创建新 Flow
- 当数据库导出 Flow 已存在时，后端不得重复创建 Flow，必须复用并确保其处于运行或已启用状态（可由后端启停既有 Flow）
- 点击数据库导出后，任务 JSON 能自动进入 `/home/yhz/iot/real_nifi_data/export_jobs/inbox`
- NiFi 能自动消费任务并将结果写入 `/home/yhz/iot/real_nifi_data/output_csv|output_json|output_tsv`
- NiFi 能写入 `/home/yhz/iot/real_nifi_data/export_jobs/done|error` 状态文件
- 后端能同步 NiFi 状态文件并注册 fileId
- 启动/停止既有 Flow、任务提交、失败回退等关键操作必须记录审计日志
 - 数据库导出、上传转换、定时导出、自动打标能按模式正确路由
 - 当 `backend_mode=nifi` 且 NiFi 容器未运行时，后端应返回明确的不可用错误或按部署配置回退到 `local`，不得在未获运维授权的情况下自动创建新容器
 - 当数据库导出 Flow 不存在时，后端应记录审计并返回 Flow 不可用；Flow 的部署由运维/发布流程负责，后端不应自动创建新 Flow
 - 当数据库导出 Flow 已存在时，后端不得重复创建 Flow，必须复用并确保其处于运行或已启用状态（可由后端启停既有 Flow）
 - 点击数据库导出后，任务 JSON 能自动进入 `/home/yhz/iot/real_nifi_data/export_jobs/inbox`
 - NiFi 能消费任务并将结果写入 `/home/yhz/iot/real_nifi_data/output_csv|output_json|output_tsv`（消费逻辑由 NiFi 侧预置）
 - NiFi 能写入 `/home/yhz/iot/real_nifi_data/export_jobs/done|error` 状态文件
 - 后端能同步 NiFi 状态文件并注册 fileId
 - 启动/停止既有 Flow、任务提交、失败回退等关键操作必须记录审计日志

## 九、与现有文档的关系

本文件是实施版，配合：
- `V2执行清单/V1-NiFi统一实施总方案.md`

使用方式：
- 总方案负责原则和边界
- 本文件负责落地顺序和验收


