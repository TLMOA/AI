# DEPRECATED: 本文件已被合并，请参考 `v2-nifi/NiFi_统一实施方案.md`

# V1-NiFi 统一实施可执行方案

> 适用范围：`v1-frontend`、`v1-backend`、`V2执行清单/V1-NiFi统一实施总方案.md`

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
3. 然后实现后端 NiFi 自动编排能力：检测容器、自动启动容器、检测 Flow、自动部署或复用 Flow、启动 Flow。
4. 再补齐数据库导出、上传转换、定时导出、自动打标四条高并发链路的 Local / NiFi 双路径实现。
5. 再补目录扫描、任务回调、审计、回滚与演练。
6. 最后再考虑更复杂的数据接入和扩展场景。

## 七点五、NiFi 后端自动编排方案

本项目目标采用“后端自动编排 NiFi 优先”的方式实现 `backend-nifi`：用户在 `nifi` 模式下点击数据库导出时，不要求人工提前启动 NiFi 或人工提前创建 Flow；后端应自动确保 NiFi 执行环境可用，然后提交任务并由 NiFi 完成导出。

运维预启动 NiFi 仍可作为生产稳定化手段，但不是当前交互闭环的唯一前提。当前 MVP 要求是：后端具备自动启动 NiFi 容器、自动检测/部署/复用导出 Flow、自动提交任务、自动回收结果的能力。

### 7.5.1 基本原则

1. 后端自动编排优先：`backend_mode=nifi` 时，点击数据库导出应自动触发 NiFi 环境就绪检查。
2. 不建议每次点击都创建新 Flow：后端应“确保 Flow 存在并运行”，若已存在则复用，若不存在才部署。
3. 单机 NiFi MVP 优先：先跑通 Docker 单机 NiFi，再考虑 Registry、集群、HA。
4. 后端负责容器编排、Flow 就绪检查、任务提交、状态同步、文件注册；NiFi 负责真正执行 SQL、转换格式、写入文件。
5. Local 与 NiFi 共享逻辑目录，但物理目录必须隔离。
6. NiFi 后端统一使用 `/home/yhz/iot/real_nifi_data` 作为宿主机数据根目录。
7. 自动编排过程必须记录审计日志：容器启动、Flow 部署、Flow 启动、任务提交、失败回退都要留痕。

### 7.5.2 NiFi 容器启动前置准备

需要先在宿主机创建以下目录：

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

### 7.5.3 后端自动启动 NiFi 容器

当前阶段推荐优先使用 Docker 单机 NiFi，并由后端在需要时自动拉起。

后端在 `backend_mode=nifi` 且用户点击数据库导出时，应执行以下容器编排逻辑：

1. 检查 Docker 是否可用。
2. 检查是否存在名为 `iot-nifi` 的容器。
3. 若容器不存在，则自动创建并启动容器。
4. 若容器存在但未运行，则自动启动容器。
5. 若容器已运行，则直接进入健康检查。
6. 容器启动后轮询 NiFi API，直到 NiFi UI/API 可访问。
7. 若超过等待时间仍不可用，则返回 NiFi 启动失败，或按降级策略回退 Local，并记录审计。

后端自动创建容器时的等价命令如下：

```bash
docker run -d \
  --name iot-nifi \
  -p 8080:8080 \
  -v /home/yhz/iot/real_nifi_data:/opt/nifi/nifi-current/data/iot \
  apache/nifi:latest
```

后端实现时不应依赖人工执行上述命令，而应通过后端的 NiFi 编排模块完成等价操作。

后续正式化时，应改为 `docker-compose.yml` 或 Docker SDK 管理，并补充：

- NiFi 数据库/Flow 仓库持久化 volume
- MySQL JDBC 驱动挂载
- 用户认证配置
- 时区配置
- 日志目录挂载
- NiFi Registry（可选）

### 7.5.3.1 后端 NiFi 编排模块建议

建议新增后端模块：`nifi_orchestrator`，职责包括：

- `ensure_nifi_container()`：确保 `iot-nifi` 容器存在并运行。
- `wait_nifi_ready()`：轮询 NiFi API，等待 NiFi 就绪。
- `ensure_export_flow()`：确保数据库导出 Flow 已存在。
- `deploy_export_flow()`：当 Flow 不存在时，通过 NiFi API、模板或 Registry 自动部署。
- `start_export_flow()`：确保导出 Flow 内 Processor / Process Group 处于运行状态。
- `submit_export_job()`：写入导出任务 JSON 到 `export_jobs/inbox`。
- `sync_export_result()`：扫描 `done/error` 并注册文件资源。

后端导出接口不得直接散落 Docker/NiFi API 调用，必须通过该编排模块统一封装。

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

### 7.5.5.1 Flow 自动部署与复用要求

后端自动编排不是每次导出都创建一个新 Flow，而是按以下规则处理：

1. 后端通过 NiFi API 检查是否已存在数据库导出 Process Group。
2. 若已存在，则检查该 Process Group 是否运行中。
3. 若已存在但未运行，则自动启动。
4. 若不存在，则从预置模板、版本化 Flow 或代码化定义中自动部署一次。
5. 部署成功后记录 Flow 标识，例如 `processGroupId`、`flowName`、`version`。
6. 后续数据库导出任务复用同一个 Flow，只通过 `export_jobs/inbox` 投递不同任务 JSON。

Flow 标识建议：

- Flow 名称：`iot_mysql_export_flow_v1`
- Process Group 名称：`iot-mysql-export`
- 监听目录：`/opt/nifi/nifi-current/data/iot/export_jobs/inbox`
- 输出根目录：`/opt/nifi/nifi-current/data/iot`

### 7.5.6 后端自动编排执行顺序

用户在 `backend_mode=nifi` 下点击数据库导出时，后端必须按以下顺序执行：

1. 读取当前 tenant 的 `backend_mode`，确认是 `nifi`。
2. 调用 `ensure_nifi_container()`，确保 `iot-nifi` 容器存在并运行。
3. 调用 `wait_nifi_ready()`，确认 NiFi API 可访问。
4. 调用 `ensure_export_flow()`，确认数据库导出 Flow 已存在；不存在则自动部署。
5. 调用 `start_export_flow()`，确保 Flow 处于运行状态。
6. 生成数据库导出任务 JSON。
7. 写入 `/home/yhz/iot/real_nifi_data/export_jobs/inbox/{jobId}.json`。
8. 返回前端 `PENDING` / `已提交到 NiFi` 状态。
9. 后端扫描 `done/error`，或由定时任务同步结果。
10. 若 NiFi 成功输出文件，则注册 fileId，并供前端预览、下载、追踪。

### 7.5.6.1 自动编排失败处理

后端自动编排失败时，必须按统一策略处理：

1. Docker 不可用：返回明确错误或按配置回退 Local。
2. NiFi 容器启动失败：记录审计，并返回 NiFi 启动失败。
3. NiFi API 超时不可用：记录审计，并返回 NiFi 不可用。
4. Flow 自动部署失败：记录审计，并返回 Flow 部署失败。
5. Flow 启动失败：记录审计，并返回 Flow 启动失败。
6. 任务 JSON 写入失败：返回任务提交失败。
7. 任何自动回退 Local 的行为都必须写入审计，不允许静默回退。

### 7.5.6.2 与后端联调顺序

1. 后端能检测 Docker 是否可用。
2. 后端能自动创建或启动 `iot-nifi` 容器。
3. 后端能等待 NiFi API 就绪。
4. 后端能检查数据库导出 Flow 是否存在。
5. 后端能在 Flow 不存在时自动部署，存在时复用。
6. 后端能自动启动 Flow。
7. 后端能写入导出任务 JSON。
8. NiFi 能消费任务并生成 CSV/JSON/TSV。
9. NiFi 能写入 `done/error` 状态文件。
10. 后端能扫描结果并注册 fileId。

### 7.5.7 数据库导出点击行为要求

当用户点击“数据库导出”时：

- `backend_mode=local`：后端本地执行导出。
- `backend_mode=nifi`：后端必须先自动确保 NiFi 容器运行、NiFi API 可用、数据库导出 Flow 已存在且处于运行状态，然后再提交 NiFi 任务，由 NiFi 完成 SQL 查询、格式转换与文件落盘。
- NiFi 可用且任务提交成功时，不允许后端本地直接生成导出文件。
- NiFi 不可用、Flow 不存在且自动部署失败、任务提交失败时，才允许根据降级策略回退 Local，并必须记录审计。
- 前端不需要知道容器启动、Flow 部署、Flow 启动等细节，只接收统一响应：`PENDING`、`SUCCEEDED`、`FAILED` 或明确错误。

## 八、验收标准

- `index.html` 顶部能看到全局切换按钮
- 切换后所有普通用户页面请求会走对应后端
- tenant 级切换状态可持久化
- 切换行为可审计
- Local 和 NiFi 返回结构一致
- 数据库导出、上传转换、定时导出、自动打标能按模式正确路由
- 当 `backend_mode=nifi` 且 NiFi 容器未运行时，点击数据库导出能由后端自动启动 `iot-nifi` 容器
- 当数据库导出 Flow 不存在时，后端能自动部署或创建该 Flow
- 当数据库导出 Flow 已存在时，后端不得重复创建 Flow，必须复用并确保其处于运行状态
- 点击数据库导出后，任务 JSON 能自动进入 `/home/yhz/iot/real_nifi_data/export_jobs/inbox`
- NiFi 能自动消费任务并将结果写入 `/home/yhz/iot/real_nifi_data/output_csv|output_json|output_tsv`
- NiFi 能写入 `/home/yhz/iot/real_nifi_data/export_jobs/done|error` 状态文件
- 后端能同步 NiFi 状态文件并注册 fileId
- 自动启动容器、自动部署 Flow、自动启动 Flow、失败回退均有审计记录

## 九、与现有文档的关系

本文件是实施版，配合：
- `V2执行清单/V1-NiFi统一实施总方案.md`

使用方式：
- 总方案负责原则和边界
- 本文件负责落地顺序和验收


