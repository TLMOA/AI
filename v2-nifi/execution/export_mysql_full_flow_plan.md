# 数据库导出完整流程方案

> 适用范围：`v1-frontend`、`v1-backend`、`v2-nifi/V1-NiFi统一实施可执行方案.md`
>
> 默认前提：NiFi 容器与数据库导出 Flow 先由运维或脚本预置，后端只负责对接、启停既有 Flow、提交任务和同步结果；自动创建容器或自动部署 Flow 仅作为显式开关能力，不作为默认生产行为。

## 一、目标

把数据库导出做成一条可闭环验证的完整链路，确保用户在前端点击导出后，无论选择 `local` 还是 `nifi`，都能得到统一的任务状态、文件产物和追踪结果。

这条链路需要同时验证以下能力：

1. 前端导出按钮能走统一 API adapter。
2. 后端能根据 `backend_mode` 路由到 Local 或 NiFi。
3. `backend_mode=nifi` 时，后端能把导出任务写入 `real_nifi_data/export_jobs/inbox`。
4. NiFi 能消费任务并输出到 `output_csv|output_json|output_tsv`。
5. NiFi 能写出 `done/error` 状态文件。
6. 后端能扫描结果并注册 `fileId`，供前端预览和下载。

## 二、当前入口

### 1. 前端入口

- 导出按钮来自 `v1-frontend` 的数据库导出区域。
- 前端通过统一 adapter 调用后端，不直接拼接多个后端地址。

### 2. 后端入口

- `POST /api/v1/export/mysql`
- `backend_mode=local`：本地直连数据库并导出。
- `backend_mode=nifi`：先检查 NiFi 就绪，再提交 NiFi 任务。

### 3. NiFi 接入入口

- 后端导出任务会写入：`/home/yhz/real_nifi_data/export_jobs/inbox/{jobId}.json`
- 输出目录：`/home/yhz/real_nifi_data/output_csv|output_json|output_tsv`
- 状态目录：`/home/yhz/real_nifi_data/export_jobs/done|error`

## 三、前置条件

### 1. 容器前置

默认需要先启动 NiFi 容器，且容器能挂载宿主机数据目录：

```bash
docker run -d \
  --name iot-nifi \
  -p 8080:8080 \
  -v /home/yhz/real_nifi_data:/opt/nifi/nifi-current/data/iot \
  apache/nifi:latest
```

### 2. Flow 前置

默认需要提前准备数据库导出 Flow，至少包含：

1. 监听 `export_jobs/inbox` 的输入处理器。
2. 解析导出任务 JSON 的属性处理器。
3. 执行 MySQL 查询的处理器。
4. 转换 CSV / JSON / TSV 的处理器。
5. 输出结果文件到 `output_*` 的处理器。
6. 成功/失败后写 `done/error` 状态文件的处理器。

### 3. JDBC 前置

- 需要 MySQL Connector/J。
- 需要在 NiFi 中配置 `DBCPConnectionPool` 或等价连接服务。
- 需要确认数据库账号具备目标表查询权限。

### 4. 后端配置前置

- `NIFI_AUTO_CREATE_CONTAINER=false`
- `NIFI_AUTO_START_CONTAINER=false`
- `NIFI_AUTO_DEPLOY_FLOW=false`

这三个默认值适合生产默认策略；如果只是演示或联调，可以显式改成 `true`。

## 四、完整执行流程

### 1. 前端发起导出

用户在前端填写数据库信息后点击导出，前端只调用统一接口，不关心后端实际是 Local 还是 NiFi。

### 2. 后端读取模式

后端读取 tenant 级 `backend_mode`：

- `local`：直接走本地导出流程。
- `nifi`：进入 NiFi 编排流程。

### 3. NiFi 就绪检查

`backend_mode=nifi` 时，后端按以下顺序处理：

1. 检查 NiFi 容器或服务是否已运行。
2. 检查 NiFi API 是否可访问。
3. 检查数据库导出 Flow 是否存在且可用。
4. 启用既有 Flow。
5. 准备任务 JSON。

若任一步失败：

- 默认返回明确错误，或按配置回退 `local`。
- 必须写审计记录。

### 4. 任务提交

后端生成导出任务 JSON，并原子写入：

`/home/yhz/real_nifi_data/export_jobs/inbox/{jobId}.json`

任务状态应先返回 `PENDING`，让前端显示“已提交到 NiFi”。

### 5. NiFi 执行

NiFi 侧消费任务后执行：

1. 读取 JSON。
2. 解析数据库连接参数、表名、过滤条件和输出格式。
3. 查询 MySQL。
4. 按输出格式生成文件。
5. 写入 `output_csv|output_json|output_tsv`。
6. 成功后写 `done/{jobId}.json`。
7. 失败后写 `error/{jobId}.json`。

### 6. 后端结果同步

后端周期性扫描 `done/error`：

1. 读取状态文件。
2. 注册输出文件到文件中心。
3. 更新任务状态。
4. 将结果暴露给前端预览、下载和追踪。

### 7. Flow 收尾

任务完成后，后端按需关闭既有 Flow；如果当前策略不允许关闭，也应保持审计可追踪。

## 五、任务 JSON 建议结构

```json
{
  "jobId": "export_20260512_120000_ab12cd",
  "factoryId": "factory-001",
  "ownerId": "admin",
  "dbType": "mysql",
  "host": "127.0.0.1",
  "port": 3306,
  "user": "root",
  "password": "******",
  "database": "nifi",
  "table": "sensor_data",
  "where": "id > 100",
  "format": "CSV",
  "appendToLatest": false,
  "targetDir": "/opt/nifi/nifi-current/data/iot/output_csv",
  "targetRoot": "/home/yhz/real_nifi_data",
  "submittedAt": "2026-05-12T10:00:00+08:00"
}
```

## 六、后端建议职责划分

建议后端由 `nifi_orchestrator` 统一承担以下职责：

1. `check_nifi_ready()`：检查 NiFi API 和容器状态。
2. `ensure_export_flow()`：确认导出 Flow 可用。
3. `start_existing_flow()`：启用既有 Flow。
4. `stop_existing_flow()`：关闭既有 Flow。
5. `submit_export_job()`：写入 `inbox`。
6. `sync_export_result()`：扫描 `done/error` 并注册文件。

## 七、验收标准

### 功能验收

- 用户点击数据库导出后，前端能看到 `PENDING` 或成功结果。
- `backend_mode=local` 时，本地导出不受 NiFi 影响。
- `backend_mode=nifi` 时，任务 JSON 能进入 `export_jobs/inbox`。
- NiFi 能消费任务并生成结果文件。
- NiFi 能写 `done/error` 状态文件。
- 后端能同步状态并注册 `fileId`。

### 稳定性验收

- NiFi 容器未运行时，后端默认不擅自创建容器。
- Flow 不存在时，后端默认不擅自部署 Flow。
- 任一步失败都要有审计记录。

### 体验验收

- 前端不需要知道容器、Flow 或目录细节。
- 同一个导出入口在 Local 和 NiFi 下返回统一结构。

## 八、实施顺序建议

1. 先确认 NiFi 容器和 Flow 预置完成。
2. 再验证 `POST /api/v1/export/mysql` 的 NiFi 分支。
3. 然后验证 `inbox -> NiFi -> done/error -> fileId` 的闭环。
4. 最后再做失败回退、审计和重试。

## 九、风险点

1. 容器未启动。
2. Flow 未预置。
3. JDBC 驱动或连接池配置错误。
4. 输出目录权限不足。
5. 状态文件扫描和文件注册不同步。

## 十、结论

这条方案的关键不是让后端“自动帮你搭环境”，而是把数据库导出跑成一条稳定、可审计、可回退的链路。生产默认建议先把 NiFi 容器和 Flow 准备好，再由后端接入执行。