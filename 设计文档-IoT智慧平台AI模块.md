# IoT 智慧平台 AI 模块 — 设计文档

版本：v2.0
日期：2026-06-02
作者：xy

---

## 一、项目概述

### 1.1 项目背景

IoT 智慧平台 AI 模块是一个面向工业物联网场景的数据管理与智能分析平台。当前已完成 V1 可演示闭环，核心目标是逐步构建"数据接入与转换 → 标签与特征 → 模型训练/推理 → 预警反馈"的完整能力，并在后续阶段以独立微服务形态对接 JetLinks 生态。

### 1.2 团队与分工

- 团队规模：2 人
- 成员 A（xy）：负责 NiFi 数据转换、流程编排、后端 API、前端控制台
- 成员 B：负责大模型训练/推理服务

### 1.3 技术栈总览

| 层级 | 技术选型 |
|------|----------|
| 后端框架 | FastAPI (Python) |
| 数据编排 | Apache NiFi (Docker 容器化部署) |
| 数据库 | SQLite（本地元数据） + MySQL/PostgreSQL/SQLServer/Oracle（业务数据） |
| 大数据生态 | HDFS / Hive / HBase（Docker 离线部署） |
| 联邦学习 | Flower（规划中） |
| 前端 | 原生 JavaScript + HTML + CSS（简化可改造版） |
| 部署方式 | systemd 常驻服务 |
| 文件元数据 | Linux xattr 扩展属性 + AES-256-GCM 加密 |

### 1.4 版本规划

| 版本 | 代号 | 目标 | 状态 |
|------|------|------|------|
| V1 | 基础平台 | 数据导出/转换/标签/前端闭环 | 已完成，可演示 |
| V2 | Hadoop + AI | 大数据生态集成、联邦学习框架、部署模式 | 规划中 |
| V3 | 元数据增强 | xattr 加密元数据、零文件管理 | 方案设计中 |

> 说明：本文档中的“已完成”默认指 V1 当前可演示基线；真实 NiFi 集群、联邦学习、JetLinks 融合和 xattr 元数据增强仍属于后续规划。

---

## 二、系统架构

### 2.1 总体架构

```
┌──────────────────────────────────────────────────────────────────┐
│                        前端层（UI）                                │
│  主控制台 (index.html)  │  内部管理页 (internal.html)              │
│  登录/注册              │  Local/NiFi 模式切换                    │
└──────────────────────────────┬───────────────────────────────────┘
                               │ REST API
┌──────────────────────────────┴───────────────────────────────────┐
│                    统一编排层（FastAPI 后端）                       │
│  任务管理  │  文件管理  │  标签管理  │  调度管理  │  认证授权       │
│  导出管理  │  静默导出  │  工厂报表  │  健康检查  │  结构化日志     │
└──────────────────────────────┬───────────────────────────────────┘
                               │
          ┌────────────────────┴────────────────────┐
          │                                         │
┌─────────┴──────────┐                ┌─────────────┴──────────────┐
│  Local 执行器       │                │  NiFi 执行器                │
│  (MockExecutor)     │                │  (NiFiExecutor)             │
│  - 本地直接处理      │                │  - REST API 驱动 NiFi Flow │
│  - 8 种数据源导出    │                │  - 已实现客户端与错误分类   │
│  - 格式转换          │                │  - 真实流程映射待配置       │
└─────────┬──────────┘                └─────────────┬──────────────┘
          │                                         │
          └────────────────────┬────────────────────┘
                               │
┌──────────────────────────────┴───────────────────────────────────┐
│                     统一逻辑目录输出                               │
│  output_csv  │  output_json  │  output_tsv  │  tagged_output      │
│  inbox_csv   │  inbox_json   │  inbox_tsv   │  csv_to_json / ...  │
│  silent_exports  │  exports  │  in_data（归档镜像）                │
└──────────────────────────────────────────────────────────────────┘
                               │
┌──────────────────────────────┴───────────────────────────────────┐
│              智能服务层（规划中）                                    │
│  Flower Server（联邦编排）  │  中心化训练服务                       │
│  Client Agent（工厂侧）     │  模型仓库                             │
└──────────────────────────────────────────────────────────────────┘
```

### 2.2 双模执行器设计

系统支持两种执行器模式，通过环境变量 `APP_EXECUTOR_MODE` 或前端切换：

- **Local 模式**：MockExecutor，后端直接在本地进程内完成数据导出、转换、标签操作，适合单机开发和演示
- **NiFi 模式**：NiFiExecutor，通过 NiFi REST API 驱动 NiFi 流程组执行；当前已具备客户端、重试和错误分类能力，但真实流程组 ID 与生产参数仍需按环境配置

两种模式输出到统一的逻辑目录，确保前端无感知切换。

---

## 三、已完成功能（V1 当前可演示基线）

### 3.1 FastAPI 后端核心能力

#### 3.1.1 任务管理

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/v1/jobs` | POST | 创建任务（支持 CONVERT / TAG_MANUAL / TAG_AUTO / COPY_MULTI_FORMAT） |
| `/api/v1/jobs` | GET | 查询任务列表（支持状态/类型过滤） |
| `/api/v1/jobs/{jobId}` | GET | 查询单个任务详情 |
| `/api/v1/jobs/{jobId}/cancel` | POST | 取消任务 |
| `/api/v1/jobs/{jobId}/outputs` | GET | 获取任务产物列表 |

- 状态枚举：`PENDING → RUNNING → SUCCEEDED / FAILED / CANCELED`
- 支持幂等键（`X-Idempotency-Key`）
- 统一响应格式：`{ code, message, data, traceId }`

#### 3.1.2 文件管理

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/v1/files` | GET | 文件列表查询（支持格式/目录过滤） |
| `/api/v1/files/{fileId}` | GET | 文件元数据 |
| `/api/v1/files/{fileId}/download` | GET | 文件下载 |
| `/api/v1/files/{fileId}/preview` | GET | 文件内容预览（分页） |
| `/api/v1/files/{fileId}/edit` | POST | 文件编辑回写 |
| `/api/v1/upload/inbox_csv` | POST | CSV 文件上传 |
| `/api/v1/upload/inbox_json` | POST | JSON 文件上传 |
| `/api/v1/upload/inbox_tsv` | POST | TSV 文件上传 |

- 支持 CSV / JSON / TSV 格式互转
- 文件自动注册到内存索引和 SQLite 数据库
- 支持 `in_data` 归档镜像

#### 3.1.3 数据库导出

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/v1/db/test-connection` | POST | 数据库连接测试 |
| `/api/v1/db/list-tables` | POST | 列出数据库表 |
| `/api/v1/export/mysql` | POST | MySQL 表导出 |
| `/api/v1/export` | POST | 通用数据库导出 |
| `/api/v1/export-jobs` | POST | 创建导出任务（含定时） |
| `/api/v1/export-jobs` | GET | 导出任务列表 |
| `/api/v1/export-jobs/{id}` | GET | 导出任务详情 |
| `/api/v1/export-jobs/{id}` | PATCH | 更新导出任务 |
| `/api/v1/export-jobs/{id}` | DELETE | 删除导出任务 |
| `/api/v1/export-jobs/trigger` | POST | 手动触发导出 |

**支持 8 种数据源**：

| 数据源 | 驱动 | 连接方式 |
|--------|------|----------|
| MySQL | PyMySQL | SQLAlchemy |
| PostgreSQL | psycopg2 | SQLAlchemy |
| SQLServer | pyodbc | SQLAlchemy |
| Oracle | oracledb | SQLAlchemy |
| SQLite | sqlite3 | SQLAlchemy |
| Hive | PyHive | beeline / PyHive |
| HBase | happybase | Thrift |
| HDFS | hdfs (WebHDFS) | HTTP REST |

#### 3.1.4 标签管理

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/v1/tags/manual` | POST | 手动打标签 |
| `/api/v1/tags/auto` | POST | 自动规则打标签 |
| `/api/v1/tags/manual-table` | POST | 表格式编辑（含重命名列） |

- 自动标签：基于奇偶 ID 规则自动分类
- 手动标签：用户指定行 ID 和标签值
- 表格式编辑：支持单元格级修改和列重命名
- 标签结果输出到 `tagged_output` 目录

#### 3.1.5 静默导出

- Manifest 驱动：用户手动导出成功后自动注册到清单
- 后台定时器按 `SILENT_EXPORT_SCHEDULE` 周期执行
- 支持增量导出（基于 marker 列）
- Schema 变更自动滚新文件
- 输出到 `silent_exports/<tenant>/<db_key>/`
- 管理接口：`/internal/tenants/{tenant}/silent-export`

#### 3.1.6 认证与授权

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/v1/auth/login` | POST | 用户登录（返回 JWT Cookie） |
| `/api/v1/auth/register` | POST | 用户注册 |
| `/api/v1/auth/me` | GET | 获取当前用户信息 |
| `/api/v1/auth/logout` | POST | 登出 |

- JWT 令牌，HttpOnly Cookie
- 双数据库认证：优先本地 SQLite，回退 NiFi 数据库
- 支持 bcrypt 和 SHA256 密码哈希
- 管理员权限校验（`_require_admin`）

#### 3.1.7 内部管理接口

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/v1/internal/backend-mode` | GET/POST | 后端模式切换（local/nifi） |
| `/api/v1/internal/nifi-status` | GET | NiFi 组件状态查询 |
| `/api/v1/internal/nifi-export-jobs` | GET | NiFi 导出任务状态 |
| `/api/v1/internal/factory-reports` | GET/POST | 工厂报表 |
| `/api/v1/internal/factory-jobs` | GET | 工厂任务列表 |
| `/api/v1/internal/factory-jobs/{id}/fetch` | POST | 拉取工厂任务 |
| `/api/v1/internal/factory-assets` | GET | 工厂资产列表 |
| `/api/v1/internal/factory-tree` | GET | 工厂文件树 |
| `/api/v1/internal/factory-tree/fetch` | POST | 同步文件到归档 |
| `/api/v1/internal/factory-tree/refresh` | POST | 刷新文件索引 |

#### 3.1.8 可观测性

- 结构化日志：每次 API 请求自动记录 `traceId`、`operation`、`durationMs`、`errorCode`
- 中间件级别拦截（`observability_middleware`）
- 健康检查端点：`/health`、`/api/v1/health/databases`

### 3.2 NiFi 集成

#### 3.2.1 容器化部署

- 已完成 NiFi Docker Compose 与独立 `iot-nifi.service` 的运行方式
- 时区同步（`TZ=Asia/Shanghai`）与宿主路径映射已校正
- 真实集群、Registry 版本管理与生产级 Flow 编排仍在后续 P0 范围内

#### 3.2.2 Flow 架构

```
GetFile (iot_db_export_getfile_v1)
  → 监控 inbox 目录中的 JSON 任务文件
  → ExecuteStreamCommand (iot_db_export_command_v1)
  → 调用 Python Worker 执行实际导出
  → 结果写入 output_csv/output_json/output_tsv
```

- 当前已落地导出流与上传转换流的本地/容器化执行逻辑，真实 NiFi 集群中的流程组映射仍需按环境填充。

#### 3.2.3 NiFi Worker

- `nifi_db_export_worker.py`：支持 8 种数据源的通用导出 Worker
- `nifi_upload_convert_worker.py`：格式转换 Worker
- 原子写入、临时文件替换、错误处理
- 目前更偏向演示与联调用途，尚未替代生产级 NiFi 集群编排

#### 3.2.4 NiFi REST API 客户端

- `NiFiClient` 类：已实现登录、Token 管理、SSL 配置
- 流程组状态查询、启停与主动线程数监控
- 错误分类：`NIFI_AUTH_ERROR` / `NIFI_FLOW_NOT_FOUND` / `NIFI_NETWORK_ERROR` / `NIFI_EXEC_ERROR`
- 真实流程映射模板需要按部署环境替换占位 ID 后才能用于实机联调

### 3.3 前端控制台

#### 3.3.1 主控制台（index.html + app.js）

- 数据库导出面板：数据库类型选择、连接测试、表列表、手动/定时导出
- 文件上传/转换面板：CSV/JSON/TSV 上传、格式转换
- 文件浏览面板：树形目录结构、文件预览、内容编辑
- 标签管理面板：手动打标、自动打标
- 后端模式切换：Local / NiFi 一键切换
- 错误提示：中文友好错误码翻译

#### 3.3.2 内部管理页（internal.html + internal.js）

- 工厂文件树浏览
- 文件资产列表
- 导出任务管理
- 静默导出配置
- 工厂报表查看

#### 3.3.3 登录/注册

- 登录页面（login.html）
- 注册页面（register.html）
- Session 管理

### 3.4 数据库设计

| 表名 | 用途 |
|------|------|
| `jobs` | 任务记录（job_id, type, status, progress, payload） |
| `files` | 文件注册（file_id, name, format, size, storage_type, path） |
| `tags` | 标签记录（file_id, row_id, label, operator） |
| `export_jobs` | 导出任务（job_name, factory_id, schedule, db_config, mode） |
| `iot_users` | 用户认证（username, password_hash, is_admin） |

### 3.5 调度系统

- 基于 APScheduler 的 BackgroundScheduler
- 支持简单调度：`5m` / `15m` / `1h` / `daily` / cron 表达式
- 启动时自动恢复已启用的调度任务
- 静默导出独立调度：`silent-export-worker`

### 3.6 文件元数据（V3 方案）

- 采用 Linux xattr 扩展属性存储元数据
- AES-256-GCM 认证加密，`user.meta` 存密文，`user.checksum` 存校验码
- 后端启动时扫描文件 → 解密 → 构建内存索引
- 零额外文件，数据与元数据一一对应
- 增量刷新：每 60 秒基于 mtime 检测变化

---

## 四、后续规划（待实现）

### 4.1 P0：真实 NiFi 集群部署（预计 1-3 周）

#### 4.1.1 公司侧 NiFi 主集群

- 3-5 节点 NiFi 集群部署
- NiFi Registry 版本管理
- 流程模板标准化
- 负载均衡与高可用

#### 4.1.2 工厂侧 NiFi 边缘节点

- 边缘节点部署与配置
- 与主集群的流程同步
- 离线容错与断点续传

#### 4.1.3 NiFi 执行器完善

- 真实 NiFi 服务器连接配置
- 流程映射模板（替换占位符）
- 完整的状态监控与回填
- 死信队列（DLQ）与补偿机制

### 4.2 P1：联邦学习框架（预计 4-6 周）

#### 4.2.1 Flower Server 部署

- Flower 联邦学习服务器容器化部署
- 轮次调度、客户端抽样、聚合（FedAvg）
- 安全聚合机制（Secure Aggregation）
- 节点证书管理（mTLS）

#### 4.2.2 FastAPI 联邦编排 API

| 接口 | 说明 |
|------|------|
| `POST /api/fl/jobs` | 创建联邦任务 |
| `POST /api/fl/jobs/{id}/start` | 启动任务 |
| `POST /api/fl/jobs/{id}/stop` | 停止任务 |
| `GET /api/fl/jobs/{id}` | 查询任务状态与轮次指标 |
| `POST /api/fl/nodes/register` | 节点注册 |
| `GET /api/fl/models/{version}` | 查询/下载模型 |

#### 4.2.3 Client Agent 开发

- 工厂侧守护进程（sidecar 模式）
- 读取 NiFi 输出目录、manifest 校验
- 本地训练合约 `client_train()` 调用
- 节点注册、心跳、任务拉取、参数上报
- 零原始数据出域（仅上传模型参数与 summary）

#### 4.2.4 模型仓库

- 全局模型 artifact 存储
- 版本管理与 checksum
- V0.1 本地目录 → V0.2 Ceph S3

### 4.3 P1：部署模式实现（预计 7-9 周）

#### 4.3.1 公有化部署

- 数据直接上传到公司数据库
- 数据脱敏与安全传输
- 中心化训练服务对接

#### 4.3.2 私有化部署

- 本地联邦训练流程
- 模型参数上传（不传输原始数据）
- 差分隐私等隐私保护措施

### 4.4 P2：生产就绪（预计 10-12 周）

#### 4.4.1 Ceph 存储迁移

- 存储抽象层设计
- Local / Ceph 适配器
- 双写迁移与一致性校验
- Ceph 对象键规范落地

#### 4.4.2 监控告警

- Prometheus + Grafana 监控指标
- 分级告警（SLA 告警、业务告警、基础设施告警）
- 日志采集（ELK / SLS）

#### 4.4.3 JetLinks 平台融合

- API 接口契约确认
- 鉴权与单点登录集成
- 追踪 ID 全链路打通
- 端到端联调测试

#### 4.4.4 运维文档

- 部署手册
- 联调手册
- 故障处理指南
- 运维 Runbook

### 4.5 功能增强（持续迭代）

#### 4.5.1 前端增强

- 任务状态实时轮询优化
- 导出结果自动展示文件路径
- 训练任务界面（模型选择、参数配置、指标展示）
- 预警中心界面

#### 4.5.2 标签规则增强

- 多列组合规则配置
- 规则可视化编辑
- 标签规则模板库

#### 4.5.3 预测预警闭环

- 使用最新业务数据预测
- 预测结果与阈值比较
- 异常触发预警 → 对接 JetLinks 工单/告警中心

---

## 五、部署与运维

### 5.1 当前部署方式

| 服务 | 管理方式 | 端口 |
|------|----------|------|
| iot-backend | systemd (`iot-backend.service`) | 127.0.0.1:8081 |
| iot-frontend | systemd (`iot-frontend.service`) | 0.0.0.0:5174 |
| iot-backend-health | systemd timer | 定时检查 |
| NiFi | Docker (`iot-nifi.service`) | 8080 |
| Hadoop | Docker Compose | 多端口 |

### 5.2 目录布局

```
/home/yhz/iot/
├── v1-backend/          # FastAPI 后端
│   ├── app/             # 应用代码
│   ├── scripts/         # NiFi Worker 脚本
│   └── deploy/          # systemd 单元
├── v1-frontend/         # 前端静态页面
├── docker/nifi/         # NiFi Docker 编排
├── docker/hadoop/       # Hadoop Docker 编排
├── real_nifi_conf/      # NiFi 运行配置
├── nifi-data/           # 本地模式数据目录
└── real_nifi_data/      # NiFi 模式数据目录
```

### 5.3 容量规划

| 档位 | 日增数据 | 保留天数 | 并发导出 | 并发训练 |
|------|----------|----------|----------|----------|
| Low | 20 GB | 30 天 | 5 | 1 GPU |
| Medium | 200 GB | 90 天 | 20 | 2×4 GPU |
| High | 2 TB | 365 天 | 100 | 8×8 GPU |

---

## 六、关键风险与应对

| 风险 | 影响 | 应对策略 |
|------|------|----------|
| NiFi 集群集成复杂 | 核心功能阻塞 | 先完成单节点验证，再扩展到集群 |
| 联邦学习 Non-IID 收敛困难 | 训练效果不达标 | 保留中心训练作为备选，混合策略 |
| 2 人团队资源紧张 | 进度延迟 | 聚焦核心路径，分阶段交付 |
| JetLinks 接口兼容性 | 集成失败 | 早期介入接口设计，建立契约测试 |
| Ceph 迁移数据一致性 | 数据丢失风险 | 双写校验 + 逐步迁移 |

---

## 七、里程碑与交付物

### 已完成

- V1 封板：数据导出/转换/标签/前端闭环可演示
- 2 轮批量回归测试通过
- 结构化报告输出

### 近期目标（1-3 周）

- NiFi 真实环境配置与验证
- NiFi 执行器核心功能完善
- 前端任务状态同步优化

### 中期目标（4-9 周）

- Flower 联邦学习 PoC 交付
- Client Agent 基础框架
- 公有化/私有化部署模式

### 长期目标（10-12 周）

- Ceph 存储迁移
- 生产级监控告警
- JetLinks 平台融合联调

---

## 八、附录

### 8.1 关键文件索引

| 文件 | 说明 |
|------|------|
| `v1-backend/app/main.py` | FastAPI 主入口，包含所有 API 端点 |
| `v1-backend/app/executors.py` | 执行器实现（MockExecutor / NiFiExecutor） |
| `v1-backend/app/nifi_client.py` | NiFi REST API 客户端 |
| `v1-backend/app/nifi_orchestrator.py` | NiFi 容器编排与 Flow 管理 |
| `v1-backend/app/export_worker.py` | 数据库导出 Worker（含 Hive/HBase/HDFS） |
| `v1-backend/app/silent_export_worker.py` | 静默导出 Worker |
| `v1-backend/app/engine_factory.py` | 多数据源引擎工厂 |
| `v1-backend/app/db_connect.py` | 数据库连接测试与表列表 |
| `v1-backend/app/auth.py` | JWT 认证 |
| `v1-backend/app/admin_routes.py` | 管理员接口 |
| `v1-backend/app/scheduler.py` | APScheduler 调度器 |
| `v1-backend/scripts/nifi_db_export_worker.py` | NiFi 多数据源导出 Worker |
| `v1-frontend/app.js` | 前端主逻辑 |
| `v1-frontend/internal.js` | 内部管理页逻辑 |
| `V1执行清单/` | V1 需求、执行、验收记录 |
| `V2执行清单/` | V2 实施方案与 Hadoop 集成方案 |
| `V3执行清单/` | V3 元数据方案 |
| `AI模块综合执行方案（融合NiFi、联邦学习与双模部署）.md` | 综合执行总方案 |
| `AI模块详细实施方案.md` | 面向 JetLinks 融合的详细方案 |
| `需求-最终版.md` | 采购与部署需求 |