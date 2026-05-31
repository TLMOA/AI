# 05-静默导出NiFi流程设计

---

## 一、功能概述

**静默导出**（Silent Export）是一种后台自动导出机制，当启用后：
- 每次手动表导出成功后，自动将该表注册到导出清单（Manifest）
- 后台调度器定期遍历清单，自动执行增量/全量导出
- 支持 Schema 变更检测，自动归档旧版本文件
- 按租户/数据库维度隔离存储

---

## 二、架构设计

### 2.1 数据流全景图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        静默导出数据流                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌──────────────┐     register_table()      ┌──────────────────────┐   │
│  │ 手动导出 API │ ────────────────────────→ │ silent_export_manifest.json │   │
│  │  /api/v1/export│                        │  (表注册清单)         │   │
│  └──────────────┘                          └──────────┬───────────┘   │
│                                                       │               │
│                    schedule_job("silent-export-worker")│               │
│                                                       ▼               │
│  ┌─────────────────────────────────────────────────────────────┐       │
│  │           Silent Export Worker (Local / NiFi)              │       │
│  │  ┌─────────────────────────────────────────────────────┐   │       │
│  │  │ 1. 读取 manifest → 遍历所有已注册表                  │   │       │
│  │  │ 2. 连接数据库 → 检查 Schema 哈希                    │   │       │
│  │  │ 3. 增量导出 / 全量导出                              │   │       │
│  │  │ 4. 原子写入 output/silent_exports/                  │   │       │
│  │  └─────────────────────────────────────────────────────┘   │       │
│  └─────────────────────────────────────────────────────────────┘       │
│                              │                                         │
│                              ▼                                         │
│  ┌─────────────────────────────────────────────────────────────┐       │
│  │           output/silent_exports/<tenant>/<db_key>/          │       │
│  │     ├── export_system_<table>_<ts>.csv                     │       │
│  │     ├── export_system_<table>_<ts>_<schema_date>.csv       │       │
│  │     └── <table>_silent_export.csv.meta.json                │       │
│  └─────────────────────────────────────────────────────────────┘       │
│                                                                        │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 三、目录结构

### 3.1 统一存储目录

| 路径 | 说明 | 权限 |
|---|---|---|
| `output/silent_exports/` | 静默导出根目录 | 读写 |
| `output/silent_exports/<tenant>/` | 租户隔离目录 | 读写 |
| `output/silent_exports/<tenant>/<db_key>/` | 数据库实例隔离目录 | 读写 |
| `output/silent_exports/<tenant>/<db_key>/tmp/` | 临时文件目录 | 读写 |

### 3.2 db_key 生成规则

```python
def _db_key(db_conf):
    db_type = (db_conf.get("db_type") or "mysql").lower()
    if db_type == "sqlite":
        path = db_conf.get("path") or db_conf.get("database") or "unknown"
        name = Path(path).stem
        return f"sqlite_{name}"
    host = db_conf.get("host") or "127.0.0.1"
    port = db_conf.get("port") or 0
    database = db_conf.get("database") or "unknown"
    return f"{db_type}_{host}_{port}_{database}"
```

**示例**：
- MySQL: `mysql_127.0.0.1_3306_sensor_db`
- SQLite: `sqlite_local_db`
- PostgreSQL: `postgres_192.168.1.10_5432_production`

---

## 四、文件命名规则

### 4.1 导出文件

| 场景 | 命名格式 | 示例 |
|---|---|---|
| 正常导出 | `export_system_{table}_{ts}.csv` | `export_system_sensor_data_20260529_120000.csv` |
| Schema 变更 | `export_system_{table}_{ts}_{schema_date}.csv` | `export_system_sensor_data_20260529_120000_20260529.csv` |

### 4.2 元数据文件

| 文件类型 | 命名格式 | 示例 |
|---|---|---|
| 元数据 | `{table}_silent_export.csv.meta.json` | `sensor_data_silent_export.csv.meta.json` |

### 4.3 元数据结构

```json
{
  "schema_hash": "abc123def456",
  "last_export_marker": "2026-05-29 12:00:00",
  "last_export_at": "2026-05-29T12:00:00Z",
  "rows_exported": 1000,
  "trigger_reason": "incremental_append",
  "previous_schema_files": [
    "export_system_sensor_data_20260520_020000_20260520.csv"
  ]
}
```

---

## 五、NiFi 流程设计

### 5.1 处理器配置

| 处理器 | 名称 | 配置说明 |
|---|---|---|
| **GetFile** | `iot_silent_export_getfile_v1` | 监听 `silent_export_jobs/inbox/` |
| **ExecuteStreamCommand** | `iot_silent_export_command_v1` | 调用 `nifi_silent_export_worker.py` |
| **PutFile** | `iot_silent_export_putfile_v1` | 输出到 `output/silent_exports/` |
| **UpdateAttribute** | `iot_silent_export_updateattr_v1` | 设置输出文件名 |

### 5.2 流程连接

```
GetFile ──→ ExecuteStreamCommand ──→ PutFile
     │                                    │
     └─────────────→ (失败) ──────────────┘
                      ↓
               PutFile (error)
```

### 5.3 Worker 脚本

**脚本路径**：`/opt/nifi/nifi-current/data/iot/bin/nifi_silent_export_worker.py`

**执行命令**：
```bash
python3 /opt/nifi/nifi-current/data/iot/bin/nifi_silent_export_worker.py \
  --manifest /opt/nifi/nifi-current/data/iot/silent_export_manifest.json \
  --config /opt/nifi/nifi-current/data/iot/silent_export_config.json \
  --output /opt/nifi/nifi-current/data/iot/output/silent_exports
```

---

## 六、导出策略

### 6.1 首次导出

```
条件：meta文件不存在 或 无 last_export_marker
操作：全量导出 → 写入新文件 → 更新meta
触发原因：initial_full_export
```

### 6.2 增量导出

```
条件：存在 last_export_marker 且 Schema 未变更
操作：增量查询(WHERE marker_col > last_marker) → 追加写入
触发原因：incremental_append
```

### 6.3 Schema 变更处理

```
条件：当前 schema_hash != 上次 schema_hash
操作：
  1. 将旧文件重命名添加日期后缀
  2. 创建新文件
  3. 记录到 previous_schema_files
触发原因：schema_change_full
```

---

## 七、配置管理

### 7.1 启用/禁用接口

| API | 方法 | 说明 |
|---|---|---|
| `/internal/tenants/{tenant}/silent-export` | GET | 查询租户静默导出配置 |
| `/internal/tenants/{tenant}/silent-export` | POST | 设置租户静默导出配置 |
| `/internal/tenants/{tenant}/silent-export/trigger` | POST | 手动触发一次静默导出 |

### 7.2 配置结构

```json
{
  "enabled": true,
  "cron": "0 2 * * *",
  "retention_days": 7,
  "incremental_marker_column": "updated_at"
}
```

---

## 八、与其他功能的关系

### 8.1 手动导出联动

```
手动导出成功 → register_table() → manifest 添加记录 → 下次调度自动纳入静默导出
```

### 8.2 数据生命周期

```
静默导出文件 → 工厂归档策略 → 定期清理过期文件（按 retention_days）
```

---

## 九、部署说明

### 9.1 目录创建

```bash
# 创建目录结构
mkdir -p /home/yhz/iot/real_nifi_data/output/silent_exports
mkdir -p /home/yhz/iot/real_nifi_data/silent_export_jobs/{inbox,done,error}

# 设置权限
chown -R nifi:nifi /home/yhz/iot/real_nifi_data/output/silent_exports
chown -R nifi:nifi /home/yhz/iot/real_nifi_data/silent_export_jobs
```

### 9.2 Worker 脚本部署

```bash
cp v1-backend/app/silent_export_worker.py \
   /home/yhz/iot/real_nifi_data/bin/nifi_silent_export_worker.py
chmod +x /home/yhz/iot/real_nifi_data/bin/nifi_silent_export_worker.py
```

---

## 十、监控与运维

### 10.1 监控指标

| 指标 | 说明 | 采集方式 |
|---|---|---|
| 导出执行次数 | 成功/失败次数 | 日志分析 |
| 导出数据量 | 每次导出行数 | meta.json |
| Schema 变更次数 | Schema 变更触发次数 | meta.json |
| 清单大小 | 已注册表数量 | manifest.json |

### 10.2 运维命令

```bash
# 手动执行一次静默导出
python3 /home/yhz/iot/real_nifi_data/bin/nifi_silent_export_worker.py --once

# 查看清单内容
cat /home/yhz/iot/real_nifi_data/silent_export_manifest.json

# 查看配置
cat /home/yhz/iot/real_nifi_data/silent_export_config.json
```

---

## 十一、安全考虑

1. **数据库连接**：使用任务 JSON 中的动态密码，不在 NiFi 中存储固定凭据
2. **文件权限**：输出目录限制为 NiFi 用户可读写
3. **并发控制**：使用文件锁防止并发写入同一文件
4. **审计日志**：记录每次导出操作到审计日志

---

## 十二、与统一命名规范的对齐

| 功能 | 格式 | 静默导出实现 |
|---|---|---|
| 数据库导出 | `export_{owner}_{table}_{ts}.csv` | `export_system_{table}_{ts}.csv` |
| 定时导出 | `export_{owner}_{table}_{ts}.csv` | 复用同一命名规则 |

> **说明**：静默导出使用 `system` 作为固定 owner，表示这是系统自动执行的导出任务。