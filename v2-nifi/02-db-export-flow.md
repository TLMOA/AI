# 02 — 数据库导出方案（多数据源通用 + 定时导出）

> 前提：已执行 `01-cleanup-old-flow.md`，NiFi 处于空白状态
>
> 核心思路：**前端填什么数据库，NiFi 就用什么驱动连接**。密码不存储在 NiFi 配置中，而是从前端表单 → 后端任务 JSON → Python Worker 动态传入。Worker 根据任务 JSON 中的 `dbType` 字段自动选择对应驱动。
>
> 依赖文件：
> - Worker: `v1-backend/scripts/nifi_db_export_worker.py`
> - Orchestrator: `v1-backend/app/nifi_orchestrator.py`

---

## 一、支持的数据源

前端下拉框支持的 8 种数据源，Worker 全部覆盖：

| dbType | 数据源 | Python 驱动 | 连接方式 |
|--------|-------|------------|---------|
| `mysql` | MySQL / MariaDB | `pymysql` | `pymysql.connect(host, port, user, password, database)` |
| `postgres` / `postgresql` | PostgreSQL | `psycopg2` | `psycopg2.connect(host, port, user, password, dbname)` |
| `sqlserver` | SQL Server | `pymssql` | `pymssql.connect(host, port, user, password, database)` |
| `oracle` | Oracle | `oracledb` | `oracledb.connect(user, password, dsn=f"{host}:{port}/{database}")` |
| `sqlite` | SQLite 文件 | `sqlite3`（标准库） | `sqlite3.connect(path)`（无 host/port/user/password） |
| `hive` | Apache Hive | `pyhive` | `pyhive.hive.connect(host, port, username, database)` |
| `hdfs` | HDFS 文件系统 | `pyarrow` + `hdfs` | 文件系统操作（无 SQL），按 path 拉取文件 |
| `hbase` | HBase NoSQL | `happybase` | `happybase.Connection(host, port)` + scan 操作 |

> HDFS 和 HBase 不是 SQL 数据库，Worker 需要特殊处理（见 2.1.1 和 2.1.2）。

---

## 二、方案架构

```
GetFile (Root Group) ──success──┐
                                │
                                ▼
                 ExecuteStreamCommand (Root Group)
                         │
                         │ 调用: python3 /opt/nifi/nifi-current/data/iot/bin/nifi_db_export_worker.py
                         │ 传参: 通过 stdin 传入任务 JSON（含 dbType / host / port / user / password / database / table 等）
                         │
                         ▼
              nifi_db_export_worker.py
              ├── json.load(sys.stdin) → 动态凭据
              ├── 根据 dbType 选择驱动
              │     mysql    → pymysql.connect(...)
              │     postgres → psycopg2.connect(...)
              │     sqlserver→ pymssql.connect(...)
              │     oracle   → oracledb.connect(...)
              │     sqlite   → sqlite3.connect(path)
              │     hive     → pyhive.hive.connect(...)
              │     hdfs     → hdfs 客户端读取文件
              │     hbase    → happybase 扫描表
              ├── 执行查询 / 读取数据
              ├── output_{format}/{jobId}_{timestamp}.{csv|json|tsv}
              └── export_jobs/done|error/{jobId}.json
```

**只有 2 个处理器，0 个 Controller Service**。一套 Flow 承接全部 8 种数据源。

---

## 三、完整数据流：从前端表单到数据库（密码从头到尾都来自前端）

**核心原则：前端用户在数据库导出页填写什么连接信息，NiFi 就用什么信息去连接数据库。密码全程不在 NiFi 配置中存储。**

### 3.1 数据流全景图

```
┌─────────────────────────────────────────────────────────────────────────┐
│  阶段 1：前端收集用户输入                                                  │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────┐       │
│  │  数据库导出表单（v1-frontend/index.html）                      │       │
│  │                                                                │       │
│  │  数据库类型: [ MySQL ▾    ]  ← 8 种可选                         │       │
│  │  Host:     [ 127.0.0.1        ]                                │       │
│  │  Port:     [ 3306             ]  ← 自动根据 dbType 切换默认端口  │       │
│  │  User:     [ root             ]                                │       │
│  │  Password: [ ••••••••         ]  ← 用户在此输入密码              │       │
│  │  Database: [ nifi             ]                                │       │
│  │  Table:    [ sensor_data      ]                                │       │
│  │  Where:    [ 1=1              ]  ← 可选筛选条件                  │       │
│  │  Format:   [ CSV ▾           ]                                │       │
│  │                                                                │       │
│  │  [ 导出 ]  ← 用户点击                                          │       │
│  └──────────────────────────────────────────────────────────────┘       │
│                              │                                          │
│              collectDbConfigForSchedule() 收集表单值                      │
│                              │                                          │
│                              ▼                                          │
│  ┌──────────────────────────────────────────────────────────────┐       │
│  │  POST /api/v1/export                                          │       │
│  │  {                                                            │       │
│  │    "db_config": {                                             │       │
│  │      "db_type": "mysql",        ← 前端选的数据库类型            │       │
│  │      "host": "127.0.0.1",       ← 前端填的                     │       │
│  │      "port": 3306,              ← 前端填的                     │       │
│  │      "user": "root",            ← 前端填的                     │       │
│  │      "password": "root",        ← 前端填的，原样传递             │       │
│  │      "database": "nifi",        ← 前端填的                     │       │
│  │      "table": "sensor_data",    ← 前端填的                     │       │
│  │      "path": "",                ← SQLite/HDFS 文件路径         │       │
│  │      "dsn": "",                 ← 可选 DSN 优先                │       │
│  │      "row_key_prefix": ""       ← HBase row key 前缀          │       │
│  │    },                                                         │       │
│  │    "format": "CSV",                                           │       │
│  │    "where": "1=1"                                             │       │
│  │  }                                                            │       │
│  └──────────────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  阶段 2：后端构建任务 JSON（v1-backend/app/main.py）                      │
│                                                                         │
│  _build_nifi_export_task(job)                                           │
│       │                                                                 │
│       │  把前端传来的 db_config 原样搬到任务 JSON 中。                    │
│       │  dbType 字段告诉 Worker 该用哪个驱动。                            │
│       │                                                                 │
│       ▼                                                                 │
│  ┌──────────────────────────────────────────────────────────────┐       │
│  │  任务 JSON → 写入 real_nifi_data/export_jobs/inbox/           │       │
│  │                                                                │       │
│  │  {                                                            │       │
│  │    "jobId":      "export_abc123",                             │       │
│  │    "dbType":     "mysql",           ← 决定驱动选择              │       │
│  │    "host":       "127.0.0.1",       ← 前端填的                 │       │
│  │    "port":       3306,               ← 前端填的                 │       │
│  │    "user":       "root",             ← 前端填的                 │       │
│  │    "password":   "root",             ← 前端填的，原样传递        │       │
│  │    "database":   "nifi",             ← 前端填的                 │       │
│  │    "table":      "sensor_data",      ← 前端填的                 │       │
│  │    "where":      "1=1",                                        │       │
│  │    "format":     "CSV",                                         │       │
│  │    "targetDir":  "/opt/nifi/nifi-current/data/iot/output_csv"   │       │
│  │  }                                                            │       │
│  └──────────────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  阶段 3：NiFi 读取任务 JSON → 调用通用 Worker                             │
│                                                                         │
│  GetFile                                                               │
│    扫描 /opt/nifi/nifi-current/data/iot/export_jobs/inbox/*.json        │
│       │                                                                 │
│       ▼（通过 success 关系，FlowFile 内容 = 任务 JSON 全文）              │
│  ExecuteStreamCommand                                                  │
│    python3 /opt/nifi/nifi-current/data/iot/bin/nifi_db_export_worker.py │
│    stdin ← 任务 JSON 全文（含 dbType + password 字段）                   │
│       │                                                                 │
│       ▼                                                                 │
│  ┌──────────────────────────────────────────────────────────────┐       │
│  │  nifi_db_export_worker.py（通用 Worker）                       │       │
│  │                                                                │       │
│  │  def connect(task):                                            │       │
│  │      t = task["dbType"]        ← 来自前端表单                   │       │
│  │      if t == "mysql":                                          │       │
│  │          return pymysql.connect(                                │       │
│  │              host=task["host"], port=task["port"],              │       │
│  │              user=task["user"], password=task["password"],      │       │
│  │              database=task["database"])                         │       │
│  │      elif t in ("postgres", "postgresql"):                     │       │
│  │          return psycopg2.connect(                               │       │
│  │              host=task["host"], port=task["port"],              │       │
│  │              user=task["user"], password=task["password"],      │       │
│  │              dbname=task["database"])                           │       │
│  │      elif t == "sqlserver":                                    │       │
│  │          return pymssql.connect(                                │       │
│  │              host=task["host"], port=task["port"],              │       │
│  │              user=task["user"], password=task["password"],      │       │
│  │              database=task["database"])                         │       │
│  │      elif t == "oracle":                                       │       │
│  │          return oracledb.connect(                               │       │
│  │              user=task["user"], password=task["password"],      │       │
│  │              dsn=f"{task['host']}:{task['port']}/{task['database']}")│
│  │      elif t == "sqlite":                                       │       │
│  │          return sqlite3.connect(task["path"])                   │       │
│  │      elif t == "hive":                                         │       │
│  │          return pyhive.hive.connect(                            │       │
│  │              host=task["host"], port=task["port"],              │       │
│  │              username=task["user"], database=task["database"])  │       │
│  │      elif t == "hdfs":                                         │       │
│  │          return hdfs_client(task)  # 文件系统客户端              │       │
│  │      elif t == "hbase":                                        │       │
│  │          return happybase.Connection(                           │       │
│  │              host=task["host"], port=task["port"])              │       │
│  │                                                                │       │
│  │  def fetch_data(task, conn):                                   │       │
│  │      t = task["dbType"]                                        │       │
│  │      if t in ("hdfs",):                                        │       │
│  │          return read_hdfs_files(task["path"])  # 读取 HDFS 文件  │       │
│  │      if t in ("hbase",):                                       │       │
│  │          return scan_hbase(conn, task["table"],                 │       │
│  │                            task.get("row_key_prefix"))          │       │
│  │      # 其余为 SQL 数据库                                         │       │
│  │      query = build_sql(task)                                    │       │
│  │      return execute_sql(conn, query)                            │       │
│  └──────────────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  阶段 4：Worker 输出结果                                                 │
│                                                                         │
│  ✓ 成功 → output_csv/export_owner_sensor_data_20260528_120000.csv      │
│          export_jobs/done/export_abc123.json（status: SUCCEEDED）       │
│                                                                         │
│  ✗ 失败 → export_jobs/error/export_abc123.json（status: FAILED）        │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.1.1 HDFS 导出说明

HDFS 不是 SQL 数据库，Worker 不使用 SQL 查询。而是：

- 通过 `task["path"]` 定位 HDFS 目录
- 列出目录下的文件（支持 `task["where"]` 作为文件名过滤条件，如 `*.csv`）
- 读取匹配的文件内容
- 按 `task["format"]` 输出到目标目录
- 如设置 `task["database"]` 或 `task["table"]`，则作为路径子目录追加

### 3.1.2 HBase 导出说明

HBase 是 NoSQL 列存储，Worker 使用 scan 操作：

- 连接 HBase 集群
- 扫描 `task["table"]` 表
- 可选 `task["row_key_prefix"]` 限定行键前缀范围
- 将 scan 结果转换为表格数据
- 按 `task["format"]` 输出

---

### 3.2 关键代码位置（密码传递链路）

| 阶段 | 文件 | 密码从哪里来 |
|------|------|-------------|
| ① 前端输入 | `v1-frontend/index.html` 数据库导出栏 | 用户在密码框手动输入 |
| ② 前端提交 | `v1-frontend/app.js` `exportFromDb()` | `document.getElementById("dbPassword").value` |
| ③ 后端构建 | `v1-backend/app/main.py` `_build_nifi_export_task()` | `db_conf.get("password")`，原样放入 JSON |
| ④ 后端写入 | `v1-backend/app/main.py` `_submit_nifi_export_task()` | 写入 `real_nifi_data/export_jobs/inbox/{jobId}.json` |
| ⑤ NiFi 传递 | NiFi GetFile → ExecuteStreamCommand | JSON 全文通过 stdin 发给 Worker |
| ⑥ Worker 连接 | `v1-backend/scripts/nifi_db_export_worker.py` `connect()` | `task["password"]` 传给对应驱动的 connect 函数 |

**每一步都不对密码做加工、加密、替换。前端填什么，目标数据库就收到什么。**

### 3.3 后端关键函数（参考）

| 文件 | 函数 | 作用 |
|------|------|------|
| `v1-backend/app/main.py` | `_current_backend_mode()` | 判断 `local` / `nifi` |
| `v1-backend/app/main.py` | `_build_nifi_export_task(job)` | 把前端 `db_config` 搬到任务 JSON |
| `v1-backend/app/main.py` | `_submit_nifi_export_task(job)` | 原子写入 `inbox/{jobId}.json` |
| `v1-backend/app/nifi_orchestrator.py` | `NIFI_REAL_BASE_DIR` | `/home/yhz/iot/real_nifi_data` |
| `v1-backend/app/nifi_orchestrator.py` | `NIFI_CONTAINER_DATA_DIR` | `/opt/nifi/nifi-current/data/iot` |

---

## 四、前置条件检查

### 4.1 NiFi 容器运行

```bash
docker ps --filter name=iot-nifi --format "{{.Names}} {{.Status}}"
```

### 4.2 Admin 凭据有效

```bash
curl -sk -X POST https://localhost:8080/nifi-api/access/token \
  -d "username=admin&password=admin@nifi123" -w "\nHTTP %{http_code}\n" | tail -1
# 预期：HTTP 201
```

### 4.3 容器到宿主机连通

```bash
docker exec iot-nifi getent hosts host.docker.internal
# 如果不通，添加：
docker exec iot-nifi bash -c \
  "grep -q host.docker.internal /etc/hosts || echo '172.23.0.1 host.docker.internal' >> /etc/hosts"
```

### 4.4 Python3 和多数据库驱动安装

#### 必须安装（按需）

```bash
# ---- 基础驱动 ----
docker exec iot-nifi pip3 install pymysql       # MySQL / MariaDB
docker exec iot-nifi pip3 install psycopg2-binary # PostgreSQL
docker exec iot-nifi pip3 install pymssql        # SQL Server

# ---- 可选驱动（按项目需求安装）----
docker exec iot-nifi pip3 install oracledb       # Oracle
docker exec iot-nifi pip3 install pyhive         # Hive
docker exec iot-nifi pip3 install pyarrow        # HDFS 文件读取
docker exec iot-nifi pip3 install hdfs           # HDFS 客户端
docker exec iot-nifi pip3 install happybase      # HBase

# ---- SQLite 无需额外安装（Python 标准库自带）----
```

#### 驱动安装状态检查

```bash
docker exec iot-nifi python3 -c "
import sys
errors = []
for mod in ['pymysql', 'psycopg2', 'pymssql', 'sqlite3']:
    try:
        __import__(mod)
        print(f'  {mod:20s} OK')
    except ImportError:
        print(f'  {mod:20s} MISSING')
        errors.append(mod)
# 可选驱动仅警告
for mod in ['oracledb', 'pyhive', 'pyarrow', 'hdfs', 'happybase']:
    try:
        __import__(mod)
        print(f'  {mod:20s} OK')
    except ImportError:
        print(f'  {mod:20s} (optional)')
if errors:
    sys.exit(1)
"
```

### 4.5 Worker 脚本部署

```bash
mkdir -p /home/yhz/iot/real_nifi_data/bin
cp /home/yhz/iot/v1-backend/scripts/nifi_db_export_worker.py \
   /home/yhz/iot/real_nifi_data/bin/nifi_db_export_worker.py
chmod +x /home/yhz/iot/real_nifi_data/bin/nifi_db_export_worker.py

docker exec iot-nifi ls -la /opt/nifi/nifi-current/data/iot/bin/nifi_db_export_worker.py
```

### 4.6 确保输出目录存在

```bash
docker exec iot-nifi mkdir -p \
  /opt/nifi/nifi-current/data/iot/export_jobs/inbox \
  /opt/nifi/nifi-current/data/iot/export_jobs/done \
  /opt/nifi/nifi-current/data/iot/export_jobs/error \
  /opt/nifi/nifi-current/data/iot/output_csv \
  /opt/nifi/nifi-current/data/iot/output_json \
  /opt/nifi/nifi-current/data/iot/output_tsv
```

---

## 五、创建 NiFi Flow（Python API 一键部署）

通过 NiFi REST API 创建 2 个处理器 + 1 条连线 + 启动，一条命令完成。

```bash
cat > /tmp/deploy_db_export_flow.py << 'PYEOF'
import requests, urllib3
urllib3.disable_warnings()

NIFI = "https://localhost:8080/nifi-api"
ROOT = "root"
DATA = "/opt/nifi/nifi-current/data/iot"
ADMIN = {"username": "admin", "password": "admin@nifi123"}

tok = requests.post(f"{NIFI}/access/token", data=ADMIN, verify=False).text


def api(method, path, **kw):
    h = kw.pop("headers", {})
    h["Authorization"] = f"Bearer {tok}"
    return requests.request(method, f"{NIFI}{path}", headers=h, verify=False, **kw)


print("1. 创建 GetFile...")
gf = api("POST", f"/process-groups/{ROOT}/processors", json={
    "revision": {"version": 0},
    "component": {
        "name": "iot_db_export_getfile_v1",
        "type": "org.apache.nifi.processors.standard.GetFile",
        "position": {"x": 320.0, "y": 240.0},
        "config": {"properties": {
            "Input Directory": f"{DATA}/export_jobs/inbox",
            "File Filter": ".*\\.json",
            "Keep Source File": "false",
            "Batch Size": "1",
        }}
    }
}).json()
gf_id = gf["component"]["id"]
print(f"   GetFile ID: {gf_id}")

print("2. 创建 ExecuteStreamCommand...")
cmd = api("POST", f"/process-groups/{ROOT}/processors", json={
    "revision": {"version": 0},
    "component": {
        "name": "iot_db_export_command_v1",
        "type": "org.apache.nifi.processors.standard.ExecuteStreamCommand",
        "position": {"x": 760.0, "y": 240.0},
        "config": {
            "properties": {
                "Command Path": "python3",
                "Command Arguments": f"{DATA}/bin/nifi_db_export_worker.py",
            },
            "autoTerminatedRelationships": ["output stream", "nonzero status", "original"],
        }
    }
}).json()
cmd_id = cmd["component"]["id"]
print(f"   ExecuteStreamCommand ID: {cmd_id}")

print("3. 创建连接 GetFile.success → ExecuteStreamCommand...")
r3 = api("POST", f"/process-groups/{ROOT}/connections", json={
    "revision": {"version": 0},
    "component": {
        "source": {"id": gf_id, "type": "PROCESSOR", "groupId": ROOT},
        "destination": {"id": cmd_id, "type": "PROCESSOR", "groupId": ROOT},
        "selectedRelationships": ["success"],
        "backPressureDataSizeThreshold": "1 GB",
        "backPressureObjectThreshold": "10000",
    }
})
print(f"   HTTP {r3.status_code}")

print("4. 启动两个处理器...")
for pid in [gf_id, cmd_id]:
    api("PUT", f"/processors/{pid}", json={
        "revision": {"version": 0},
        "component": {"id": pid, "state": "RUNNING"}
    })
    print(f"   START: {pid}")

print()
print("Flow 创建完成（8 种数据源通用）")
print(f"  GetFile:              {gf_id}")
print(f"  ExecuteStreamCommand: {cmd_id}")
PYEOF

python3 /tmp/deploy_db_export_flow.py
```

**处理器标识**：
- GetFile: `iot_db_export_getfile_v1`
- ExecuteStreamCommand: `iot_db_export_command_v1`

---

## 六、验证 Worker 能正常工作（按数据源逐个测试）

### 6.1 MySQL 测试

```bash
cat > /tmp/test_mysql.json << 'JSONEOF'
{
  "jobId": "test-mysql-001",
  "factoryId": "factory-001",
  "ownerId": "admin",
  "dbType": "mysql",
  "host": "host.docker.internal",
  "port": 3306,
  "user": "root",
  "password": "root",
  "database": "nifi",
  "table": "sensor_data",
  "where": "1=1",
  "format": "CSV",
  "targetDir": "/opt/nifi/nifi-current/data/iot/output_csv",
  "targetRoot": "/opt/nifi/nifi-current/data/iot"
}
JSONEOF

docker cp /tmp/test_mysql.json iot-nifi:/tmp/test_mysql.json
docker exec iot-nifi python3 \
  /opt/nifi/nifi-current/data/iot/bin/nifi_db_export_worker.py \
  < /tmp/test_mysql.json

docker exec iot-nifi cat /opt/nifi/nifi-current/data/iot/export_jobs/done/test-mysql-001.json
```

### 6.2 PostgreSQL 测试

```bash
cat > /tmp/test_pg.json << 'JSONEOF'
{
  "jobId": "test-pg-001",
  "factoryId": "factory-001",
  "ownerId": "admin",
  "dbType": "postgres",
  "host": "host.docker.internal",
  "port": 5432,
  "user": "postgres",
  "password": "difyai123456",
  "database": "postgres",
  "table": "sensor_data",
  "where": "1=1",
  "format": "JSON",
  "targetDir": "/opt/nifi/nifi-current/data/iot/output_json",
  "targetRoot": "/opt/nifi/nifi-current/data/iot"
}
JSONEOF

docker cp /tmp/test_pg.json iot-nifi:/tmp/test_pg.json
docker exec iot-nifi python3 \
  /opt/nifi/nifi-current/data/iot/bin/nifi_db_export_worker.py \
  < /tmp/test_pg.json

docker exec iot-nifi cat /opt/nifi/nifi-current/data/iot/export_jobs/done/test-pg-001.json
```

### 6.3 SQLite 测试（无 host/port，使用 path 字段）

```bash
cat > /tmp/test_sqlite.json << 'JSONEOF'
{
  "jobId": "test-sqlite-001",
  "factoryId": "factory-001",
  "ownerId": "admin",
  "dbType": "sqlite",
  "path": "/opt/nifi/nifi-current/data/iot/test.db",
  "table": "sensor_data",
  "where": "1=1",
  "format": "CSV",
  "targetDir": "/opt/nifi/nifi-current/data/iot/output_csv",
  "targetRoot": "/opt/nifi/nifi-current/data/iot"
}
JSONEOF
```

### 6.4 Oracle / SQLServer / Hive 测试

> 测试 JSON 结构与 MySQL 相同，仅修改 `dbType`、`port` 和连接凭据。HDFS 和 HBase 需额外字段（path / row_key_prefix）。

---

## 七、NiFi 全链路测试

```bash
# 7.1 复制测试任务到 NiFi inbox
docker cp /tmp/test_mysql.json \
  iot-nifi:/opt/nifi/nifi-current/data/iot/export_jobs/inbox/test-auto-002.json

# 7.2 等待 GetFile 扫描（15 秒）
sleep 15

# 7.3 inbox 已被消费
docker exec iot-nifi ls /opt/nifi/nifi-current/data/iot/export_jobs/inbox/

# 7.4 检查输出
docker exec iot-nifi ls /opt/nifi/nifi-current/data/iot/output_csv/

# 7.5 检查状态
docker exec iot-nifi ls /opt/nifi/nifi-current/data/iot/export_jobs/done/
```

---

## 八、多数据源 / 多密码场景验证（关键验收）

新方案的核心价值：**不同数据库类型、不同密码的任务各自由 Worker 自动选择驱动**。

```bash
# 8.1 MySQL + nifi 用户
cat > /tmp/test_mysql_nifi.json << 'JSONEOF'
{
  "jobId": "test-multi-mysql-001",
  "dbType": "mysql",
  "host": "host.docker.internal", "port": 3306,
  "user": "nifi", "password": "nifi@export123",
  "database": "nifi", "table": "sensor_data",
  "where": "id > 0", "format": "CSV",
  "targetDir": "/opt/nifi/nifi-current/data/iot/output_csv",
  "targetRoot": "/opt/nifi/nifi-current/data/iot"
}
JSONEOF

# 8.2 PostgreSQL + 不同用户
cat > /tmp/test_pg_admin.json << 'JSONEOF'
{
  "jobId": "test-multi-pg-001",
  "dbType": "postgres",
  "host": "host.docker.internal", "port": 5432,
  "user": "postgres", "password": "difyai123456",
  "database": "postgres", "table": "sensor_data",
  "where": "id > 0", "format": "JSON",
  "targetDir": "/opt/nifi/nifi-current/data/iot/output_json",
  "targetRoot": "/opt/nifi/nifi-current/data/iot"
}
JSONEOF

docker cp /tmp/test_mysql_nifi.json \
  iot-nifi:/opt/nifi/nifi-current/data/iot/export_jobs/inbox/
docker cp /tmp/test_pg_admin.json \
  iot-nifi:/opt/nifi/nifi-current/data/iot/export_jobs/inbox/

sleep 20

echo "=== CSV (MySQL via nifi user) ==="
docker exec iot-nifi ls /opt/nifi/nifi-current/data/iot/output_csv/ | grep test-multi-mysql

echo "=== JSON (PostgreSQL via postgres user) ==="
docker exec iot-nifi ls /opt/nifi/nifi-current/data/iot/output_json/ | grep test-multi-pg

echo "=== done ==="
docker exec iot-nifi ls /opt/nifi/nifi-current/data/iot/export_jobs/done/
```

> 验收标准：MySQL 和 PostgreSQL 两个不同数据库类型的任务分别用各自驱动和各自密码成功连接并产出文件。

---

## 九、定时导出

定时导出**不创建单独的 NiFi Flow**，而是复用本章的数据库导出 Flow。

### 9.1 架构

```
后端 cron/APScheduler
    │
    │ 按工厂调度策略，到达时间点
    │
    ▼
生成任务 JSON（与手动导出完全一致）
    │
    │ 写入相同的 inbox
    │
    ▼
NiFi 现有 2 个处理器自动消费（无需额外配置）
    │
    │ iot_db_export_getfile_v1 消费
    │ iot_db_export_command_v1 调用同一个 Worker
    │
    ▼
输出到 output_csv|output_json|output_tsv
文件命名加 _scheduled 前缀区分手动导出
```

### 9.2 前端触发

前端 `index.html` 数据库导出栏的"定时导出"区域已内置：

- 频率选择：每5分钟 / 每15分钟 / 每小时 / 每日 / 自定义 cron
- 创建 / 管理已有定时任务
- `POST /api/v1/schedules` 创建调度
- `GET /api/v1/schedules` 列出已有调度

### 9.3 后端实现参考

```python
# v1-backend/app/main.py（伪代码示意）

def on_schedule_tick(schedule_config: dict):
    """每个调度 tick 时触发"""
    if _current_backend_mode() == "nifi":
        # 复用手动导出的任务构建逻辑
        task = _build_nifi_export_task(schedule_config)
        task["jobId"] = f"scheduled_{schedule_config['id']}_{now_ts()}"
        task["isScheduled"] = True
        # 投递到同一个 inbox
        _submit_nifi_export_task(task)
    else:
        # Local 模式的后端直接执行
        ...
```

### 9.4 调度与手动导出的区别

| 维度 | 手动导出 | 定时导出 |
|------|---------|---------|
| NiFi Flow | 同一个 GetFile + ExecuteStreamCommand | 同一个（复用） |
| Worker 脚本 | `nifi_db_export_worker.py` | 同一个 |
| 任务 JSON inbox | `export_jobs/inbox/` | 同一个 |
| 输出目录 | `output_csv\|json\|tsv` | 同一个 |
| 文件命名 | `export_{owner}_{table}_{ts}.csv` | `export_{owner}_{table}_{ts}.csv` |
| jobId 前缀 | `export_` | `scheduled_` |
| 触发方 | 前端用户点击 | 后端 cron / APScheduler |

---

## 十、后端对接

### 10.1 切换到 nifi 模式

```bash
curl -X POST http://127.0.0.1:8082/api/v1/internal/backend-mode \
  -H "Content-Type: application/json" \
  -d '{"mode": "nifi"}'
```

### 10.2 从后端 API 触发导出

```bash
# MySQL
curl -X POST http://127.0.0.1:8082/api/v1/export \
  -H "Content-Type: application/json" \
  -d '{
    "db_config": {
      "db_type": "mysql",
      "host": "127.0.0.1", "port": 3306,
      "user": "root", "password": "root",
      "database": "nifi", "table": "sensor_data"
    },
    "format": "CSV", "where": "1=1"
  }'

# PostgreSQL
curl -X POST http://127.0.0.1:8082/api/v1/export \
  -H "Content-Type: application/json" \
  -d '{
    "db_config": {
      "db_type": "postgres",
      "host": "127.0.0.1", "port": 5432,
      "user": "postgres", "password": "difyai123456",
      "database": "postgres", "table": "sensor_data"
    },
    "format": "JSON", "where": "1=1"
  }'
```

### 10.3 检查后端是否正确写入 inbox

```bash
ls -la /home/yhz/iot/real_nifi_data/export_jobs/inbox/
cat /home/yhz/iot/real_nifi_data/export_jobs/inbox/export_*.json | python3 -m json.tool
```

> 确认 JSON 中包含 `dbType` 和 `password` 字段。

### 10.4 后端同步结果

```bash
curl -X POST http://127.0.0.1:8082/api/v1/internal/nifi-export-jobs/sync
```

---

## 十一、验收清单

| # | 检查项 | 方法 | 预期 |
|---|--------|------|------|
| 1 | NiFi 容器运行 | `docker ps \| grep iot-nifi` | Up |
| 2 | 基础驱动已安装 | `docker exec iot-nifi python3 -c "import pymysql,psycopg2,pymssql,sqlite3"` | 无报错 |
| 3 | 可选驱动按需安装 | `docker exec iot-nifi python3 -c "import oracledb"` 等 | 按需 |
| 4 | Worker 脚本已部署 | `docker exec iot-nifi ls .../bin/nifi_db_export_worker.py` | 文件存在 |
| 5 | GetFile RUNNING | NiFi UI 或 API | RUNNING |
| 6 | ExecuteStreamCommand RUNNING | NiFi UI 或 API | RUNNING |
| 7 | MySQL Worker 手动执行成功 | 第六章 6.1 | done/ 有 SUCCEEDED |
| 8 | PostgreSQL Worker 手动执行成功 | 第六章 6.2 | done/ 有 SUCCEEDED |
| 9 | NiFi 全链路成功 | 第七章 | inbox 被消费，output 有文件 |
| 10 | 不同 dbType + 不同密码各自成功 | 第八章 | MySQL + PostgreSQL 均成功 |
| 11 | SQLite 文件导出成功（path 模式） | 第六章 6.3 | 无 host/port 也能导出 |
| 12 | 定时导出任务 JSON 可写入同一 inbox | 投递 `scheduled_` 前缀任务 | Worker 正常消费 |
| 13 | 后端写入 inbox | `ls real_nifi_data/.../inbox/` | JSON 含 dbType + password |
| 14 | 后端可同步结果 | `curl ... /sync` | 返回文件列表 |

---

## 十二、故障排查

| 现象 | 排查命令 | 可能原因 |
|------|---------|---------|
| Worker 报 `pymysql` 找不到 | `docker exec iot-nifi python3 -c "import pymysql"` | 未安装对应驱动 |
| Worker 报 `psycopg2` 找不到 | `docker exec iot-nifi python3 -c "import psycopg2"` | 未安装对应驱动 |
| MySQL Access denied | 检查任务 JSON 中 user/password | MySQL 权限不足 |
| PG authentication failed | 检查 `pg_hba.conf` | PG 未允许网络连接 |
| SQL Server 连接失败 | 检查端口 1433 是否开放 | 防火墙 / SQL Server 配置 |
| Oracle 连接超时 | 检查 listener.ora | Oracle 监听未启动 |
| SQLite 文件不存在 | `docker exec iot-nifi ls path` | 文件路径未正确挂载 |
| Hive 连接失败 | 检查 Thrift Server | HiveServer2 未启动 |
| HDFS 读失败 | 检查 NameNode 连通性 | Hadoop 集群不可达 |
| HBase 连接失败 | 检查 Thrift Server | HBase Thrift 未启动 |
| GetFile 不消费文件 | `docker logs iot-nifi --since 2m \| grep GetFile` | Input Directory 路径错误 |
| ExecuteStreamCommand 无输出 | `docker logs iot-nifi --since 2m \| grep ExecuteStreamCommand` | Worker 脚本路径错误 |
| 容器无法访问宿主机 DB | `docker exec iot-nifi ping host.docker.internal` | /etc/hosts 缺少映射 |
| 定时任务 JSON 未被消费 | 检查 inbox 是否有 `scheduled_` 前缀文件 | 文件命名或路径问题 |

---

## 十三、架构决策记录

| 决策 | 理由 |
|------|------|
| 不用 DBCPConnectionPool | 静态密码 + 单一驱动，无法满足动态多数据库需求 |
| 不用 ExecuteSQLRecord | 依赖 DBCP，且不同 DB 的 SQL 方言不同 |
| 用 ExecuteStreamCommand | NiFi 原生支持，通过 stdin 传 JSON |
| 用通用 Python Worker（非每种 DB 一个） | 一套 Flow 承接全部 8 种数据源，dbType 路由驱动 |
| 2 个处理器而非 N 个 | 复杂度最低，Worker 内部做路由 |
| 密码不存储在 NiFi | 前端填什么就是什么，无泄密风险 |
| 宿主机 ↔ 容器通过 `real_nifi_data` 挂载 | 后端写、NiFi 读、NiFi 写、后端读，同一目录 |
| 定时导出复用同一 Flow | 不增加处理器数量，仅通过 jobId 前缀和命名区分 |
| HDFS/HBase 特殊处理 | 非 SQL 数据源，Worker 内部用专用客户端 |