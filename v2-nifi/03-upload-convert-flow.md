# 03 — 上传自动转换方案（ListFile + FetchFile + ExecuteStreamCommand + Worker）

> 前提：已执行 `01-cleanup-old-flow.md`，NiFi 处于空白状态；已完成 `02-db-export-flow.md` 的数据库导出链路。
>
> 核心思路：与数据库导出同模式——前端上传文件后，后端将任务 JSON 写入共享目录，NiFi 消费任务 JSON，调用 Python Worker 完成格式转换。
>
> 依赖文件：
> - Worker: `v1-backend/scripts/nifi_upload_convert_worker.py`（待创建）
> - Orchestrator: `v1-backend/app/nifi_orchestrator.py`

---

## 一、方案架构

```
ListFile (Root Group)
  扫描 /opt/nifi/.../inbox_csv, inbox_json, inbox_tsv
    │
    ▼
FetchFile (Root Group)
  读取源文件内容
    │
    ▼ (success)
ExecuteStreamCommand (Root Group)
    │
    │ 调用: python3 /opt/nifi/.../bin/nifi_upload_convert_worker.py
    │ 传参: stdin ← 任务 JSON（含 sourcePath / sourceFormat / targetFormats / fileName）
    │
    ▼
nifi_upload_convert_worker.py
  ├── json.load(sys.stdin) → 读取任务参数
  ├── 读取源文件（inbox_csv / inbox_json / inbox_tsv）
  ├── 检测/解析输入格式（CSV / NDJSON / JSON数组 / JSON单对象 / TSV）
  ├── 转换为目标格式
  │     CSV → JSON (csv_to_json/), CSV → TSV (csv_to_tsv/)
  │     JSON → CSV (json_to_csv/), JSON → TSV (json_to_tsv/)
  │     TSV → CSV (tsv_to_csv/), TSV → JSON (tsv_to_json/)
  ├── 原子写：临时文件 → 校验 → 重命名
  └── 写状态文件：convert_jobs/done|error/{jobId}.json
```

**3 个处理器，0 个 Controller Service**。

---

## 二、数据流（前端 → 后端 → NiFi → Worker）

```
┌──────────────────────────────────────────────────────────────────┐
│  阶段 1：前端上传文件 + 选择目标格式                                │
│                                                                  │
│  ┌─────────────────────────────────────────────┐                 │
│  │  上传表单                                    │                 │
│  │                                             │                 │
│  │  选择文件: [ data.csv           ] [浏览...]  │                 │
│  │  源格式:    [ CSV ▾            ]            │                 │
│  │  目标格式:  ☑ JSON  ☑ TSV                  │                 │
│  │                                             │                 │
│  │  [ 上传并转换 ]                               │                 │
│  └─────────────────────────────────────────────┘                 │
│                        │                                         │
│  POST /api/v1/upload/inbox_csv                                   │
│  multipart/form-data: file=data.csv, target_formats=["JSON","TSV"]│
│                        ▼                                         │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│  阶段 2：后端保存文件 + 写入任务 JSON                              │
│                                                                  │
│  v1-backend/app/main.py                                          │
│  1. 保存 uploaded_<user>_csv_YYYYMMDD_HHMMSS.csv 到             │
│     real_nifi_data/inbox_csv/                                    │
│  2. 生成任务 JSON → real_nifi_data/convert_jobs/inbox/           │
│                                                                  │
│  {                                                               │
│    "jobId":        "convert_abc123",                             │
│    "sourcePath":   "/opt/nifi/.../inbox_csv/uploaded_user_csv_...│
│    "sourceFormat": "CSV",                                        │
│    "targetFormats": ["JSON", "TSV"],                             │
│    "fileName":     "uploaded_user_csv_20260528_120000",          │
│    "ownerId":      "user-001",                                   │
│    "factoryId":    "factory-001"                                 │
│  }                                                               │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│  阶段 3：NiFi → Worker 处理                                       │
│                                                                  │
│  ListFile → FetchFile → ExecuteStreamCommand                     │
│        ↓                                                        │
│  nifi_upload_convert_worker.py                                   │
│    ├── 读取源文件                                                 │
│    ├── CSV→JSON: 按行解析 CSV，生成 NDJSON                        │
│    ├── CSV→TSV: 逗号 → 制表符                                     │
│    ├── JSON→CSV: JSON数组/NDJSON → CSV                            │
│    ├── JSON→TSV: JSON数组/NDJSON → TSV                            │
│    ├── TSV→CSV: 制表符 → 逗号                                     │
│    ├── TSV→JSON: 按行解析 TSV，生成 NDJSON                         │
│    └── 原子写每个目标文件                                          │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│  阶段 4：Worker 输出结果                                           │
│                                                                  │
│  ✓ 成功 → csv_to_json/uploaded_user_csv_20260528_120000.json     │
│          csv_to_tsv/uploaded_user_csv_20260528_120000.tsv         │
│          convert_jobs/done/convert_abc123.json (SUCCEEDED)        │
│                                                                  │
│  ✗ 失败 → convert_jobs/error/convert_abc123.json (FAILED)        │
└──────────────────────────────────────────────────────────────────┘
```

---

## 三、前置条件检查

### 3.1 目录创建

```bash
docker exec iot-nifi mkdir -p \
  /opt/nifi/nifi-current/data/iot/convert_jobs/inbox \
  /opt/nifi/nifi-current/data/iot/convert_jobs/done \
  /opt/nifi/nifi-current/data/iot/convert_jobs/error \
  /opt/nifi/nifi-current/data/iot/inbox_csv \
  /opt/nifi/nifi-current/data/iot/inbox_json \
  /opt/nifi/nifi-current/data/iot/inbox_tsv \
  /opt/nifi/nifi-current/data/iot/csv_to_json \
  /opt/nifi/nifi-current/data/iot/csv_to_tsv \
  /opt/nifi/nifi-current/data/iot/json_to_csv \
  /opt/nifi/nifi-current/data/iot/json_to_tsv \
  /opt/nifi/nifi-current/data/iot/tsv_to_csv \
  /opt/nifi/nifi-current/data/iot/tsv_to_json
```

### 3.2 Worker 脚本部署

```bash
cp /home/yhz/iot/v1-backend/scripts/nifi_upload_convert_worker.py \
   /home/yhz/iot/real_nifi_data/bin/nifi_upload_convert_worker.py
chmod +x /home/yhz/iot/real_nifi_data/bin/nifi_upload_convert_worker.py

docker exec iot-nifi ls -la \
  /opt/nifi/nifi-current/data/iot/bin/nifi_upload_convert_worker.py
```

### 3.3 其余前置条件

与 `02-mysql-export-new-flow.md` 第三章一致（容器运行、Admin 凭据、Python3 + pymysql 已可用）。

---

## 四、创建 NiFi Flow（Python API 一键部署）

```bash
cat > /tmp/deploy_convert_flow.py << 'PYEOF'
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


print("1. 创建 ListFile...")
lf = api("POST", f"/process-groups/{ROOT}/processors", json={
    "revision": {"version": 0},
    "component": {
        "name": "iot_upload_convert_listfile_v1",
        "type": "org.apache.nifi.processors.standard.ListFile",
        "position": {"x": 100.0, "y": 120.0},
        "config": {
            "properties": {
                "Input Directory": f"{DATA}/convert_jobs/inbox",
                "File Filter": ".*\\.json",
            },
            "schedulingPeriod": "5 sec",
        }
    }
}).json()
lf_id = lf["component"]["id"]
print(f"   ListFile ID: {lf_id}")

print("2. 创建 FetchFile...")
ff = api("POST", f"/process-groups/{ROOT}/processors", json={
    "revision": {"version": 0},
    "component": {
        "name": "iot_upload_convert_fetchfile_v1",
        "type": "org.apache.nifi.processors.standard.FetchFile",
        "position": {"x": 320.0, "y": 120.0},
    }
}).json()
ff_id = ff["component"]["id"]
print(f"   FetchFile ID: {ff_id}")

print("3. 创建 ExecuteStreamCommand...")
cmd = api("POST", f"/process-groups/{ROOT}/processors", json={
    "revision": {"version": 0},
    "component": {
        "name": "iot_upload_convert_command_v1",
        "type": "org.apache.nifi.processors.standard.ExecuteStreamCommand",
        "position": {"x": 540.0, "y": 120.0},
        "config": {
            "properties": {
                "Command Path": "python3",
                "Command Arguments": f"{DATA}/bin/nifi_upload_convert_worker.py",
            },
            "autoTerminatedRelationships": [
                "output stream", "nonzero status", "original",
            ],
        }
    }
}).json()
cmd_id = cmd["component"]["id"]
print(f"   ExecuteStreamCommand ID: {cmd_id}")

print("4. 创建连接 ListFile → FetchFile...")
api("POST", f"/process-groups/{ROOT}/connections", json={
    "revision": {"version": 0},
    "component": {
        "source": {"id": lf_id, "type": "PROCESSOR", "groupId": ROOT},
        "destination": {"id": ff_id, "type": "PROCESSOR", "groupId": ROOT},
        "selectedRelationships": ["success"],
        "backPressureDataSizeThreshold": "1 GB",
        "backPressureObjectThreshold": "10000",
    }
})

print("5. 创建连接 FetchFile → ExecuteStreamCommand...")
api("POST", f"/process-groups/{ROOT}/connections", json={
    "revision": {"version": 0},
    "component": {
        "source": {"id": ff_id, "type": "PROCESSOR", "groupId": ROOT},
        "destination": {"id": cmd_id, "type": "PROCESSOR", "groupId": ROOT},
        "selectedRelationships": ["success"],
        "backPressureDataSizeThreshold": "1 GB",
        "backPressureObjectThreshold": "10000",
    }
})

print("6. 启动...")
for pid in [lf_id, ff_id, cmd_id]:
    api("PUT", f"/processors/{pid}", json={
        "revision": {"version": 0},
        "component": {"id": pid, "state": "RUNNING"}
    })
    print(f"   START: {pid}")

print()
print("✓ 上传转换 Flow 创建并启动完成")
print(f"  ListFile:              {lf_id}")
print(f"  FetchFile:             {ff_id}")
print(f"  ExecuteStreamCommand:  {cmd_id}")
PYEOF

python3 /tmp/deploy_convert_flow.py
```

---

## 五、Worker 脚本关键逻辑（概述）

`nifi_upload_convert_worker.py` 核心流程：

```python
def main():
    task = json.load(sys.stdin)

    source_path   = task["sourcePath"]
    source_format = task["sourceFormat"]
    targets       = task["targetFormats"]   # ["JSON", "TSV"]
    file_name     = task["fileName"]
    job_id        = task["jobId"]

    rows = read_file(source_path, source_format)
    # read_file: 根据 source_format 解析 CSV/NDJSON/JSON数组/TSV

    for target_format in targets:
        output_path = build_output_path(source_format, target_format, file_name)
        write_file(output_path, target_format, rows)
        # write_file: CSV→writerows, JSON→NDJSON逐行写入, TSV→制表符分隔

    write_status("done", job_id, "SUCCEEDED")
```

**Worker 细节由开发阶段补齐**，包括：
- `read_file()` 的格式自动检测（CSV 可检测分隔符、JSON 自动判断 NDJSON/数组/单对象）
- `write_file()` 的原子写（临时文件 + `os.rename`）
- 大文件流式处理（`csv.reader` + 逐行 `json.dumps`，避免内存溢出）
- 错误处理（解析失败 → 写 `error/{jobId}.json` 含 `errorMessage`）

---

## 六、全链路测试

```bash
# 6.1 准备测试 CSV 文件
cat > /tmp/test_upload.csv << 'CSVEOF'
id,name,value
1,温度,23.5
2,湿度,67.2
3,气压,1013.2
CSVEOF

docker cp /tmp/test_upload.csv \
  iot-nifi:/opt/nifi/nifi-current/data/iot/inbox_csv/uploaded_admin_csv_20260528_120000.csv

# 6.2 投递转换任务 JSON
cat > /tmp/test_convert_task.json << 'JSONEOF'
{
  "jobId": "convert-test-001",
  "sourcePath": "/opt/nifi/nifi-current/data/iot/inbox_csv/uploaded_admin_csv_20260528_120000.csv",
  "sourceFormat": "CSV",
  "targetFormats": ["JSON", "TSV"],
  "fileName": "uploaded_admin_csv_20260528_120000",
  "ownerId": "admin",
  "factoryId": "factory-001"
}
JSONEOF

docker cp /tmp/test_convert_task.json \
  iot-nifi:/opt/nifi/nifi-current/data/iot/convert_jobs/inbox/convert-test-001.json

# 6.3 等待 NiFi 处理
sleep 15

# 6.4 检查输出
echo "=== csv_to_json ==="
docker exec iot-nifi ls -la /opt/nifi/nifi-current/data/iot/csv_to_json/

echo "=== csv_to_tsv ==="
docker exec iot-nifi ls -la /opt/nifi/nifi-current/data/iot/csv_to_tsv/

echo "=== done ==="
docker exec iot-nifi cat /opt/nifi/nifi-current/data/iot/convert_jobs/done/convert-test-001.json
```

---

## 七、与数据库导出的关键区别

| 对比项 | 数据库导出 (02) | 上传转换 (03) |
|--------|----------------|---------------|
| 触发方式 | GetFile（一次性消费任务 JSON） | ListFile + FetchFile（定时扫描 + 读取） |
| 源数据 | MySQL 查询结果 | 已上传的源文件（inbox_csv/json/tsv） |
| 输出目录 | output_csv / output_json / output_tsv | csv_to_json, csv_to_tsv, json_to_csv, json_to_tsv, tsv_to_csv, tsv_to_json |
| Worker 依赖 | pymysql | 标准库 csv / json（无外部依赖） |
| 处理器数量 | 2 | 3（多一个 FetchFile） |
| 任务目录 | export_jobs/inbox, done, error | convert_jobs/inbox, done, error |

> 使用 ListFile + FetchFile 而非 GetFile 的原因：源文件在 inbox_csv/json/tsv 而非任务 inbox，需要先通过任务 JSON 拿到源路径，再由 Worker 读取源文件。ListFile 持续扫描任务 inbox，任务完成后删除。

---

## 八、验收清单

- [ ] `convert_jobs/` 目录结构就位
- [ ] `*_to_*` 六个转换目录就位
- [ ] ListFile + FetchFile + ExecuteStreamCommand 三个处理器 RUNNING
- [ ] Worker 脚本部署到共享目录并 `chmod +x`
- [ ] 上传 CSV → 生成 csv_to_json/ 和 csv_to_tsv/ 输出
- [ ] 上传 JSON（NDJSON）→ 生成 json_to_csv/ 和 json_to_tsv/ 输出
- [ ] 上传 JSON（数组）→ 同上
- [ ] 上传 JSON（单对象）→ 同上
- [ ] 上传 TSV → 生成 tsv_to_csv/ 和 tsv_to_json/ 输出
- [ ] 原子写：输出文件完整可读
- [ ] done/error 状态文件正确写入
- [ ] 后端能扫描 result 并注册 fileId