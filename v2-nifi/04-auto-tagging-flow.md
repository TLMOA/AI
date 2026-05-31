# 04 — 自动打标方案（GetFile + ExecuteStreamCommand + Worker）

> 前提：已执行 `01-cleanup-old-flow.md` 和 `02-db-export-flow.md`。
>
> 核心思路：前端选择文件 + 填写打标规则后，后端将打标任务 JSON 写入共享目录，NiFi 消费后调用 Python Worker 完成打标并输出到 `tagged_output`。
>
> 依赖文件：
> - Worker: `v1-backend/scripts/nifi_auto_tagging_worker.py`（待创建）
> - Orchestrator: `v1-backend/app/nifi_orchestrator.py`

---

## 一、方案架构

```
GetFile (Root Group)
  扫描 /opt/nifi/.../tagging_jobs/inbox/*.json
    │
    ▼ (success)
ExecuteStreamCommand (Root Group)
    │
    │ 调用: python3 /opt/nifi/.../bin/nifi_auto_tagging_worker.py
    │ 传参: stdin ← 任务 JSON（含 sourceFile / tagConfig / targetFormat）
    │
    ▼
nifi_auto_tagging_worker.py
  ├── json.load(sys.stdin) → 读取打标任务参数
  ├── 读取源文件（CSV / JSON / TSV）
  ├── 应用打标规则
  │     tagType: "manual-table" | "auto-rule" | "ai-suggestion"
  │     tagConfig: { columns: [...], rules: [...], ... }
  ├── 生成打标产物
  │     格式：CSV / JSON / TSV（与源格式一致或按 targetFormat）
  │     命名：tag_{owner}_{source}_YYYYMMDD_HHMMSS.<ext>
  ├── 原子写：临时文件 → 校验 → 重命名
  └── 写状态文件：tagging_jobs/done|error/{jobId}.json
```

**2 个处理器，0 个 Controller Service**。与数据库导出一致的架构。

---

## 二、数据流（前端 → 后端 → NiFi → Worker）

```
┌──────────────────────────────────────────────────────────────────────┐
│  阶段 1：前端选择文件 + 配置打标规则                                    │
│                                                                      │
│  ┌──────────────────────────────────────────────────┐               │
│  │  打标表单                                          │               │
│  │                                                  │               │
│  │  选择文件:    [ export_owner_sensor_data_2026....csv ]  │               │
│  │  打标方式:    [ 手动打标 ▾ ]                      │               │
│  │                                                  │               │
│  │  打标规则:    列名  标签值  置信度                  │               │
│  │              ┌─────────────────────────┐         │               │
│  │              │ status  "正常"   高     │         │               │
│  │              │ type    "温度"   高     │         │               │
│  │              │ + 添加  [______] [__]  │         │               │
│  │              └─────────────────────────┘         │               │
│  │                                                  │               │
│  │  [ 执行打标 ]                                     │               │
│  └──────────────────────────────────────────────────┘               │
│                        │                                            │
│  POST /api/v1/tags/manual-table                                     │
│  { sourceFileId, tagType, tagConfig: {...} }                        │
│                        ▼                                            │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│  阶段 2：后端构建打标任务 JSON → 写入 tagging_jobs/inbox/             │
│                                                                      │
│  {                                                                   │
│    "jobId":       "tag_abc123",                                      │
│    "sourcePath":  "/opt/nifi/.../output_csv/export_owner_sensor_...",│
│    "sourceFormat": "CSV",                                            │
│    "tagType":     "manual-table",                                    │
│    "tagConfig": {                                                    │
│      "columns": ["status", "type"],                                  │
│      "mappings": {                                                   │
│        "row_rules": [                                                │
│          { "column": "status", "mapping": {"default": "正常"}},      │
│          { "column": "type",   "mapping": {"temperature":"温度"}}     │
│        ]                                                             │
│      }                                                               │
│    },                                                                │
│    "targetFormat": "CSV",                                            │
│    "fileName":    "export_owner_sensor_data_20260528_120000.csv",    │
│    "factoryId":   "factory-001",                                     │
│    "ownerId":     "admin"                                            │
│  }                                                                   │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│  阶段 3：NiFi → Worker 处理                                           │
│                                                                      │
│  GetFile → ExecuteStreamCommand                                      │
│         ↓                                                            │
│  nifi_auto_tagging_worker.py                                         │
│    ├── 读取 sourcePath 源文件                                         │
│    ├── 根据 tagConfig 逐行匹配/打标                                    │
│    ├── 生成 tagged 输出文件                                           │
│    └── 原子写 tagged_output/tag_admin_sensor_data_20260528_120000.csv       │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│  阶段 4：Worker 输出结果                                               │
│                                                                      │
│  ✓ 成功 → tagged_output/tag_owner_sensor_data_20260528_120000.csv   │
│          tagging_jobs/done/tag_abc123.json (SUCCEEDED)                │
│                                                                      │
│  ✗ 失败 → tagging_jobs/error/tag_abc123.json (FAILED)                │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 三、前置条件检查

### 3.1 目录创建

```bash
docker exec iot-nifi mkdir -p \
  /opt/nifi/nifi-current/data/iot/tagging_jobs/inbox \
  /opt/nifi/nifi-current/data/iot/tagging_jobs/done \
  /opt/nifi/nifi-current/data/iot/tagging_jobs/error \
  /opt/nifi/nifi-current/data/iot/tagged_output
```

### 3.2 Worker 脚本部署

```bash
cp /home/yhz/iot/v1-backend/scripts/nifi_auto_tagging_worker.py \
   /home/yhz/iot/real_nifi_data/bin/nifi_auto_tagging_worker.py
chmod +x /home/yhz/iot/real_nifi_data/bin/nifi_auto_tagging_worker.py

docker exec iot-nifi ls -la \
  /opt/nifi/nifi-current/data/iot/bin/nifi_auto_tagging_worker.py
```

### 3.3 其余前置条件

与 `02-mysql-export-new-flow.md` 第三章一致（容器运行、Admin 凭据有效、Python3 可用）。

> 打标 Worker 仅依赖 Python 标准库（csv / json），无需额外 pip 安装。

---

## 四、创建 NiFi Flow（Python API 一键部署）

```bash
cat > /tmp/deploy_tagging_flow.py << 'PYEOF'
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
        "name": "iot_auto_tagging_getfile_v1",
        "type": "org.apache.nifi.processors.standard.GetFile",
        "position": {"x": 320.0, "y": 400.0},
        "config": {"properties": {
            "Input Directory": f"{DATA}/tagging_jobs/inbox",
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
        "name": "iot_auto_tagging_command_v1",
        "type": "org.apache.nifi.processors.standard.ExecuteStreamCommand",
        "position": {"x": 760.0, "y": 400.0},
        "config": {
            "properties": {
                "Command Path": "python3",
                "Command Arguments": f"{DATA}/bin/nifi_auto_tagging_worker.py",
            },
            "autoTerminatedRelationships": [
                "output stream", "nonzero status", "original",
            ],
        }
    }
}).json()
cmd_id = cmd["component"]["id"]
print(f"   ExecuteStreamCommand ID: {cmd_id}")

print("3. 创建连接 GetFile.success → ExecuteStreamCommand...")
r = api("POST", f"/process-groups/{ROOT}/connections", json={
    "revision": {"version": 0},
    "component": {
        "source": {"id": gf_id, "type": "PROCESSOR", "groupId": ROOT},
        "destination": {"id": cmd_id, "type": "PROCESSOR", "groupId": ROOT},
        "selectedRelationships": ["success"],
        "backPressureDataSizeThreshold": "1 GB",
        "backPressureObjectThreshold": "10000",
    }
})
print(f"   HTTP {r.status_code}")

print("4. 启动两个处理器...")
for pid in [gf_id, cmd_id]:
    api("PUT", f"/processors/{pid}", json={
        "revision": {"version": 0},
        "component": {"id": pid, "state": "RUNNING"}
    })
    print(f"   START: {pid}")

print()
print("✓ 打标 Flow 创建并启动完成")
print(f"  GetFile:              {gf_id}")
print(f"  ExecuteStreamCommand: {cmd_id}")
PYEOF

python3 /tmp/deploy_tagging_flow.py
```

---

## 五、Worker 脚本关键逻辑（概述）

`nifi_auto_tagging_worker.py` 核心流程：

```python
def main():
    task = json.load(sys.stdin)

    source_path   = task["sourcePath"]
    source_format = task["sourceFormat"]
    tag_type      = task["tagType"]
    tag_config    = task["tagConfig"]
    target_format = task.get("targetFormat", source_format)
    file_name     = task["fileName"]
    job_id        = task["jobId"]

    # 1. 读取源文件
    rows = read_file(source_path, source_format)

    # 2. 根据打标方式应用规则
    if tag_type == "manual-table":
        tagged = apply_manual_table_tags(rows, tag_config)
    elif tag_type == "auto-rule":
        tagged = apply_auto_rules(rows, tag_config)
    elif tag_type == "ai-suggestion":
        tagged = apply_ai_suggestions(rows, tag_config)
    else:
        raise ValueError(f"unsupported tagType: {tag_type}")

    # 3. 写入 tagged_output
    output_path = os.path.join(
        TAGGED_OUTPUT_DIR,
        f"{file_name}_tagged_{datetime.now():%Y%m%d_%H%M%S}.{target_format.lower()}"
    )
    write_file_atomic(output_path, target_format, tagged)

    # 4. 写状态
    write_status("done", job_id, "SUCCEEDED", output_path)


def apply_manual_table_tags(rows, config):
    """手动打标：按 column→mapping 规则逐列应用标签"""
    for row in rows:
        for col, mapping in config.get("mappings", {}).get("row_rules", []):
            column_name = col if isinstance(col, str) else col["column"]
            rules = col if isinstance(col, dict) else {"mapping": mapping}
            # ... 应用 mapping: 值匹配 → 标签替换，否则 default
    return rows
```

**Worker 细节由开发阶段补齐**，包括：
- `read_file()` 复用 03 上传转换 Worker 的文件读取逻辑
- `apply_auto_rules()` 的规则引擎（正则匹配 / 值范围判断 / 条件表达式）
- `apply_ai_suggestions()` 预留 AI 接口（可调用外部 AI 服务）
- 大文件流式处理（逐行读、逐行写，避免内存溢出）

---

## 六、全链路测试

```bash
# 6.1 准备测试源文件（复用已有导出产物）
docker exec iot-nifi ls /opt/nifi/nifi-current/data/iot/output_csv/

# 6.2 投递打标任务 JSON
cat > /tmp/test_tag_task.json << 'JSONEOF'
{
  "jobId": "tag-test-001",
  "sourcePath": "/opt/nifi/nifi-current/data/iot/output_csv/export_admin_sensor_data_20260528_120000.csv",
  "sourceFormat": "CSV",
  "tagType": "manual-table",
  "tagConfig": {
    "columns": ["status"],
    "mappings": {
      "row_rules": [
        {
          "column": "status",
          "mapping": {"default": "已打标", "0": "正常", "1": "告警"}
        }
      ]
    }
  },
  "targetFormat": "CSV",
  "fileName": "export_admin_sensor_data_20260528_120000.csv",
  "factoryId": "factory-001",
  "ownerId": "admin"
}
JSONEOF

docker cp /tmp/test_tag_task.json \
  iot-nifi:/opt/nifi/nifi-current/data/iot/tagging_jobs/inbox/tag-test-001.json

# 6.3 等待 NiFi 处理
sleep 15

# 6.4 检查输出
echo "=== tagged_output ==="
docker exec iot-nifi ls -la /opt/nifi/nifi-current/data/iot/tagged_output/

echo "=== done ==="
docker exec iot-nifi cat /opt/nifi/nifi-current/data/iot/tagging_jobs/done/tag-test-001.json

# 6.5 验证标签是否正确应用
docker exec iot-nifi head -5 \
  /opt/nifi/nifi-current/data/iot/tagged_output/tag_admin_sensor_data_20260528_120000.csv
```

---

## 七、打标方式说明

| 打标方式 | tagType | 说明 | Worker 行为 |
|---------|---------|------|-------------|
| 手动打标 | `manual-table` | 用户在打标表单中逐列配置 `值→标签` 映射 | 遍历每行，按映射表替换值为标签 |
| 自动规则 | `auto-rule` | 预定义的规则集（正则/范围/条件表达式） | 逐行匹配规则，命中则打标 |
| AI 建议 | `ai-suggestion` | AI 模型推荐标签，用户确认后生效 | 调用外部 AI 服务，返回标签建议列表 |

---

## 八、验收清单

- [ ] `tagging_jobs/` 目录结构就位
- [ ] `tagged_output/` 目录就位
- [ ] GetFile + ExecuteStreamCommand 两个处理器 RUNNING
- [ ] Worker 脚本部署到共享目录并 `chmod +x`
- [ ] 手动打标：输入 CSV 源文件 + column→mapping → 生成 `{source}_tagged_*.csv`
- [ ] 手动打标：输入 JSON 源文件 → 生成 `{source}_tagged_*.json`
- [ ] 手动打标：输入 TSV 源文件 → 生成 `{source}_tagged_*.tsv`
- [ ] 自动规则打标：规则引擎正确应用
- [ ] 原子写：输出文件完整可读
- [ ] done/error 状态文件正确写入
- [ ] 后端能扫描 result 并注册 fileId

---

## 九、与其他 Flow 的关系

```
┌────────────────────────────────────────────────────────────────┐
│                    NiFi Root Group                               │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ 02 数据库导出: GetFile → ExecuteStreamCommand              │ │
│  │   inbox: export_jobs/inbox  │ 输出: output_csv/json/tsv    │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ 03 上传转换: ListFile → FetchFile → ExecuteStreamCommand   │ │
│  │   inbox: convert_jobs/inbox │ 输出: *_to_*                 │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ 04 自动打标: GetFile → ExecuteStreamCommand  ← 当前方案    │ │
│  │   inbox: tagging_jobs/inbox │ 输出: tagged_output           │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  注：打标源文件可以是 02 导出产物或 03 转换产物，均可跨 Flow 使用。│
│     三个 Flow 通过统一的 done/error 状态回调供后端轮询。         │
└────────────────────────────────────────────────────────────────┘
```