"""
NiFi 数据库导出 Flow 创建脚本
根据 V1-NiFi统一实施可执行方案 7.5.5 节创建完整流程
"""
import requests
import json
import sys
import urllib3

urllib3.disable_warnings()

NIFI_BASE = "https://localhost:8080/nifi-api"
USERNAME = "admin"
PASSWORD = "admin@nifi123"
BASE_DIR = "/opt/nifi/nifi-current/data/iot"

def get_token():
    r = requests.post(f"{NIFI_BASE}/access/token",
                      data={"username": USERNAME, "password": PASSWORD},
                      verify=False)
    r.raise_for_status()
    return r.text

def api(method, path, **kwargs):
    """统一的 NiFi API 调用"""
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {TOKEN}"
    kwargs.setdefault("verify", False)
    return requests.request(method, f"{NIFI_BASE}{path}", headers=headers, **kwargs)

TOKEN = get_token()
print(f"✓ Token 获取成功 ({TOKEN[:20]}...)")

# Step 1: 获取 Root PG ID
r = api("GET", "/flow/process-groups/root")
ROOT_PG_ID = r.json()["processGroupFlow"]["id"]
print(f"✓ Root PG: {ROOT_PG_ID}")

# ============================================================
# Step 2: 创建 DBCPConnectionPool Controller Service
# ============================================================
print("\n--- 创建 DBCPConnectionPool ---")

# 先获取可用的 controller service types
r = api("GET", f"/flow/controller-service-types")
types = {t["type"]: t for t in r.json()["controllerServiceTypes"]}

# 查找 DBCPConnectionPool 类型
dbcpp_type = None
for t in types:
    if "DBCPConnectionPool" in t and "DBCP" in t:
        dbcpp_type = t
        print(f"  找到类型: {t}")
        break

if not dbcpp_type:
    # 尝试查找 HikariCP 版本
    for t in types:
        if "Hikari" in t and "Connection" in t:
            dbcpp_type = t
            print(f"  找到类型(Hikari): {t}")
            break

if not dbcpp_type:
    print("ERROR: 找不到 DBCPConnectionPool 类型!")
    print("可用类型:", list(types.keys())[:10])
    sys.exit(1)

# 创建 controller service
cs_payload = {
    "revision": {"version": 0},
    "component": {
        "name": "MySQL-DBCPConnectionPool",
        "type": dbcpp_type,
        "properties": {
            "Database Connection URL": "jdbc:mysql://host.docker.internal:3306/nifi?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai",
            "Database Driver Class Name": "com.mysql.cj.jdbc.Driver",
            "Database Driver Location(s)": "/opt/nifi/nifi-current/lib/mysql-connector-j-8.4.0.jar",
            "Database User": "root",
            "Password": "root",
        }
    }
}

# Controller services 在 root process group 层级创建
r = api("POST", f"/process-groups/{ROOT_PG_ID}/controller-services",
        json=cs_payload)
if r.status_code == 201:
    cs_id = r.json()["component"]["id"]
    print(f"✓ DBCPConnectionPool 创建成功: {cs_id}")
else:
    print(f"ERROR 创建 CS: {r.status_code} {r.text[:200]}")
    sys.exit(1)

# Step 3: 创建 Process Group "iot-mysql-export"
print("\n--- 创建 Process Group ---")
pg_payload = {
    "revision": {"version": 0},
    "component": {
        "name": "iot-mysql-export",
        "position": {"x": 0, "y": 0}
    }
}
r = api("POST", f"/process-groups/{ROOT_PG_ID}/process-groups", json=pg_payload)
if r.status_code == 201:
    PG_ID = r.json()["id"]
    print(f"✓ Process Group 创建成功: {PG_ID}")
else:
    print(f"ERROR: {r.status_code} {r.text[:200]}")
    sys.exit(1)

# ============================================================
# Step 4: 创建所有处理器
# ============================================================
print("\n--- 创建处理器 ---")

def create_processor(pg_id, name, proc_type, properties, x, y):
    """创建单个处理器并返回其 ID"""
    payload = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "type": proc_type,
            "position": {"x": x, "y": y},
            "properties": properties
        }
    }
    r = api("POST", f"/process-groups/{pg_id}/processors", json=payload)
    if r.status_code == 201:
        pid = r.json()["id"]
        print(f"  ✓ {name}: {pid}")
        return pid
    else:
        print(f"  ✗ {name}: HTTP {r.status_code} {r.text[:200]}")
        return None

processor_ids = {}

# 1. ListFile - 监听 inbox 目录
pid = create_processor(PG_ID, "ListFile-inbox",
    "org.apache.nifi.processors.standard.ListFile",
    {
        "Input Directory": f"{BASE_DIR}/export_jobs/inbox",
        "File Filter": "[^\\.].*\\.json",
        "Minimum File Age": "1 sec",
    }, 0, 0)
if pid: processor_ids["ListFile"] = pid

# 2. FetchFile - 读取任务 JSON 内容
pid = create_processor(PG_ID, "FetchFile-job",
    "org.apache.nifi.processors.standard.FetchFile",
    {
        "File to Fetch": "${absolute.path}",
        "Completion Strategy": "Delete File",
    }, 300, 0)
if pid: processor_ids["FetchFile"] = pid

# 3. EvaluateJsonPath - 解析任务 JSON
pid = create_processor(PG_ID, "EvaluateJsonPath-parse",
    "org.apache.nifi.processors.standard.EvaluateJsonPath",
    {
        "jobId": "$.jobId",
        "host": "$.host",
        "port": "$.port",
        "database": "$.database",
        "table_name": "$.table",
        "where_clause": "$.where",
        "format": "$.format",
        "targetDir": "$.targetDir",
    }, 600, 0)
if pid: processor_ids["EvaluateJsonPath"] = pid

# 4. UpdateAttribute - 构建 SQL 查询
pid = create_processor(PG_ID, "UpdateAttribute-build-sql",
    "org.apache.nifi.processors.attributes.UpdateAttribute",
    {
        "db.host": "${host}",
        "db.port": "${port}",
        "db.database": "${database}",
        "sql.query": "SELECT * FROM ${table_name} WHERE ${where_clause:replace('__AND__',' AND '):replace('__OR__',' OR ')}",
    }, 900, 0)
if pid: processor_ids["UpdateAttribute"] = pid

# 5. ExecuteSQLRecord - 执行查询
pid = create_processor(PG_ID, "ExecuteSQLRecord-query",
    "org.apache.nifi.processors.standard.ExecuteSQLRecord",
    {
        "Database Connection Pooling Service": cs_id,
        "SQL select query": "${sql.query}",
        "Record Writer": "",  # 需要创建 JsonRecordSetWriter
        "Max Rows Per Flow File": "10000",
    }, 1200, 0)
if pid: processor_ids["ExecuteSQLRecord"] = pid

# 6. ConvertRecord - 格式转换
pid = create_processor(PG_ID, "ConvertRecord-format",
    "org.apache.nifi.processors.standard.ConvertRecord",
    {
        "Record Reader": "",  # 需要创建 JsonTreeReader
        "Record Writer": "",  # 需要创建对应格式的 Writer
    }, 1500, 0)
if pid: processor_ids["ConvertRecord"] = pid

# 7. UpdateAttribute-filename - 构建输出文件名
pid = create_processor(PG_ID, "UpdateAttribute-filename",
    "org.apache.nifi.processors.attributes.UpdateAttribute",
    {
        "filename": "${table_name}_export_${now():format('yyyyMMdd_HHmmss')}.${format}",
    }, 1800, 0)
if pid: processor_ids["UpdateFilename"] = pid

# 8. PutFile-output - 写入结果文件
pid = create_processor(PG_ID, "PutFile-output",
    "org.apache.nifi.processors.standard.PutFile",
    {
        "Directory": f"{BASE_DIR}/output_${{format}}",
        "Conflict Resolution Strategy": "replace",
        "Create Missing Directories": "true",
    }, 2100, 0)
if pid: processor_ids["PutFileOutput"] = pid

# 9. PutFile-done - 写入成功状态
pid = create_processor(PG_ID, "PutFile-done",
    "org.apache.nifi.processors.standard.PutFile",
    {
        "Directory": f"{BASE_DIR}/export_jobs/done",
        "Conflict Resolution Strategy": "replace",
    }, 2100, 200)
if pid: processor_ids["PutFileDone"] = pid

# 10. PutFile-error - 写入失败状态
pid = create_processor(PG_ID, "PutFile-error",
    "org.apache.nifi.processors.standard.PutFile",
    {
        "Directory": f"{BASE_DIR}/export_jobs/error",
        "Conflict Resolution Strategy": "replace",
    }, 300, 400)
if pid: processor_ids["PutFileError"] = pid

print(f"\n处理器创建完毕: {len(processor_ids)} 个")

# ============================================================
# Step 5: 创建连接（连线）
# ============================================================
print("\n--- 创建连接 ---")

def create_connection(pg_id, source_id, dest_id, relationships, name=""):
    """创建处理器之间的连接"""
    payload = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "source": {"id": source_id, "type": "PROCESSOR"},
            "destination": {"id": dest_id, "type": "PROCESSOR"},
            "selectedRelationships": relationships,
        }
    }
    r = api("POST", f"/process-groups/{pg_id}/connections", json=payload)
    if r.status_code == 201:
        print(f"  ✓ {name or f'{source_id[:8]}->{dest_id[:8]}'}")
        return r.json()["id"]
    else:
        print(f"  ✗ {name}: HTTP {r.status_code} {r.text[:200]}")
        return None

# ListFile -> FetchFile (success)
if "ListFile" in processor_ids and "FetchFile" in processor_ids:
    create_connection(PG_ID, processor_ids["ListFile"], processor_ids["FetchFile"],
                      ["success"], "ListFile→FetchFile")

# FetchFile -> EvaluateJsonPath (success)
if "FetchFile" in processor_ids and "EvaluateJsonPath" in processor_ids:
    create_connection(PG_ID, processor_ids["FetchFile"], processor_ids["EvaluateJsonPath"],
                      ["success"], "FetchFile→EvaluateJsonPath")
    # FetchFile 失败 → error
    if "PutFileError" in processor_ids:
        create_connection(PG_ID, processor_ids["FetchFile"], processor_ids["PutFileError"],
                          ["failure"], "FetchFile→Error")

# EvaluateJsonPath -> UpdateAttribute (matched/success)
if "EvaluateJsonPath" in processor_ids and "UpdateAttribute" in processor_ids:
    create_connection(PG_ID, processor_ids["EvaluateJsonPath"], processor_ids["UpdateAttribute"],
                      ["matched"], "EvaluateJsonPath→UpdateAttribute")
    if "PutFileError" in processor_ids:
        create_connection(PG_ID, processor_ids["EvaluateJsonPath"], processor_ids["PutFileError"],
                          ["unmatched"], "EvaluateJsonPath→Error")

# UpdateAttribute -> ExecuteSQLRecord (success)
if "UpdateAttribute" in processor_ids and "ExecuteSQLRecord" in processor_ids:
    create_connection(PG_ID, processor_ids["UpdateAttribute"], processor_ids["ExecuteSQLRecord"],
                      ["success"], "UpdateAttribute→ExecuteSQLRecord")

# ExecuteSQLRecord -> ConvertRecord (success)
if "ExecuteSQLRecord" in processor_ids and "ConvertRecord" in processor_ids:
    create_connection(PG_ID, processor_ids["ExecuteSQLRecord"], processor_ids["ConvertRecord"],
                      ["success"], "ExecuteSQLRecord→ConvertRecord")
    if "PutFileError" in processor_ids:
        create_connection(PG_ID, processor_ids["ExecuteSQLRecord"], processor_ids["PutFileError"],
                          ["failure"], "ExecuteSQLRecord→Error")

# ConvertRecord -> UpdateFilename (success)
if "ConvertRecord" in processor_ids and "UpdateFilename" in processor_ids:
    create_connection(PG_ID, processor_ids["ConvertRecord"], processor_ids["UpdateFilename"],
                      ["success"], "ConvertRecord→UpdateFilename")
    if "PutFileError" in processor_ids:
        create_connection(PG_ID, processor_ids["ConvertRecord"], processor_ids["PutFileError"],
                          ["failure"], "ConvertRecord→Error")

# UpdateFilename -> PutFileOutput (success)
if "UpdateFilename" in processor_ids and "PutFileOutput" in processor_ids:
    create_connection(PG_ID, processor_ids["UpdateFilename"], processor_ids["PutFileOutput"],
                      ["success"], "UpdateFilename→PutFileOutput")

# PutFileOutput -> PutFileDone (success)
if "PutFileOutput" in processor_ids and "PutFileDone" in processor_ids:
    create_connection(PG_ID, processor_ids["PutFileOutput"], processor_ids["PutFileDone"],
                      ["success"], "PutFileOutput→PutFileDone")
    if "PutFileError" in processor_ids:
        create_connection(PG_ID, processor_ids["PutFileOutput"], processor_ids["PutFileError"],
                          ["failure"], "PutFileOutput→Error")

print("\n连接创建完毕")

# ============================================================
# Step 6: 输出结果
# ============================================================
print("\n" + "=" * 60)
print("Flow 创建完成!")
print(f"  Process Group ID: {PG_ID}")
print(f"  DBCPConnectionPool ID: {cs_id}")
print(f"  处理器数量: {len(processor_ids)}")
print(f"  NiFi 访问: https://localhost:8080/nifi")
print(f"  用户名: {USERNAME}")
print(f"  密码: {PASSWORD}")
print("=" * 60)
print("\n下一步: 需要在 NiFi UI 中手动完成以下配置:")
print("  1. 启用 DBCPConnectionPool Controller Service")
print("  2. 为 ExecuteSQLRecord 创建 JsonRecordSetWriter 并配置")
print("  3. 为 ConvertRecord 创建 JsonTreeReader 和对应格式的 RecordWriter")
print("  4. 启动 Process Group 中所有处理器")