#!/usr/bin/env python3
"""清理旧 iot processor + 重新部署 3 个 flow + 设置 NIFI_REAL_EXECUTION=true 重启后端。"""
import json, os, sys, time
import requests
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

sys.path.insert(0, "/home/yhz/iot/v1-backend")

# === NiFi API 认证 ===
def nifi_auth():
    s = requests.Session()
    r = s.post('https://localhost:8080/nifi-api/access/token',
        data='username=admin&password=admin@nifi123',
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        verify=False)
    if r.status_code != 201:
        raise RuntimeError(f"NiFi login failed: {r.status_code}")
    token = r.text.strip()
    s.headers.update({'Authorization': f'Bearer {token}'})
    return s

s = nifi_auth()

# === 1. 获取所有 iot 相关 processor 并停止/删除 ===
r = s.get('https://localhost:8080/nifi-api/process-groups/root/processors', verify=False)
procs = r.json().get('processors', [])
iot_procs = [p for p in procs if 'iot' in p['component']['name'].lower()]

print(f"找到 {len(iot_procs)} 个 iot processor: {[p['component']['name'] for p in iot_procs]}")

# 停止所有
for p in iot_procs:
    pid = p['id']
    status = p['status']['runStatus']
    print(f"  停止 {p['component']['name']} (当前 {status})...")
    if status != 'Stopped':
        r2 = s.put(f'https://localhost:8080/nifi-api/processors/{pid}/run-status',
            json={'revision': p['revision'], 'state': 'STOPPED'}, verify=False)
        if r2.status_code not in (200, 201):
            # 刷新 revision 重试
            r_refresh = s.get(f'https://localhost:8080/nifi-api/processors/{pid}', verify=False)
            p_refresh = r_refresh.json()
            r2 = s.put(f'https://localhost:8080/nifi-api/processors/{pid}/run-status',
                json={'revision': p_refresh['revision'], 'state': 'STOPPED'}, verify=False)
        print(f"    → {r2.status_code}")

# 删除所有
for p in iot_procs:
    pid = p['id']
    name = p['component']['name']
    print(f"  删除 {name}...")
    # 刷新 revision
    r_refresh = s.get(f'https://localhost:8080/nifi-api/processors/{pid}', verify=False)
    if r_refresh.status_code == 200:
        rev = r_refresh.json()['revision']
        r2 = s.delete(f'https://localhost:8080/nifi-api/processors/{pid}?version={rev["version"]}',
            verify=False)
        print(f"    → {r2.status_code}")

# === 2. 删除所有 connection ===
r = s.get('https://localhost:8080/nifi-api/process-groups/root/connections', verify=False)
conns = r.json().get('connections', [])
print(f"\n找到 {len(conns)} 个 connection")
for c in conns:
    cid = c['id']
    cname = c['component'].get('name', cid[:8])
    print(f"  删除 {cname}...")
    rev = c['revision']
    r2 = s.delete(f'https://localhost:8080/nifi-api/connections/{cid}?version={rev["version"]}',
        verify=False)
    print(f"    → {r2.status_code}")

# === 3. 删除旧的 flow marker（强制重新部署） ===
marker_path = '/home/yhz/real_nifi_data/export_jobs/.iot_mysql_export_flow_v1.ready.json'
if os.path.exists(marker_path):
    os.remove(marker_path)
    print(f"\n删除旧 marker: {marker_path}")

# === 4. 设置环境变量并部署 ===
os.environ.setdefault("NIFI_AUTO_DEPLOY_FLOW", "true")
os.environ.setdefault("NIFI_CONTAINER_NAME", "iot-nifi")
os.environ.setdefault("NIFI_HTTP_PORT", "8080")
os.environ.setdefault("NIFI_API_BASE", "https://localhost:8080/nifi-api")
os.environ.setdefault("NIFI_ADMIN_USER", "admin")
os.environ.setdefault("NIFI_ADMIN_PASSWORD", "admin@nifi123")
os.environ.setdefault("NIFI_REAL_BASE_DIR", "/home/yhz/real_nifi_data")

print("\n=== 部署 flow ===")
from app.nifi_orchestrator import ensure_nifi_ready_for_all_flows

result = ensure_nifi_ready_for_all_flows()
print(json.dumps(result, ensure_ascii=False, indent=2, default=str))

if result.get("ok"):
    print("\n✅ 部署成功!")
else:
    print(f"\n❌ 部署失败: {result.get('error')}")
    sys.exit(1)
