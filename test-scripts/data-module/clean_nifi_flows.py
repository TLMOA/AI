#!/usr/bin/env python3
"""第一步：清理 NiFi 根组中所有旧的 iot processor 和 connection。"""
import requests
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def nifi_auth():
    s = requests.Session()
    r = s.post('https://localhost:8080/nifi-api/access/token',
        data='username=admin&password=admin@nifi123',
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        verify=False)
    if r.status_code != 201:
        raise RuntimeError(f"NiFi login failed: {r.status_code}")
    s.headers.update({'Authorization': f'Bearer {r.text.strip()}'})
    return s

s = nifi_auth()

# 获取所有 processor，停止 + 删除 iot 开头的
r = s.get('https://localhost:8080/nifi-api/process-groups/root/processors', verify=False)
procs = r.json().get('processors', [])
iot_procs = [p for p in procs if 'iot' in p['component']['name'].lower()]
print(f"找到 {len(iot_procs)} 个 iot processor")

for p in iot_procs:
    pid = p['id']; name = p['component']['name']
    # 停止
    rev = s.get('https://localhost:8080/nifi-api/processors/' + pid, verify=False).json()['revision']
    r2 = s.put(f'https://localhost:8080/nifi-api/processors/{pid}/run-status',
        json={'revision': rev, 'state': 'STOPPED'}, verify=False)
    print(f"  停止 {name}: {r2.status_code}")

for p in iot_procs:
    pid = p['id']; name = p['component']['name']
    rev = s.get('https://localhost:8080/nifi-api/processors/' + pid, verify=False).json()['revision']
    url = 'https://localhost:8080/nifi-api/processors/' + pid + '?version=' + str(rev['version'])
    r2 = s.delete(url, verify=False)
    print(f"  删除 {name}: {r2.status_code}")

# 删除所有 connection
r = s.get('https://localhost:8080/nifi-api/process-groups/root/connections', verify=False)
conns = r.json().get('connections', [])
print(f"\n找到 {len(conns)} 个 connection")
for c in conns:
    cid = c['id']; cname = c['component'].get('name', cid[:8])
    rev = c['revision']
    url = 'https://localhost:8080/nifi-api/connections/' + cid + '?version=' + str(rev['version'])
    r2 = s.delete(url, verify=False)
    print(f"  删除 {cname}: {r2.status_code}")

# 删除旧 marker
import os
marker = '/home/yhz/real_nifi_data/export_jobs/.iot_mysql_export_flow_v1.ready.json'
if os.path.exists(marker):
    os.remove(marker)
    print(f"\n删除旧 marker: {marker}")

print("\n✅ 清理完成")
