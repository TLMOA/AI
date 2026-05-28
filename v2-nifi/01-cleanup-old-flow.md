# 01 — 清理旧方案遗留（ExecuteSQLRecord + DBCP）

> 目的：彻底删除之前建立的错误 Flow，还原 NiFi 到干净状态，为正确方案铺路。
>
> 当前状态（`2026-05-28` 实测）：
> - `iot-mysql-export` Process Group：10 个处理器 + 13 个 Controller Service
> - Root 层 DBCPConnectionPool：1 个
> - Root 层残留处理器：`AttributeRollingWindow`（STOPPED）
> - Admin 凭据：`admin / admin@nifi123`（未变更）

---

## 一、确认当前状态

执行以下命令，了解当前需要清理的内容：

```bash
TOKEN=$(curl -sk -X POST https://localhost:8080/nifi-api/access/token \
  -d "username=admin&password=admin@nifi123")

echo "=== 1. Root Process Groups ==="
curl -sk "https://localhost:8080/nifi-api/process-groups/root/process-groups" \
  -H "Authorization: Bearer $TOKEN" | python3 -c "
import json,sys
for pg in json.load(sys.stdin).get('processGroups',[]):
    c=pg['component']
    print(f\"  {c['name']} | id={pg['id']} | running={c['runningCount']} stopped={c['stoppedCount']}\")
"

echo "=== 2. Root Controller Services ==="
curl -sk "https://localhost:8080/nifi-api/flow/process-groups/root/controller-services" \
  -H "Authorization: Bearer $TOKEN" | python3 -c "
import json,sys
for cs in json.load(sys.stdin).get('controllerServices',[]):
    c=cs['component']
    print(f\"  {c['name']} | type={c['type'].split('.')[-1]} | state={c['state']}\")
"

echo "=== 3. Root Processors ==="
curl -sk "https://localhost:8080/nifi-api/process-groups/root/processors" \
  -H "Authorization: Bearer $TOKEN" | python3 -c "
import json,sys
for p in json.load(sys.stdin).get('processors',[]):
    c=p['component']
    print(f\"  {c.get('name','')} | type={c['type'].split('.')[-1]} | state={c['state']}\")
"
```

预期看到的需清理项：
- Process Group: `iot-mysql-export`（含 10 个处理器）
- Root CS: `MySQL-DBCPConnectionPool` 等 DBCP 相关
- Root Processor: `AttributeRollingWindow`（如有旧版遗留）

---

## 二、确认 Admin 凭据正确

```bash
# 验证 admin 凭据
curl -sk -X POST https://localhost:8080/nifi-api/access/token \
  -d "username=admin&password=admin@nifi123" -w "\nHTTP %{http_code}\n"

# 预期：返回 JWT Token + HTTP 201
```

### 如果凭据被改过，还原方式

如果 `admin@nifi123` 认证失败，进入容器重置：

```bash
# 进入容器
docker exec -it iot-nifi bash

# 找到并检查 users.xml
ls -la /opt/nifi/nifi-current/conf/users.xml
ls -la /opt/nifi/nifi-current/conf/authorizations.xml

# 如果 users.xml 中 admin 密码已被哈希修改，最简单的做法是：
# 1. 停止容器
docker stop iot-nifi

# 2. 清空 NiFi 安全配置（重启后重新生成初始 admin）
docker run --rm -v nifi-conf-backup:/backup \
  alpine cp -a /opt/nifi/nifi-current/conf /backup/conf-$(date +%Y%m%d%H%M%S) 2>/dev/null

# 或者直接重置 NiFi 的安全数据库：
docker exec iot-nifi bash -c '
  cd /opt/nifi/nifi-current/conf
  # 备份当前配置
  cp users.xml users.xml.bak.$(date +%Y%m%d)
  cp authorizations.xml authorizations.xml.bak.$(date +%Y%m%d)
  # 重置为初始状态
  rm -f users.xml authorizations.xml
'

# 3. 重启容器
docker restart iot-nifi

# 4. 等待启动后用初始凭据登录（admin / 随机密码，或重新设置）
```

---

## 三、执行清理

### 3.1 清理 Process Group `iot-mysql-export`

PG ID: `6c3473b4-019e-1000-a4b5-0f1da5c08a64`

```bash
TOKEN=$(curl -sk -X POST https://localhost:8080/nifi-api/access/token \
  -d "username=admin&password=admin@nifi123")

PG_ID="6c3473b4-019e-1000-a4b5-0f1da5c08a64"

# ---- 3.1.1 停止 PG 内所有处理器 ----
PROC_IDS=$(curl -sk "https://localhost:8080/nifi-api/process-groups/${PG_ID}/processors" \
  -H "Authorization: Bearer $TOKEN" | \
  python3 -c "
import json,sys
items=json.load(sys.stdin).get('processors',[])
for p in items:
    c=p['component']
    print(f\"{p['id']}|{p['revision']['version']}|{c.get('name','')}|{c['state']}\")
")

echo "停止以下处理器:"
echo "$PROC_IDS"

while IFS='|' read -r pid ver name state; do
  if [ "$state" = "RUNNING" ]; then
    echo "  停止 $name ($pid)..."
    curl -sk -X PUT "https://localhost:8080/nifi-api/processors/${pid}" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "{\"revision\":{\"version\":${ver}},\"component\":{\"id\":\"${pid}\",\"state\":\"STOPPED\"}}" \
      -w " HTTP %{http_code}\n"
  fi
done <<< "$PROC_IDS"

# ---- 3.1.2 等待停止完成 ----
sleep 5

# ---- 3.1.3 停止并删除 PG 内的 Controller Services ----
CS_IDS=$(curl -sk "https://localhost:8080/nifi-api/flow/process-groups/${PG_ID}/controller-services" \
  -H "Authorization: Bearer $TOKEN" | \
  python3 -c "
import json,sys
for cs in json.load(sys.stdin).get('controllerServices',[]):
    c=cs['component']
    print(f\"{cs['id']}|{cs['revision']['version']}|{c.get('name','')}|{c['state']}\")
")

echo "停止并删除以下 Controller Services:"
echo "$CS_IDS"

while IFS='|' read -r csid ver name state; do
  if [ "$state" = "ENABLED" ] || [ "$state" = "ENABLING" ]; then
    echo "  停止 $name ($csid)..."
    curl -sk -X PUT "https://localhost:8080/nifi-api/controller-services/${csid}" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "{\"revision\":{\"version\":${ver}},\"component\":{\"id\":\"${csid}\",\"state\":\"DISABLED\"}}" \
      -w " HTTP %{http_code}\n"
  fi
done <<< "$CS_IDS"

sleep 5

while IFS='|' read -r csid ver name state; do
  echo "  删除 $name ($csid)..."
  curl -sk -X DELETE "https://localhost:8080/nifi-api/controller-services/${csid}?version=${ver}&clientId=cleanup" \
    -H "Authorization: Bearer $TOKEN" \
    -w " HTTP %{http_code}\n"
done <<< "$CS_IDS"

# ---- 3.1.4 获取 PG revision 并删除 ----
PG_REV=$(curl -sk "https://localhost:8080/nifi-api/process-groups/${PG_ID}" \
  -H "Authorization: Bearer $TOKEN" | \
  python3 -c "import json,sys;print(json.load(sys.stdin)['revision']['version'])")

echo "删除 Process Group (revision: ${PG_REV})..."
curl -sk -X DELETE "https://localhost:8080/nifi-api/process-groups/${PG_ID}?version=${PG_REV}&clientId=cleanup" \
  -H "Authorization: Bearer $TOKEN" \
  -w "\nHTTP %{http_code}\n"

# 预期：HTTP 200
```

### 3.2 清理 Root 层 Controller Service

```bash
TOKEN=$(curl -sk -X POST https://localhost:8080/nifi-api/access/token \
  -d "username=admin&password=admin@nifi123")

# ---- 3.2.1 列出所有 root CS ----
echo "Root 层 Controller Services:"
curl -sk "https://localhost:8080/nifi-api/flow/process-groups/root/controller-services" \
  -H "Authorization: Bearer $TOKEN" | python3 -c "
import json,sys
for cs in json.load(sys.stdin).get('controllerServices',[]):
    c=cs['component']
    print(f\"  {c['name']} | id={cs['id']} | state={c['state']}\")
"

# ---- 3.2.2 停止并删除 DBCP-related CS ----
# 已知需要清理的 CS ID（当前环境）：
CS_LIST="6c347351-019e-1000-4281-6d781e5284c2"   # MySQL-DBCPConnectionPool

for CS_ID in $CS_LIST; do
  echo "--- 处理 CS: $CS_ID ---"

  # 获取 revision
  CS_REV=$(curl -sk "https://localhost:8080/nifi-api/controller-services/${CS_ID}" \
    -H "Authorization: Bearer $TOKEN" | \
    python3 -c "import json,sys;print(json.load(sys.stdin)['revision']['version'])" 2>/dev/null)

  if [ -n "$CS_REV" ]; then
    # 停止
    curl -sk -X PUT "https://localhost:8080/nifi-api/controller-services/${CS_ID}" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "{\"revision\":{\"version\":${CS_REV}},\"component\":{\"id\":\"${CS_ID}\",\"state\":\"DISABLED\"}}" \
      -w "\nSTOP HTTP %{http_code}\n"

    sleep 3

    # 删除
    curl -sk -X DELETE "https://localhost:8080/nifi-api/controller-services/${CS_ID}?version=${CS_REV}&clientId=cleanup" \
      -H "Authorization: Bearer $TOKEN" \
      -w "\nDELETE HTTP %{http_code}\n"
  fi
done
```

### 3.3 清理 Root 层残留处理器

```bash
TOKEN=$(curl -sk -X POST https://localhost:8080/nifi-api/access/token \
  -d "username=admin&password=admin@nifi123")

# AttributeRollingWindow（旧版遗留）
PROC_ID="6c3473b4-019e-1000-ec1a-7ac41cbe19b0"  # 需确认真实 ID

# 获取 revision
PROC_REV=$(curl -sk "https://localhost:8080/nifi-api/processors/${PROC_ID}" \
  -H "Authorization: Bearer $TOKEN" | \
  python3 -c "import json,sys;print(json.load(sys.stdin)['revision']['version'])" 2>/dev/null)

if [ -n "$PROC_REV" ]; then
  # 停止
  curl -sk -X PUT "https://localhost:8080/nifi-api/processors/${PROC_ID}" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"revision\":{\"version\":${PROC_REV}},\"component\":{\"id\":\"${PROC_ID}\",\"state\":\"STOPPED\"}}"

  sleep 2

  # 删除
  curl -sk -X DELETE "https://localhost:8080/nifi-api/processors/${PROC_ID}?version=${PROC_REV}&clientId=cleanup" \
    -H "Authorization: Bearer $TOKEN" \
    -w "\nDELETE HTTP %{http_code}\n"
fi
```

---

## 四、Python 一键清理脚本（推荐）

如果上述手动步骤太繁琐，使用此脚本一键完成全部清理：

```bash
cat > /tmp/cleanup_all.py << 'PYEOF'
import requests, urllib3, time, json, sys
urllib3.disable_warnings()

NIFI = "https://localhost:8080/nifi-api"
ADMIN = {"username": "admin", "password": "admin@nifi123"}

tok = requests.post(f"{NIFI}/access/token", data=ADMIN, verify=False).text
if len(tok) < 20:
    print("认证失败，请检查 admin 凭据")
    sys.exit(1)

def api(method, path, **kw):
    h = kw.pop("headers", {})
    h["Authorization"] = f"Bearer {tok}"
    return requests.request(method, f"{NIFI}{path}", headers=h, verify=False, **kw)

def stop_all_processors(pg_id):
    """停止 Process Group 内所有 RUNNING 处理器"""
    r = api("GET", f"/process-groups/{pg_id}/processors")
    if r.status_code != 200:
        print(f"  获取处理器失败: {r.status_code}")
        return
    for p in r.json().get("processors", []):
        c = p["component"]
        if c["state"] == "RUNNING":
            api("PUT", f"/processors/{p['id']}", json={
                "revision": p["revision"],
                "component": {"id": p["id"], "state": "STOPPED"}
            })
            print(f"  STOP: {c['name']}")

def disable_and_delete_cs(pg_id):
    """停止并删除 Process Group 内所有 Controller Service"""
    r = api("GET", f"/flow/process-groups/{pg_id}/controller-services")
    if r.status_code != 200:
        return
    for cs in r.json().get("controllerServices", []):
        c = cs["component"]
        rev = cs["revision"]
        # 先停止
        if c["state"] in ("ENABLED", "ENABLING"):
            api("PUT", f"/controller-services/{cs['id']}", json={
                "revision": rev,
                "component": {"id": cs["id"], "state": "DISABLED"}
            })
            print(f"  DISABLE CS: {c['name']}")
    time.sleep(5)
    # 再删除
    for cs in r.json().get("controllerServices", []):
        c = cs["component"]
        api("DELETE", f"/controller-services/{cs['id']}?version={cs['revision']['version']}&clientId=cleanup")
        print(f"  DELETE CS: {c['name']}")

def delete_root_cs():
    """清理 Root 层 Controller Services"""
    r = api("GET", "/flow/process-groups/root/controller-services")
    if r.status_code != 200:
        return
    for cs in r.json().get("controllerServices", []):
        c = cs["component"]
        rev = cs["revision"]
        if c["state"] in ("ENABLED", "ENABLING"):
            api("PUT", f"/controller-services/{cs['id']}", json={
                "revision": rev,
                "component": {"id": cs["id"], "state": "DISABLED"}
            })
            time.sleep(2)
        api("DELETE", f"/controller-services/{cs['id']}?version={rev['version']}&clientId=cleanup")
        print(f"  DELETE ROOT CS: {c['name']}")

def delete_root_processors():
    """清理 Root 层遗留处理器"""
    r = api("GET", "/process-groups/root/processors")
    if r.status_code != 200:
        return
    for p in r.json().get("processors", []):
        api("PUT", f"/processors/{p['id']}", json={
            "revision": p["revision"],
            "component": {"id": p["id"], "state": "STOPPED"}
        })
        time.sleep(2)
        api("DELETE", f"/processors/{p['id']}?version={p['revision']['version']}&clientId=cleanup")
        print(f"  DELETE ROOT PROC: {p['component'].get('name', '')}")

def delete_process_groups():
    """删除所有子 Process Group"""
    r = api("GET", "/process-groups/root/process-groups")
    for pg in r.json().get("processGroups", []):
        pg_id = pg["id"]
        name = pg["component"]["name"]
        print(f"\n清理 Process Group: {name} ({pg_id})")
        stop_all_processors(pg_id)
        time.sleep(3)
        disable_and_delete_cs(pg_id)
        time.sleep(3)
        rev = api("GET", f"/process-groups/{pg_id}").json()["revision"]["version"]
        resp = api("DELETE", f"/process-groups/{pg_id}?version={rev}&clientId=cleanup")
        print(f"  DELETE PG {name}: HTTP {resp.status_code}")

# ---- 执行 ----
print("=" * 50)
print("开始清理 NiFi 旧 Flow")
print("=" * 50)

print("\n1. 删除子 Process Groups...")
delete_process_groups()

print("\n2. 删除 Root 层 Controller Services...")
delete_root_cs()

print("\n3. 删除 Root 层遗留处理器...")
delete_root_processors()

print("\n" + "=" * 50)
print("清理完成！验证中...")
print("=" * 50)

# 验证
pg_r = api("GET", "/process-groups/root/process-groups")
pg_count = len(pg_r.json().get("processGroups", []))
cs_r = api("GET", "/flow/process-groups/root/controller-services")
cs_count = len(cs_r.json().get("controllerServices", []))
proc_r = api("GET", "/process-groups/root/processors")
proc_count = len(proc_r.json().get("processors", []))

print(f"  Process Groups: {pg_count}")
print(f"  Root CS: {cs_count}")
print(f"  Root Processors: {proc_count}")

if pg_count == 0 and cs_count == 0 and proc_count == 0:
    print("\n✓ NiFi 已还原到干净状态")
else:
    print("\n⚠ 仍有残留，请手动检查")
PYEOF

python3 /tmp/cleanup_all.py
```

---

## 五、清理宿主机目录

```bash
# 只清理任务残留文件，保留目录结构
rm -f /home/yhz/iot/real_nifi_data/export_jobs/inbox/*.json
rm -f /home/yhz/iot/real_nifi_data/export_jobs/done/*.json
rm -f /home/yhz/iot/real_nifi_data/export_jobs/error/*.json
rm -f /home/yhz/iot/real_nifi_data/output_csv/*.{csv,json,tsv}
rm -f /home/yhz/iot/real_nifi_data/output_json/*.{csv,json,tsv}
rm -f /home/yhz/iot/real_nifi_data/output_tsv/*.{csv,json,tsv}

# 清理旧的部署标记文件（如有）
rm -f /home/yhz/iot/real_nifi_data/export_jobs/.iot_mysql_export_flow_v1.ready.json
rm -f /home/yhz/iot/real_nifi_data/export_jobs/nifi_mysql_export_flow.md

echo "宿主机目录已清理"
```

---

## 六、清理完成确认

```bash
TOKEN=$(curl -sk -X POST https://localhost:8080/nifi-api/access/token \
  -d "username=admin&password=admin@nifi123")

echo "=== 验收：NiFi 应处于空白状态 ==="

echo "--- Process Groups ---"
curl -sk "https://localhost:8080/nifi-api/process-groups/root/process-groups" \
  -H "Authorization: Bearer $TOKEN" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(f\"  Count: {len(d.get('processGroups',[]))}\")
for pg in d.get('processGroups',[]):
    print(f\"  → {pg['component']['name']}\")
"

echo "--- Root Controller Services ---"
curl -sk "https://localhost:8080/nifi-api/flow/process-groups/root/controller-services" \
  -H "Authorization: Bearer $TOKEN" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(f\"  Count: {len(d.get('controllerServices',[]))}\")
for cs in d.get('controllerServices',[]):
    print(f\"  → {cs['component']['name']}\")
"

echo "--- Root Processors ---"
curl -sk "https://localhost:8080/nifi-api/process-groups/root/processors" \
  -H "Authorization: Bearer $TOKEN" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(f\"  Count: {len(d.get('processors',[]))}\")
for p in d.get('processors',[]):
    print(f\"  → {p['component'].get('name','?')}\")
"

echo "--- Admin 凭据 ---"
curl -sk -X POST https://localhost:8080/nifi-api/access/token \
  -d "username=admin&password=admin@nifi123" -w "\n  HTTP %{http_code}\n" | head -c 30
echo ""
```

全部输出 `Count: 0`，Admin 凭据返回 `HTTP 201`，即为清理成功。

---

## 七、清理对象清单

| 清理对象 | 位置 | 数量 |
|---------|------|------|
| `iot-mysql-export` Process Group | Root Group | 1 个 |
| PG 内处理器 (ListFile/FetchFile/ExecuteSQLRecord 等) | PG 内 | 10 个 |
| PG 内 ControllerService (DBCP ×4 + RecordWriter ×5 + Reader) | PG 内 | 13 个 |
| Root DBCPConnectionPool | Root Controller Services | 1 个 |
| Root 残留处理器 (AttributeRollingWindow) | Root Processors | 1 个 |
| 宿主机任务残留文件 | real_nifi_data/export_jobs/ | 0 个（已空） |
| 宿主机输出残留文件 | real_nifi_data/output_*/ | 0 个（已空） |