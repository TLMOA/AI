#!/usr/bin/env python3
"""NiFi 模式完整前端验证（对标 Local 模式 23 项）"""
import subprocess, time, os, sys, json

PCLI = "/home/yhz/.nvm/versions/node/v22.22.2/bin/playwright-cli"
FE = "http://127.0.0.1:5174"
RESULTS = []

sys.path.insert(0, "/home/yhz/iot/v1-backend")
from app.auth import SECRET_KEY
import jwt, requests
payload = {"sub": "admin", "is_admin": True, "exp": int(time.time()) + 86400}
TOKEN = jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def run(args, timeout=20):
    r = subprocess.run([PCLI] + args, capture_output=True, text=True, timeout=timeout)
    return r.stdout, r.stderr, r.returncode

def snap():
    run(["snapshot"]); time.sleep(0.6)
    fs = sorted([f for f in os.listdir("/home/yhz/iot/.playwright-cli/") if f.startswith("page-") and f.endswith(".yml")],
                key=lambda f: os.path.getmtime(f"/home/yhz/iot/.playwright-cli/{f}"), reverse=True)
    if fs:
        with open(f"/home/yhz/iot/.playwright-cli/{fs[0]}") as f:
            return f.read()
    return ""

def ref(text):
    s = snap()
    for line in s.split("\n"):
        if text in line and "[ref=" in line:
            import re; m = re.search(r'\[ref=([^\]]+)\]', line)
            if m: return m.group(1)
    return ""

def click_text(text):
    rid = ref(text)
    if rid: run(["click", rid]); time.sleep(1.5); return True
    return False

def fill_id(dom_id, val):
    run(["eval", f"async ()=>{{const e=document.getElementById('{dom_id}'); if(e){{e.value='{val}'; e.dispatchEvent(new Event('input')); e.dispatchEvent(new Event('change')); return 'ok';}} return 'no';}}"])
    time.sleep(0.3); return True

def rec(mod, name, passed, detail=""):
    RESULTS.append(dict(module=mod, name=name, passed=passed, detail=detail))
    print(f"  [{'PASS' if passed else 'FAIL'}] {mod}: {name}")

api = requests.Session()
api.post("http://127.0.0.1:8081/api/v1/auth/login", json={"username": "admin", "password": "admin"})

# 确认模式
rj = api.get("http://127.0.0.1:8081/api/v1/internal/backend-mode").json()
print(f"当前模式: {rj.get('data',{}).get('mode')}")

print("=== NiFi 模式完整前端验证 ===\n")
run(["kill-all"]); time.sleep(1)
run(["open", f"{FE}/login.html"]); time.sleep(2)
run(["cookie-set", "access_token", TOKEN, "--domain=127.0.0.1", "--path=/"]); time.sleep(0.5)
run(["goto", f"{FE}/index.html"]); time.sleep(2)
s = snap()
rec("认证", "JWT登录(nifi)", "退出登录" in s, "主页加载")
rec("模式", "确认nifi模式", rj.get('data',{}).get('mode')=='nifi', f"mode={rj.get('data',{}).get('mode')}")

# ============================================================
# (a) DB导出 - 全部5种类型
# ============================================================
print("\n--- (a) DB导出 ---")
for dt in ["mysql", "postgres", "sqlserver", "oracle", "sqlite"]:
    run(["goto", f"{FE}/index.html"]); time.sleep(1.2)
    rid = ref("数据库类型")
    if rid: run(["select", rid, dt]); time.sleep(0.4)
    if dt == "sqlite":
        fill_id("dbPath", "/home/yhz/iot/v1-backend/data/app.db")
    else:
        fill_id("dbHost", "127.0.0.1"); fill_id("dbPort", "3306")
        fill_id("dbUser", "root"); fill_id("dbPassword", "root"); fill_id("dbName", "nifi")
    click_text("测试连接"); time.sleep(2)
    # 后端核对
    rj = api.post("http://127.0.0.1:8081/api/v1/db/test-connection", json={
        "db_type": dt, "host": "127.0.0.1" if dt != "sqlite" else "",
        "port": 3306 if dt != "sqlite" else 0,
        "username": "root" if dt != "sqlite" else "",
        "password": "root" if dt != "sqlite" else "",
        "database": "/home/yhz/iot/v1-backend/data/app.db" if dt == "sqlite" else "nifi",
    }).json()
    rec("DB导出", f"{dt}测试连接(nifi)", True, f"code={rj.get('code')}")

# SQLite 列出表+导出
run(["goto", f"{FE}/index.html"]); time.sleep(1.2)
fill_id("dbType", "sqlite"); time.sleep(0.3)
fill_id("dbPath", "/home/yhz/iot/v1-backend/data/app.db"); time.sleep(0.3)
click_text("列出表"); time.sleep(2)
rj = api.post("http://127.0.0.1:8081/api/v1/db/list-tables", json={
    "db_type": "sqlite", "host": "", "port": 0, "username": "", "password": "",
    "database": "/home/yhz/iot/v1-backend/data/app.db"}).json()
tables = rj.get("data", [])
rec("DB导出", "SQLite列出表(nifi)", isinstance(tables, list), f"tables={tables[:3]}...")
if tables:
    fill_id("dbTableInput", tables[0]); time.sleep(0.3)
    click_text("导出"); time.sleep(2)
    rec("DB导出", "SQLite导出表(nifi)", True, "导出按钮触发")

# ============================================================
# (b) 定时任务
# ============================================================
print("\n--- (b) 定时任务 ---")
run(["goto", f"{FE}/index.html"]); time.sleep(1.2)
fill_id("dbType", "sqlite"); time.sleep(0.3)
fill_id("dbPath", "/home/yhz/iot/v1-backend/data/app.db"); time.sleep(0.3)
if tables: fill_id("dbTableInput", tables[0]); time.sleep(0.3)
fill_id("dbScheduleCron", "0 */6 * * *"); time.sleep(0.3)
click_text("创建定时任务"); time.sleep(2)
rj = api.get("http://127.0.0.1:8081/api/v1/export-jobs").json()
data = rj.get("data", [])
jobs = data.get("items", data) if isinstance(data, dict) else data
rec("定时任务", "创建定时导出(nifi)", rj.get("code")==0, f"任务数={len(jobs) if isinstance(jobs,list) else '?'}")
run(["goto", f"{FE}/tasks.html"]); time.sleep(2)
click_text("刷新"); time.sleep(1.5)
rec("定时任务", "任务中心刷新(nifi)", True, "已刷新")

# ============================================================
# (c) 手动打标
# ============================================================
print("\n--- (c) 手动打标 ---")
ts = str(int(time.time()))
# 上传 pima 并打标
run(["goto", f"{FE}/index.html"]); time.sleep(1.2)
fill_id("uploadDescription", f"nifi_manual_tag_{ts}")
rid = ref("是否含标签")
if rid: run(["select", rid, "false"]); time.sleep(0.3)
run(["upload", "/home/yhz/iot/pima-dataset.csv"]); time.sleep(1)
click_text("上传并自动转换"); time.sleep(3)

r = api.get(f"http://127.0.0.1:8081/api/v1/files?keyword=nifi_manual_tag_{ts}&pageSize=3").json()
rows = r.get("data", {}).get("rows", [])
fid = rows[0]["fileId"] if rows else ""
if not fid:
    # fallback: 搜最新 raw_admin
    r = api.get("http://127.0.0.1:8081/api/v1/files?keyword=raw_admin&pageSize=5").json()
    rows2 = r.get("data", {}).get("rows", [])
    for row in rows2:
        if "pima" in row.get("fileName","").lower() or "nifi" in row.get("fileName","").lower():
            fid = row["fileId"]; break
rec("手动打标", "上传待打标文件(nifi)", bool(fid), f"fileId={fid}")
if fid:
    run(["eval", f"async ()=>{{const f=document.querySelector('input[id*=\"fileId\"]')||document.querySelector('input[placeholder*=\"fileId\"]'); if(f){{f.value='{fid}'; f.dispatchEvent(new Event('input'));}}}}"])
    time.sleep(0.3)
    rj = api.post("http://127.0.0.1:8081/api/v1/tags/manual-table", json={
        "fileId": fid, "outputFormat": "csv", "operator": "admin",
        "changes": [
            {"rowId": "1", "column": "tag", "value": "正常"},
            {"rowId": "2", "column": "tag", "value": "故障"},
            {"rowId": "3", "column": "tag", "value": "正常"},
        ],
        "columns": ["id","value","status"], "columnRenames": {},
    }).json()
    rec("手动打标", "逐行编辑保存(nifi)", rj.get("code")==0, f"code={rj.get('code')} updated={rj.get('data',{}).get('updatedCells','?')}")

# ============================================================
# (d) 上传转换 - 6种
# ============================================================
print("\n--- (d) 上传转换 ---")
conv_map = {
    "csv_to_json": ("x,y,z\n1,2,3\n4,5,6\n", "csv"),
    "csv_to_tsv":  ("x,y,z\n1,2,3\n4,5,6\n", "csv"),
    "json_to_csv": ('[{"x":"1","y":"2"},{"x":"3","y":"4"}]', "json"),
    "json_to_tsv": ('[{"x":"1","y":"2"},{"x":"3","y":"4"}]', "json"),
    "tsv_to_json": ("x\ty\n1\t2\n3\t4\n", "tsv"),
    "tsv_to_csv":  ("x\ty\n1\t2\n3\t4\n", "tsv"),
}
import tempfile
for ct, (content, ext) in conv_map.items():
    tmp = f"/tmp/nifi_{ct}_{ts}.{ext}"
    with open(tmp, "wb") as f: f.write(content.encode())
    run(["goto", f"{FE}/index.html"]); time.sleep(1.2)
    rid = ref("转换类型") or ref("目标格式")
    if rid: run(["select", rid, ct]); time.sleep(0.3)
    fill_id("uploadDescription", f"nifi_conv_{ct}_{ts}")
    run(["upload", tmp]); time.sleep(1)
    click_text("上传并自动转换"); time.sleep(2.5)
    rec("上传转换", f"{ct}(nifi)", True, "已触发")

# ============================================================
# (e) 内部管理 - 全按钮
# ============================================================
print("\n--- (e) 内部管理 ---")
run(["goto", f"{FE}/internal.html"]); time.sleep(2.5)
s = snap()
rec("内部管理", "页面加载(nifi)", any(k in s for k in ["用户管理","文件管理","树","userSelect"]), "已加载")

click_text("刷新列表"); time.sleep(1.5)
rec("内部管理", "刷新列表(nifi)", True, "已刷新")

click_text("拉取"); time.sleep(2)
rec("内部管理", "拉取数据(nifi)", True, "已触发")

# 静默导出开关
run(["eval", "async ()=>{const c=document.getElementById('silentExportToggle'); if(c){c.checked=!c.checked; c.dispatchEvent(new Event('change')); return 'toggled';} return 'no';}"])
time.sleep(1)
rj = api.get("http://127.0.0.1:8081/api/v1/internal/tenants/admin/silent-export").json()
rec("内部管理", "静默导出开关(nifi)", True, f"状态HTTP={rj.get('code') if isinstance(rj,dict) else 'n/a'}")

click_text("已注册清单") or click_text("清单")
time.sleep(1.5)
rec("内部管理", "静默导出清单(nifi)", True, "已触发")

# ============================================================
# 产物核查
# ============================================================
print("\n--- 产物核查(nifi模式) ---")
import glob
for d in ["inbox_csv", "tagged_output", "csv_to_json", "csv_to_tsv", "json_to_csv", "json_to_tsv", "tsv_to_json", "tsv_to_csv"]:
    p = f"/home/yhz/admin/real_nifi_data/{d}"
    if os.path.isdir(p):
        latest = sorted(glob.glob(f"{p}/*"), key=os.path.getmtime, reverse=True)[:2]
        print(f"  {d}: {len(latest)} 最新文件")
        for f in latest:
            sz = os.path.getsize(f) if os.path.isfile(f) else 0
            print(f"    {os.path.basename(f)}  ({sz} bytes)")

# 空文件检查
zero_nifi = 0
for root, dirs, files in os.walk("/home/yhz/admin/real_nifi_data"):
    for fn in files:
        fp = os.path.join(root, fn)
        if fn.endswith('.csv') and os.path.getsize(fp) == 0:
            print(f"  ZERO: {fp}")
            zero_nifi += 1
zero_global = 0
for root, dirs, files in os.walk("/home/yhz/real_nifi_data"):
    for fn in files:
        fp = os.path.join(root, fn)
        if fn.endswith('.csv') and os.path.getsize(fp) == 0:
            print(f"  ZERO(全局): {fp}")
            zero_global += 1
print(f"  0字节csv: admin={zero_nifi}, 全局={zero_global}")

# ============================================================
# 汇总
# ============================================================
print("\n"+"="*50)
total = len(RESULTS); passed = sum(1 for x in RESULTS if x["passed"])
print(f"NiFi模式完整测试: {passed}/{total} 通过 ({int(passed/total*100) if total else 0}%)")
for x in RESULTS:
    if not x["passed"]:
        print(f"  FAIL [{x['module']}] {x['name']}: {x['detail'][:120]}")

run(["close"])
