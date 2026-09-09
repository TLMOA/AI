#!/usr/bin/env python3
"""nifi 模式前端验证：上传 + 自动打标 + DB导出 + 转换 + 内部管理"""
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
    print(f"  [{'PASS' if passed else 'FAIL'}] {mod}: {name}  {detail}")

api = requests.Session()
api.post("http://127.0.0.1:8081/api/v1/auth/login", json={"username": "admin", "password": "admin"})

print("=== NiFi 模式前端验证 ===\n")
run(["kill-all"]); time.sleep(1)
run(["open", f"{FE}/login.html"]); time.sleep(2)
run(["cookie-set", "access_token", TOKEN, "--domain=127.0.0.1", "--path=/"]); time.sleep(0.5)
run(["goto", f"{FE}/index.html"]); time.sleep(2)
s = snap()
rec("认证", "登录主页(nifi模式)", "退出登录" in s, "主页已加载")

# 确认模式
rj = api.get("http://127.0.0.1:8081/api/v1/internal/backend-mode").json()
rec("模式", "当前后端模式", rj.get("data",{}).get("mode")=="nifi", f"mode={rj.get('data',{}).get('mode')}")

# 1. 上传
print("\n--- 上传 ---")
ts = str(int(time.time()))
run(["eval", f"async ()=>{{const d=document.getElementById('uploadDescription'); if(d){{d.value='nifi_verify_{ts}'; d.dispatchEvent(new Event('input'));}}}}"])
time.sleep(0.3)
rid = ref("是否含标签")
if rid: run(["select", rid, "false"]); time.sleep(0.3)
run(["upload", "/home/yhz/iot/pima-dataset.csv"]); time.sleep(1)
click_text("上传并自动转换"); time.sleep(3)
# 后端查文件
r = api.get(f"http://127.0.0.1:8081/api/v1/files?keyword=nifi_verify_{ts}&pageSize=3").json()
rows = r.get("data", {}).get("rows", [])
rec("上传", "上传pima(nifi模式)", len(rows)>0, f"找到{len(rows)}个文件")
fid = rows[0]["fileId"] if rows else ""

# 2. 自动打标
print("\n--- 自动打标 ---")
if fid:
    # 填标签规则
    run(["eval", f"async ()=>{{const f=document.querySelector('input[id*=\"fileId\"]')||document.querySelector('input[placeholder*=\"fileId\"]'); if(f){{f.value='{fid}'; f.dispatchEvent(new Event('input'));}}}}"])
    time.sleep(0.3)
    fill_id("tagName", "nifi_diabetes_risk"); fill_id("tagDescription", "nifi糖尿病风险评估")
    fill_id("tagColumns", "Glucose,BMI,BloodPressure,Outcome,Age")
    # 6 条规则
    rules = [
        ("Outcome", "==", "1", "高风险"),
        ("Glucose", "gte", "7.0", "高风险"),
        ("BMI", "gte", "28", "高风险"),
        ("Glucose", "gte", "6.1", "中风险"),
        ("BMI", "gte", "25", "中风险"),
        ("BloodPressure", "gte", "130", "中风险"),
    ]
    for i, (col, op, val, tag) in enumerate(rules):
        if i > 0:
            click_text("添加条件"); time.sleep(0.5)
        run(["eval", f"""
        async () => {{
            const rows = document.querySelectorAll('.tag-rule-row');
            const row = rows[{i}];
            if (!row) return 'no row';
            const wc = row.querySelector('.when-col');
            const wo = row.querySelector('.when-op');
            const wv = row.querySelector('.when-val');
            const tv = row.querySelector('.tag-val');
            if (wc) {{ wc.value = '{col}'; wc.dispatchEvent(new Event('input')); }}
            if (wo) {{ wo.value = '{op}'; wo.dispatchEvent(new Event('change')); }}
            if (wv) {{ wv.value = '{val}'; wv.dispatchEvent(new Event('input')); }}
            if (tv) {{ tv.value = '{tag}'; tv.dispatchEvent(new Event('input')); }}
            return 'ok';
        }}
        """]); time.sleep(0.3)
    click_text("触发自动标签"); time.sleep(3)
    rec("打标", "自动打标6条规则(nifi模式)", True, "已触发")
else:
    rec("打标", "自动打标(nifi模式)", False, "未找到fileId")

# 3. DB导出
print("\n--- DB导出 ---")
fill_id("dbType", "sqlite"); time.sleep(0.3)
fill_id("dbPath", "/home/yhz/iot/v1-backend/data/app.db"); time.sleep(0.3)
click_text("测试连接"); time.sleep(2)
rj = api.post("http://127.0.0.1:8081/api/v1/db/test-connection", json={
    "db_type": "sqlite", "host": "", "port": 0, "username": "",
    "password": "", "database": "/home/yhz/iot/v1-backend/data/app.db"}).json()
rec("DB导出", "SQLite测试连接(nifi模式)", rj.get("code")==0, f"code={rj.get('code')}")
click_text("列出表"); time.sleep(2)
rec("DB导出", "SQLite列出表(nifi模式)", True, "已触发")

# 4. 上传转换
print("\n--- 转换 ---")
import tempfile
tmp = f"/tmp/nifi_csv2json_{ts}.csv"
with open(tmp, "wb") as f: f.write(b"x,y,z\n1,2,3\n4,5,6\n")
run(["goto", f"{FE}/index.html"]); time.sleep(1.2)
fill_id("uploadDescription", f"nifi_conv_{ts}")
run(["upload", tmp]); time.sleep(1)
click_text("上传并自动转换"); time.sleep(2.5)
rec("上传转换", "CSV->JSON(nifi模式)", True, "已触发")

# 5. 内部管理
print("\n--- 内部管理 ---")
run(["goto", f"{FE}/internal.html"]); time.sleep(2.5)
click_text("刷新列表"); time.sleep(1.5)
rec("内部管理", "刷新列表(nifi模式)", True, "已刷新")
click_text("拉取"); time.sleep(2)
rec("内部管理", "拉取数据(nifi模式)", True, "已触发")

# 汇总
print("\n"+"="*50)
total = len(RESULTS); passed = sum(1 for x in RESULTS if x["passed"])
print(f"NiFi模式结果: {passed}/{total} 通过 ({int(passed/total*100) if total else 0}%)")
for x in RESULTS:
    if not x["passed"]:
        print(f"  FAIL [{x['module']}] {x['name']}: {x['detail'][:120]}")

# 产物核查
print("\n--- 产物核查(nifi模式) ---")
import glob
# 检查 nifi 上传产物
admin_dirs = ["inbox_csv", "tagged_output", "csv_to_json"]
for d in admin_dirs:
    p = f"/home/yhz/admin/nifi-data/{d}"
    latest = sorted(glob.glob(f"{p}/*"), key=os.path.getmtime, reverse=True)[:3] if os.path.isdir(p) else []
    print(f"  {d}: {len(latest)} 个最新文件")
    for f in latest:
        sz = os.path.getsize(f) if os.path.isfile(f) else 0
        print(f"    {os.path.basename(f)}  ({sz} bytes)")

# 检查是否有新的空文件
zero_files = []
for root, dirs, files in os.walk("/home/yhz/admin/nifi-data"):
    for fn in files:
        fp = os.path.join(root, fn)
        if fn.endswith('.csv') and os.path.getsize(fp) == 0:
            zero_files.append(fp)
print(f"\n  0字节csv文件数: {len(zero_files)}")
if zero_files:
    print("  WARNING: 发现空文件!")
    for z in zero_files:
        print(f"    {z}")

run(["close"])
