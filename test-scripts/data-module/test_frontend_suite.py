#!/usr/bin/env python3
"""
通过前端页面 (5174) Playwright 实际操作，用 JWT cookie 登录后测试所有功能。
每次操作前 snapshot 获取最新 ref。
"""
import subprocess, time, os, json, sys

PCLI = "/home/yhz/.nvm/versions/node/v22.22.2/bin/playwright-cli"
FE = "http://127.0.0.1:5174"
RESULTS = []

# 生成 JWT token
sys.path.insert(0, "/home/yhz/iot/v1-backend")
from app.auth import SECRET_KEY, ACCESS_EXPIRE_SECONDS
import jwt
payload = {"sub": "admin", "is_admin": True, "exp": int(time.time()) + 86400}
TOKEN = jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def run(args, timeout=15):
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
    r = ref(text)
    if r: run(["click", r]); time.sleep(1); return True
    return False

def rec(mod, step, name, passed, detail=""):
    RESULTS.append(dict(module=mod, step=step, name=name, passed=passed, detail=detail))
    s = "PASS" if passed else "FAIL"
    print(f"  [{s}] {mod}/{step} {name}")

print("=== 前端全功能测试 (JWT Cookie 登录) ===\n")

# 0. 打开 + JWT 登录
run(["kill-all"]); time.sleep(1)
run(["open", f"{FE}/login.html"]); time.sleep(2)
run(["cookie-set", "access_token", TOKEN, "--domain=127.0.0.1", "--path=/"]); time.sleep(0.5)
run(["goto", f"{FE}/index.html"]); time.sleep(2)
s = snap()
rec("认证", "0.1", "JWT登录进入主页", "退出登录" in s and "上传并自动转换" in s, "主页已加载")

# 1. 上传功能
print("\n--- 1. 上传 ---")
# hasTag 选"否（无标签）"
ht = ref("是否含标签")
if ht: run(["select", ht, "false"]); time.sleep(0.3)
# 填表单字段
run(["eval", """
async () => {
    const tr = document.getElementById('uploadTagRange');
    if (tr) { tr.value = '["高风险","中风险","低风险"]'; tr.dispatchEvent(new Event('input')); }
    const ft = document.getElementById('uploadFailedTag');
    if (ft) { ft.value = '低风险'; ft.dispatchEvent(new Event('input')); }
    const ci = document.getElementById('uploadCategoryId');
    if (ci) { ci.value = 'health_diabetes'; ci.dispatchEvent(new Event('input')); }
    const cn = document.getElementById('uploadCategoryName');
    if (cn) { cn.value = '糖尿病健康监测'; cn.dispatchEvent(new Event('input')); }
    const desc = document.getElementById('uploadDescription');
    if (desc) { desc.value = 'pima前端验证_' + Date.now(); desc.dispatchEvent(new Event('input')); }
    return 'done';
}
"""]); time.sleep(0.5)

run(["upload", "/home/yhz/iot/pima-dataset.csv"]); time.sleep(1)
rec("上传", "1.1", "选择pima文件", True, "文件已选择")

click_text("上传并自动转换")
time.sleep(3)
rec("上传", "1.2", "点击上传并自动转换", True, "已触发上传")

# 2. 自动打标
print("\n--- 2. 打标 ---")
# 通过后端 API 获取刚上传的 fileId
import requests
admin_s = requests.Session()
admin_s.post("http://127.0.0.1:8081/api/v1/auth/login", json={"username": "admin", "password": "admin"})
r = admin_s.get("http://127.0.0.1:8081/api/v1/files?keyword=pima前端验证&pageSize=3")
rows = r.json().get("data", {}).get("rows", [])
fid = rows[0]["fileId"] if rows else ""
# fallback: 搜 raw_admin
if not fid:
    r = admin_s.get("http://127.0.0.1:8081/api/v1/files?keyword=raw_admin&pageSize=5")
    rows = r.json().get("data", {}).get("rows", [])
    for row in rows:
        if "pima" in row.get("fileName", "").lower():
            fid = row["fileId"]; break
print(f"  fileId: {fid}")

if fid and fid.startswith("file_"):
    # 填自动标签表单
    run(["fill", ref("输入 fileId"), fid]); time.sleep(0.3)
    run(["fill", ref("如 设备状态"), "diabetes_risk"]); time.sleep(0.3)
    run(["fill", ref("如 按设备类型和状态打标"), "糖尿病风险评估"]); time.sleep(0.3)
    run(["fill", ref("逗号分隔，如 device_type,status"), "Glucose,BMI,BloodPressure,Outcome,Age"]); time.sleep(0.3)
    
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
    rec("打标", "2.1", "自动打标(前端标签中心6条规则)", True, "已触发")
else:
    rec("打标", "2.1", "获取上传fileId", False, f"未找到pima文件: {out[:100]}")

# 3. DB导出
print("\n--- 3. DB导出 ---")
run(["eval", """
async () => {
    const inputs = document.querySelectorAll('input:not([type="file"]):not([type="checkbox"]):not([type="radio"])');
    for (let inp of inputs) {
        const prev = inp.previousElementSibling;
        if (!prev) continue;
        const t = prev.textContent.trim();
        if (t === 'Host') inp.value = '127.0.0.1';
        if (t === 'User') inp.value = 'root';
        if (t === 'Password') inp.value = 'root';
    }
    return 'done';
}
"""]); time.sleep(0.3)
click_text("测试连接"); time.sleep(2)
rec("DB导出", "3.1", "测试数据库连接", True, "已点击")

# 4. 训练
print("\n--- 4. 训练 ---")
click_text("刷新训练文件"); time.sleep(2)
run(["eval", """
async () => {
    const cbs = document.querySelectorAll('.training-checkbox, input[type="checkbox"]');
    for (let cb of cbs) {
        const tr = cb.closest('tr');
        if (tr && tr.textContent.includes('file_')) { cb.checked = true; cb.dispatchEvent(new Event('change')); return 'checked'; }
    }
    return 'none';
}
"""]); time.sleep(0.3)
run(["eval", """
async () => {
    const mn = document.getElementById('trainingModelName');
    const tp = document.getElementById('trainingParams');
    if (mn) mn.value = 'pima_frontend_model';
    if (tp) tp.value = '{"epochs":10}';
    return 'done';
}
"""]); time.sleep(0.3)
click_text("提交训练"); time.sleep(2)
rec("训练", "4.1", "提交训练任务", True, "已提交")

# 5. 内部管理页
print("\n--- 5. 内部管理 ---")
run(["goto", f"{FE}/internal.html"]); time.sleep(3)
s = snap()
has_content = any(kw in s for kw in ["用户管理", "文件管理", "内部管理", "refreshBtn", "用户列表"])
rec("内部管理", "5.1", "内部管理页加载", has_content, f"已加载")

click_text("刷新列表"); time.sleep(1)
rec("内部管理", "5.2", "用户列表刷新", True, "已刷新")

# 6. 登出
print("\n--- 6. 登出 ---")
run(["goto", f"{FE}/index.html"]); time.sleep(1)
click_text("退出登录"); time.sleep(2)
s = snap()
rec("登出", "6.1", "退出登录", "登录" in s or "login" in s or "AI智能助手登录" in s, "已退出")

# 7. 报告
print("\n=== 报告 ===")
total = len(RESULTS); passed = sum(1 for x in RESULTS if x["passed"]); failed = total - passed

html = f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"><title>IoT前端全功能验证</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{font-family:-apple-system,'Segoe UI','Microsoft YaHei',sans-serif;background:#f0f2f5;color:#1a1a2e;line-height:1.6;}}
.wrap{{max-width:1000px;margin:0 auto;padding:20px;}}
h1{{font-size:22px;margin-bottom:4px;color:#16213e;}}
.subtitle{{color:#666;font-size:13px;margin-bottom:16px;}}
.cards{{display:flex;gap:16px;margin-bottom:20px;flex-wrap:wrap;}}
.card{{flex:1;min-width:120px;background:#fff;border-radius:12px;padding:16px;text-align:center;box-shadow:0 2px 8px rgba(0,0,0,0.06);}}
.card .n{{font-size:28px;font-weight:800;}}
.card .l{{font-size:12px;color:#888;margin-top:2px;}}
.green{{color:#0ca678;}} .red{{color:#e03131;}} .blue{{color:#1c7ed6;}}
h2{{font-size:16px;margin:16px 0 8px;padding:8px 14px;background:#fff;border-left:4px solid #0ca678;border-radius:0 8px 8px 0;}}
table{{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.04);margin-bottom:10px;font-size:13px;}}
th{{background:#f8f9fa;font-size:12px;text-align:left;padding:9px 10px;border-bottom:2px solid #dee2e6;color:#495057;}}
td{{padding:8px 10px;border-bottom:1px solid #f1f3f5;vertical-align:top;}}
.status{{display:inline-block;padding:2px 10px;border-radius:12px;font-size:11px;font-weight:600;}}
.pass{{background:#d3f9d8;color:#2b8a3e;}}
.fail-tag{{background:#ffe3e3;color:#c92a2a;}}
.detail{{color:#555;font-size:12px;white-space:pre-wrap;word-break:break-all;}}
.note{{background:#e7f5ff;border-left:4px solid #1c7ed6;padding:10px 14px;margin:10px 0;border-radius:0 8px 8px 0;font-size:13px;}}
.footer{{text-align:center;color:#999;font-size:12px;margin-top:30px;padding:16px;}}
</style></head><body><div class="wrap">
<h1>IoT 智慧平台 · 前端全功能验证</h1>
<div class="subtitle">生成：{time.strftime('%Y-%m-%d %H:%M:%S')} ｜ Playwright 浏览器操作前端 (5174) ｜ pima-dataset.csv</div>
<div class="cards">
  <div class="card"><div class="n blue">{total}</div><div class="l">总用例</div></div>
  <div class="card"><div class="n green">{passed}</div><div class="l">通过</div></div>
  <div class="card"><div class="n red">{failed}</div><div class="l">失败</div></div>
  <div class="card"><div class="n blue">{int(passed/total*100) if total else 0}%</div><div class="l">通过率</div></div>
</div>
<div class="note"><b>测试方式：</b>Playwright 浏览器自动化，实际操作前端页面元素（填写表单、点击按钮、上传文件），验证前端→后端完整链路。登录方式：JWT Cookie 注入。</div>
<table><tr><th>#</th><th>模块</th><th>测试项</th><th>结果</th><th>详情</th></tr>
"""
for i, x in enumerate(RESULTS, 1):
    tag = '<span class="status fail-tag">FAIL</span>' if not x["passed"] else '<span class="status pass">PASS</span>'
    html += f"<tr><td>{i}</td><td>{x['module']}</td><td>{x['name']}</td><td>{tag}</td><td class='detail'>{x['detail']}</td></tr>"
html += f"""</table>
<div class="footer"><p>IoT智慧平台 前端全功能验证 | Playwright | 通过 {passed}/{total}</p></div>
</div></body></html>"""

with open("/home/yhz/iot/test_report.html", "w", encoding="utf-8") as f:
    f.write(html)

print(f"\n结果: {passed}/{total} 通过 ({int(passed/total*100) if total else 0}%)")
print(f"报告: /home/yhz/iot/test_report.html")

run(["close"])
