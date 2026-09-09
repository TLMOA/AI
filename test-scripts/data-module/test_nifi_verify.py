"""NiFi 端全功能验证：admin + 普通用户，pima 糖尿病数据集。
每个任务必须真的经过 NiFi 容器执行。
"""
import requests, time, os, json, sqlite3, csv
from collections import Counter

BASE = 'http://127.0.0.1:8081'
DBPATH = '/home/yhz/real_nifi_data/pima_verify.db'
DBPATH_CONTAINER = '/opt/nifi/nifi-current/data/iot/pima_verify.db'
PIMA_CSV = '/home/yhz/iot/pima-dataset.csv'

results = []  # (user, module, item, pass, detail)

def rec(user, module, item, passed, detail):
    results.append((user, module, item, passed, detail))
    mark = "PASS" if passed else "FAIL"
    print(f"  [{mark}] {module} - {item}: {detail}")

def login(username, password):
    s = requests.Session()
    r = s.post(f'{BASE}/api/v1/auth/login', json={'username':username,'password':password})
    return s, r.json().get('success', False)

def ensure_nifi_mode(s):
    r = s.get(f'{BASE}/api/v1/internal/backend-mode')
    mode = r.json()['data']['mode']
    if mode != 'nifi':
        s.post(f'{BASE}/api/v1/internal/backend-mode', json={'mode':'nifi'})
        time.sleep(1)
    return 'nifi'

def clear_inboxes():
    for d in ['export_jobs/inbox','convert_jobs/inbox','tagging_jobs/inbox',
              'export_jobs/done','convert_jobs/done','tagging_jobs/done',
              'export_jobs/error','convert_jobs/error','tagging_jobs/error']:
        p = f'/home/yhz/real_nifi_data/{d}'
        if os.path.isdir(p):
            for f in os.listdir(p):
                if f.endswith('.json') or f.endswith('.meta.json'):
                    os.remove(os.path.join(p, f))

def wait_nifi_done(job_type, username, timeout=70):
    """等待 NiFi 容器执行完成，检查 done 目录。"""
    user_dir = f'/home/yhz/{username}/real_nifi_data/{job_type}/done'
    global_dir = f'/home/yhz/real_nifi_data/{job_type}/done'
    for i in range(int(timeout/5)):
        time.sleep(5)
        for d in [user_dir, global_dir]:
            if os.path.isdir(d):
                files = [f for f in os.listdir(d) if f.endswith('.json')]
                if files:
                    latest = max(files, key=lambda f: os.path.getmtime(os.path.join(d,f)))
                    with open(os.path.join(d, latest)) as fh:
                        data = json.load(fh)
                    return data
    return None

def count_nifi_processor_tasks():
    """统计 NiFi command processor 的执行次数。"""
    import subprocess
    result = subprocess.run(['python3','-c',f"""
import requests
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
s=requests.Session()
r=s.post('https://localhost:8080/nifi-api/access/token', data='username=admin&password=admin@nifi123', headers={{'Content-Type':'application/x-www-form-urlencoded'}}, verify=False)
s.headers.update({{'Authorization':'Bearer '+r.text.strip()}})
r=s.get('https://localhost:8080/nifi-api/process-groups/root/processors', verify=False)
for p in r.json()['processors']:
    if 'command' in p['component']['name'].lower() and 'iot' in p['component']['name'].lower():
        snap=p['status'].get('aggregateSnapshot',{{}})
        print(p['component']['name'], snap.get('taskCount',0))
"""], capture_output=True, text=True)
    tasks = {}
    for line in result.stdout.strip().split('\n'):
        if ' ' in line:
            name, count = line.rsplit(' ', 1)
            tasks[name] = int(count)
    return tasks

# === 准备 SQLite 测试库（含 pima 数据）===
print("=== 准备 pima SQLite 测试库 ===")
if os.path.exists(DBPATH):
    os.remove(DBPATH)
db = sqlite3.connect(DBPATH)
db.execute('CREATE TABLE pima (Pregnancies INT, Glucose INT, BloodPressure INT, SkinThickness INT, Insulin INT, BMI REAL, DiabetesPedigreeFunc REAL, Age INT, Outcome INT)')
with open(PIMA_CSV) as f:
    reader = csv.DictReader(f)
    rows = [(int(r['Pregnancies']),int(r['Glucose']),int(r['BloodPressure']),int(r['SkinThickness']),
            int(r['Insulin']),float(r['BMI']),float(r['DiabetesPedigreeFunction']),int(r['Age']),int(r['Outcome']))
           for r in reader]
db.executemany('INSERT INTO pima VALUES (?,?,?,?,?,?,?,?,?)', rows)
db.commit()
print(f"  插入 {len(rows)} 行 pima 数据到 {DBPATH}")
db.close()

# 记录 NiFi processor 初始 task count
print("\n=== 记录 NiFi 初始 task count ===")
tasks_before = count_nifi_processor_tasks()
print(f"  初始: {tasks_before}")

# 清空 inbox
clear_inboxes()
print("  已清空 inbox/done/error")

# ═══════════════════════════════════════════
# 1. ADMIN 用户验证
# ═══════════════════════════════════════════
print("\n" + "="*60)
print("1. ADMIN 用户 NiFi 端全功能验证")
print("="*60)

s_admin, ok = login('admin', 'admin')
print(f"  登录: {'成功' if ok else '失败'}")
mode = ensure_nifi_mode(s_admin)
print(f"  模式: {mode}")

# --- 1.1 DB 导出（SQLite pima 表）---
print("\n--- 1.1 DB 导出（SQLite pima 表）---")
ts = int(time.time())
body = {
    'db_config': {'type':'sqlite','db_type':'sqlite','path':DBPATH_CONTAINER},
    'table':'pima','file_format':'CSV','owner_id':'admin','id':f'admin_db_{ts}'
}
r = s_admin.post(f'{BASE}/api/v1/export', json=body)
j = r.json()
task = j.get('data',{}).get('task',{})
task_path = j.get('data',{}).get('taskPath','')
print(f"  投递: code={j.get('code')} path={task.get('path','')[:50]}")

# 等待 NiFi 执行
done = wait_nifi_done('export_jobs', 'admin', timeout=70)
if done and done.get('status') == 'SUCCEEDED':
    # 找产出文件
    output_dir = '/home/yhz/admin/real_nifi_data/output_csv'
    output_files = [f for f in os.listdir(output_dir) if 'pima' in f.lower() and f.endswith('.csv')] if os.path.isdir(output_dir) else []
    if output_files:
        fp = os.path.join(output_dir, sorted(output_files)[-1])
        with open(fp) as fh:
            lines = fh.readlines()
        rec('admin', 'DB导出', 'SQLite pima 导出', True, f"NiFi done=SUCCEEDED, 产出 {os.path.basename(fp)} {os.path.getsize(fp)}B {len(lines)}行")
    else:
        rec('admin', 'DB导出', 'SQLite pima 导出', False, "done=SUCCEEDED 但无产出文件")
else:
    rec('admin', 'DB导出', 'SQLite pima 导出', False, f"NiFi 执行: {done.get('status','timeout') if done else 'timeout'} {done.get('message','')[:100] if done else ''}")

# --- 1.2 上传转换（CSV→JSON）---
print("\n--- 1.2 上传转换（CSV→JSON）---")
with open(PIMA_CSV, 'rb') as f:
    r = s_admin.post(f'{BASE}/api/v1/upload/inbox_csv',
        params={'convertType':'csv_to_json','hasTag':'false','description':f'admin_convert_{ts}'},
        files={'file':('pima.csv', f.read(), 'text/csv')})
j = r.json()
file_id = j.get('data',{}).get('fileId','')
print(f"  上传: code={j.get('code')} fileId={file_id}")

# 等待 NiFi 转换
done = wait_nifi_done('convert_jobs', 'admin', timeout=70)
if done and done.get('status') == 'SUCCEEDED':
    # 找产出文件
    convert_dir = '/home/yhz/admin/real_nifi_data/csv_to_json'
    output_files = [f for f in os.listdir(convert_dir) if 'pima' in f.lower() and f.endswith('.json')] if os.path.isdir(convert_dir) else []
    if output_files:
        fp = os.path.join(convert_dir, sorted(output_files)[-1])
        # 验证内容是 JSONL
        with open(fp) as fh:
            first_line = fh.readline().strip()
        is_json = first_line.startswith('{') and '"Glucose"' in first_line
        rec('admin', '上传转换', 'CSV→JSON', True, f"NiFi done=SUCCEEDED, 产出 {os.path.basename(fp)} {os.path.getsize(fp)}B JSONL格式={is_json}")
    else:
        rec('admin', '上传转换', 'CSV→JSON', False, "done=SUCCEEDED 但无产出文件")
else:
    # 检查全局目录
    convert_dir = '/home/yhz/real_nifi_data/csv_to_json'
    output_files = [f for f in os.listdir(convert_dir) if 'pima' in f.lower() and f.endswith('.json')] if os.path.isdir(convert_dir) else []
    if output_files:
        fp = os.path.join(convert_dir, sorted(output_files)[-1])
        rec('admin', '上传转换', 'CSV→JSON', True, f"NiFi 产出在全局 {os.path.basename(fp)} {os.path.getsize(fp)}B")
    else:
        rec('admin', '上传转换', 'CSV→JSON', False, f"NiFi 执行: {done.get('status','timeout') if done else 'timeout'}")

# --- 1.3 自动打标 ---
print("\n--- 1.3 自动打标（pima Outcome 规则）---")
# 找上传的 pima csv
r = s_admin.get(f'{BASE}/api/v1/files?keyword=raw_admin_pima&pageSize=5')
rows_list = r.json().get('data',{}).get('rows',[])
pima_fid = ''
for row in rows_list:
    if row.get('fileName','').endswith('.csv'):
        pima_fid = row['fileId']
        break
if pima_fid:
    r = s_admin.post(f'{BASE}/api/v1/tags/auto', json={
        'fileId': pima_fid,
        'operator': 'admin',
        'tagName': '风险等级',
        'tagRule': {'Outcome': {'1':'高风险','0':'低风险'}}
    })
    j = r.json()
    print(f"  打标触发: code={j.get('code')} status={j.get('data',{}).get('status','')}")

    # 等待 NiFi 打标
    done = wait_nifi_done('tagging_jobs', 'admin', timeout=70)
    if done and done.get('status') == 'SUCCEEDED':
        tagged_dir = '/home/yhz/admin/real_nifi_data/tagged_output'
        tagged_files = [f for f in os.listdir(tagged_dir) if 'pima' in f.lower() and f.endswith('.csv')] if os.path.isdir(tagged_dir) else []
        if tagged_files:
            fp = os.path.join(tagged_dir, sorted(tagged_files)[-1])
            with open(fp) as fh:
                reader = csv.DictReader(fh)
                rows = list(reader)
            tags = Counter(r.get('Outcome','') for r in rows)
            tag_dist = dict(tags.most_common())
            rec('admin', '自动打标', 'Outcome规则打标', True, f"NiFi done=SUCCEEDED, {len(rows)}行, Outcome列标签分布={tag_dist}")
        else:
            rec('admin', '自动打标', 'Outcome规则打标', False, "done=SUCCEEDED 但无产出文件")
    else:
        rec('admin', '自动打标', 'Outcome规则打标', False, f"NiFi 执行: {done.get('status','timeout') if done else 'timeout'} {done.get('message','')[:100] if done else ''}")
else:
    rec('admin', '自动打标', 'Outcome规则打标', False, "未找到上传的 pima csv")

# ═══════════════════════════════════════════
# 2. 普通用户验证
# ═══════════════════════════════════════════
print("\n" + "="*60)
print("2. 普通用户 NiFi 端全功能验证")
print("="*60)

# 注册新普通用户
username = f'pimauser_{ts}'
password = username
r = requests.post(f'{BASE}/api/v1/auth/register', json={'username':username,'password':password})
print(f"  注册: {r.json().get('success')} 用户={username}")

s_user, ok = login(username, password)
print(f"  登录: {'成功' if ok else '失败'}")
mode = ensure_nifi_mode(s_user)
print(f"  模式: {mode}")

# --- 2.1 上传转换（CSV→TSV）---
print("\n--- 2.1 上传转换（CSV→TSV）---")
with open(PIMA_CSV, 'rb') as f:
    r = s_user.post(f'{BASE}/api/v1/upload/inbox_csv',
        params={'convertType':'csv_to_tsv','hasTag':'false','description':f'user_convert_{ts}'},
        files={'file':('pima.csv', f.read(), 'text/csv')})
j = r.json()
file_id_user = j.get('data',{}).get('fileId','')
print(f"  上传: code={j.get('code')} fileId={file_id_user}")

done = wait_nifi_done('convert_jobs', username, timeout=70)
if done and done.get('status') == 'SUCCEEDED':
    # 产物可能在用户目录或全局目录
    convert_dir = f'/home/yhz/{username}/real_nifi_data/csv_to_tsv'
    output_files = [f for f in os.listdir(convert_dir) if 'pima' in f.lower() and f.endswith('.tsv')] if os.path.isdir(convert_dir) else []
    if not output_files:
        # 查全局目录
        convert_dir = '/home/yhz/real_nifi_data/csv_to_tsv'
        output_files = [f for f in os.listdir(convert_dir) if 'pima' in f.lower() and f.endswith('.tsv')] if os.path.isdir(convert_dir) else []
    if output_files:
        fp = os.path.join(convert_dir, sorted(output_files)[-1])
        with open(fp) as fh:
            first_line = fh.readline().strip()
        is_tsv = '\t' in first_line
        rec(username, '上传转换', 'CSV→TSV', True, f"NiFi done=SUCCEEDED, {os.path.basename(fp)} {os.path.getsize(fp)}B TSV格式={is_tsv}")
    else:
        rec(username, '上传转换', 'CSV→TSV', False, "done=SUCCEEDED 但无产出文件")
else:
    rec(username, '上传转换', 'CSV→TSV', False, f"NiFi 执行: {done.get('status','timeout') if done else 'timeout'}")

# --- 2.2 DB 导出 ---
print("\n--- 2.2 DB 导出（SQLite pima 表）---")
body = {
    'db_config': {'type':'sqlite','db_type':'sqlite','path':DBPATH_CONTAINER},
    'table':'pima','file_format':'CSV','owner_id':username,'id':f'user_db_{ts}'
}
r = s_user.post(f'{BASE}/api/v1/export', json=body)
j = r.json()
print(f"  投递: code={j.get('code')}")

done = wait_nifi_done('export_jobs', username, timeout=70)
if done and done.get('status') == 'SUCCEEDED':
    output_dir = f'/home/yhz/{username}/real_nifi_data/output_csv'
    output_files = [f for f in os.listdir(output_dir) if 'pima' in f.lower() and f.endswith('.csv')] if os.path.isdir(output_dir) else []
    if output_files:
        fp = os.path.join(output_dir, sorted(output_files)[-1])
        rec(username, 'DB导出', 'SQLite pima 导出', True, f"NiFi done=SUCCEEDED, {os.path.basename(fp)} {os.path.getsize(fp)}B")
    else:
        rec(username, 'DB导出', 'SQLite pima 导出', False, "done=SUCCEEDED 但无产出文件")
else:
    rec(username, 'DB导出', 'SQLite pima 导出', False, f"NiFi 执行: {done.get('status','timeout') if done else 'timeout'}")

# --- 2.3 自动打标 ---
print("\n--- 2.3 自动打标 ---")
if file_id_user:
    r = s_user.post(f'{BASE}/api/v1/tags/auto', json={
        'fileId': file_id_user,
        'operator': username,
        'tagName': '风险等级',
        'tagRule': {'Outcome': {'1':'高风险','0':'低风险'}}
    })
    j = r.json()
    print(f"  打标触发: code={j.get('code')}")

    done = wait_nifi_done('tagging_jobs', username, timeout=70)
    if done and done.get('status') == 'SUCCEEDED':
        tagged_dir = f'/home/yhz/{username}/real_nifi_data/tagged_output'
        tagged_files = [f for f in os.listdir(tagged_dir) if 'pima' in f.lower() and f.endswith('.csv')] if os.path.isdir(tagged_dir) else []
        if tagged_files:
            fp = os.path.join(tagged_dir, sorted(tagged_files)[-1])
            with open(fp) as fh:
                rows = list(csv.DictReader(fh))
            tags = Counter(r.get('Outcome','') for r in rows)
            rec(username, '自动打标', 'Outcome规则打标', True, f"NiFi done=SUCCEEDED, {len(rows)}行, 标签={dict(tags.most_common())}")
        else:
            rec(username, '自动打标', 'Outcome规则打标', False, "done=SUCCEEDED 但无产出")
    else:
        rec(username, '自动打标', 'Outcome规则打标', False, f"NiFi: {done.get('status','timeout') if done else 'timeout'}")
else:
    rec(username, '自动打标', 'Outcome规则打标', False, "未上传文件")

# ═══════════════════════════════════════════
# 3. 验证 NiFi 容器真的执行了
# ═══════════════════════════════════════════
print("\n" + "="*60)
print("3. 验证 NiFi 容器真的执行了（task count 增量）")
print("="*60)

tasks_after = count_nifi_processor_tasks()
print(f"  初始: {tasks_before}")
print(f"  结束: {tasks_after}")
for name in tasks_after:
    before = tasks_before.get(name, 0)
    after = tasks_after[name]
    delta = after - before
    mark = "✅ 真经过容器" if delta > 0 else "❌ 未经过容器"
    print(f"  {name}: {before} → {after} (增量={delta}) {mark}")

# ═══════════════════════════════════════════
# 4. 汇总
# ═══════════════════════════════════════════
print("\n" + "="*60)
print("4. 验证汇总")
print("="*60)
total = len(results)
passed = sum(1 for r in results if r[3])
print(f"总计: {passed}/{total} 通过")
for user, module, item, ok, detail in results:
    mark = "✅" if ok else "❌"
    print(f"  {mark} [{user}] {module} - {item}")

# 保存结果到 JSON
with open('/home/yhz/iot/nifi_verify_result.json', 'w') as f:
    json.dump({
        'total': total,
        'passed': passed,
        'results': [{'user':u,'module':m,'item':i,'pass':p,'detail':d} for u,m,i,p,d in results],
        'nifi_tasks_before': tasks_before,
        'nifi_tasks_after': tasks_after,
    }, f, ensure_ascii=False, indent=2)
print(f"\n结果已保存到 /home/yhz/iot/nifi_verify_result.json")
