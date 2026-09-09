"""验证 hasTag=true 时产物落 tagged_output/ 目录，admin + 普通用户 × Local + NiFi。"""
import requests, time, os, json, csv, sqlite3
from collections import Counter

BASE = 'http://127.0.0.1:8081'
PIMA = '/home/yhz/iot/pima-dataset.csv'
results = []

def rec(user, mode, test, passed, detail):
    results.append((user, mode, test, passed, detail))
    mark = "PASS" if passed else "FAIL"
    print(f"  [{mark}] [{user}/{mode}] {test}: {detail}")

def login(u, p):
    s = requests.Session()
    r = s.post(f'{BASE}/api/v1/auth/login', json={'username':u,'password':p})
    return s, r.json().get('success')

def set_mode(s, m):
    s.post(f'{BASE}/api/v1/internal/backend-mode', json={'mode':m})
    time.sleep(1)

# === 准备 SQLite ===
DBPATH = '/home/yhz/real_nifi_data/pima_verify.db'
if not os.path.exists(DBPATH):
    db = sqlite3.connect(DBPATH)
    db.execute('CREATE TABLE pima(Pregnancies INT,Glucose INT,BloodPressure INT,SkinThickness INT,Insulin INT,BMI REAL,DiabetesPedigreeFunc REAL,Age INT,Outcome INT)')
    with open(PIMA) as f:
        for r in csv.DictReader(f):
            db.execute('INSERT INTO pima VALUES(?,?,?,?,?,?,?,?,?)', 
                (int(r['Pregnancies']),int(r['Glucose']),int(r['BloodPressure']),
                 int(r['SkinThickness']),int(r['Insulin']),float(r['BMI']),
                 float(r['DiabetesPedigreeFunction']),int(r['Age']),int(r['Outcome'])))
    db.commit(); db.close()

print("="*60)
print("1. ADMIN + LOCAL: hasTag=true 产物验证")
print("="*60)
s, ok = login('admin', 'admin')
set_mode(s, 'local')

# 1.1 Local 上传带标签
ts = int(time.time())
with open(PIMA, 'rb') as f:
    r = s.post(f'{BASE}/api/v1/upload/inbox_csv',
        params={'convertType':'csv_to_json','hasTag':'true','description':f'tagged_local_{ts}'},
        files={'file':('pima.csv', f.read(), 'text/csv')})
j = r.json()
print(f"  上传: code={j.get('code')} path={j.get('data',{}).get('targetPath','')[:80]}")

# 检查产物是否在 tagged_output 下
user_tagged = '/home/yhz/admin/nifi-data/tagged_output/csv_to_json'
normal_csv_to_json = '/home/yhz/admin/nifi-data/csv_to_json'
tagged_files = [f for f in os.listdir(user_tagged) if 'tagged_local' in f.lower()] if os.path.isdir(user_tagged) else []
normal_files = [f for f in os.listdir(normal_csv_to_json) if 'tagged_local' in f.lower()] if os.path.isdir(normal_csv_to_json) else []
hasTag = bool(r.json().get('data',{}).get('extra',{}).get('hasTag'))
rec('admin','local','上传csv→json(hasTag)', len(tagged_files)>0, 
    f'tagged_output={len(tagged_files)}个, 普通csv_to_json={len(normal_files)}个')

# 1.2 Local DB导出带标签
body={'db_config':{'type':'sqlite','db_type':'sqlite','path':DBPATH},'table':'pima','file_format':'CSV','owner_id':'admin','hasTag':True,'id':f'tagged_db_local_{ts}'}
r = s.post(f'{BASE}/api/v1/export', json=body)
j = r.json()
print(f"  导出: code={j.get('code')} status={j.get('data',{}).get('status','')}")
tagged_out = '/home/yhz/admin/nifi-data/tagged_output/output_csv'
normal_out = '/home/yhz/admin/nifi-data/output_csv'
tagged_files2 = [f for f in os.listdir(tagged_out) if 'tagged_db_local' in f.lower()] if os.path.isdir(tagged_out) else []
normal_files2 = [f for f in os.listdir(normal_out) if 'tagged_db_local' in f.lower()] if os.path.isdir(normal_out) else []
rec('admin','local','DB导出(hasTag)', len(tagged_files2)>0,
    f'tagged_output={len(tagged_files2)}个, 普通output_csv={len(normal_files2)}个')

# 1.3 Local 不带标签对比
with open(PIMA, 'rb') as f:
    r = s.post(f'{BASE}/api/v1/upload/inbox_csv',
        params={'convertType':'csv_to_tsv','hasTag':'false','description':f'notag_local_{ts}'},
        files={'file':('pima.csv', f.read(), 'text/csv')})
j = r.json()
tagged_no = [f for f in os.listdir(user_tagged) if 'notag_local' in f.lower()] if os.path.isdir(user_tagged) else []
normal_no = [f for f in os.listdir(normal_csv_to_json) if 'notag_local' in f.lower()] if os.path.isdir(normal_csv_to_json) else []
# 也检查 tsv
normal_tsv = '/home/yhz/admin/nifi-data/csv_to_tsv'
normal_tsv_files = [f for f in os.listdir(normal_tsv) if 'notag_local' in f.lower()] if os.path.isdir(normal_tsv) else []
rec('admin','local','上传csv→tsv(无tag)', len(tagged_no)==0 and (len(normal_no)>0 or len(normal_tsv_files)>0),
    f'tagged_output={len(tagged_no)}个, 普通={len(normal_no)+len(normal_tsv_files)}个')

print("\n" + "="*60)
print("2. 普通用户 + LOCAL: hasTag=true 产物验证")
print("="*60)
username = f'taggeduser_{ts}'
r = requests.post(f'{BASE}/api/v1/auth/register', json={'username':username,'password':username})
print(f"  注册: {r.json().get('success')}")
s2, ok = login(username, username)
set_mode(s2, 'local')

with open(PIMA, 'rb') as f:
    r = s2.post(f'{BASE}/api/v1/upload/inbox_csv',
        params={'convertType':'csv_to_json','hasTag':'true','description':f'user_tagged_{ts}'},
        files={'file':('pima.csv', f.read(), 'text/csv')})
j = r.json()
user_tagged_dir = f'/home/yhz/{username}/nifi-data/tagged_output/csv_to_json'
tagged_user_files = [f for f in os.listdir(user_tagged_dir) if 'user_tagged' in f.lower()] if os.path.isdir(user_tagged_dir) else []
rec(username,'local','上传csv→json(hasTag)', len(tagged_user_files)>0,
    f'tagged_output={len(tagged_user_files)}个')

# 注册另一个用户做无标签对比
username2 = f'notaguser_{ts}'
r = requests.post(f'{BASE}/api/v1/auth/register', json={'username':username2,'password':username2})
s3, ok = login(username2, username2)
set_mode(s3, 'local')
with open(PIMA, 'rb') as f:
    r = s3.post(f'{BASE}/api/v1/upload/inbox_csv',
        params={'convertType':'csv_to_json','hasTag':'false','description':f'user_notag_{ts}'},
        files={'file':('pima.csv', f.read(), 'text/csv')})
user2_tagged_dir = f'/home/yhz/{username2}/nifi-data/tagged_output/csv_to_json'
user2_normal_dir = f'/home/yhz/{username2}/nifi-data/csv_to_json'
tagged_user2_files = [f for f in os.listdir(user2_tagged_dir) if 'user_notag' in f.lower()] if os.path.isdir(user2_tagged_dir) else []
normal_user2_files = [f for f in os.listdir(user2_normal_dir) if 'user_notag' in f.lower()] if os.path.isdir(user2_normal_dir) else []
rec(username2,'local','上传csv→json(无tag)', len(tagged_user2_files)==0 and len(normal_user2_files)>0,
    f'tagged_output={len(tagged_user2_files)}个, 普通={len(normal_user2_files)}个')

# ═══════════════ NiFi 模式 ═══════════════
print("\n" + "="*60)
print("3. ADMIN + NIFI: hasTag=true 产物验证")
print("="*60)
s4, ok = login('admin', 'admin')
set_mode(s4, 'nifi')
time.sleep(1)

# 清空 inbox
for d in ['convert_jobs/inbox','export_jobs/inbox']:
    p = f'/home/yhz/real_nifi_data/{d}'
    if os.path.isdir(p):
        for f in os.listdir(p):
            if f.endswith('.json'): os.remove(os.path.join(p,f))

# 3.1 NiFi 上传带标签
ts2 = int(time.time())
with open(PIMA, 'rb') as f:
    r = s4.post(f'{BASE}/api/v1/upload/inbox_csv',
        params={'convertType':'csv_to_json','hasTag':'true','description':f'tagged_nifi_{ts2}'},
        files={'file':('pima.csv', f.read(), 'text/csv')})
j = r.json()
print(f"  上传: code={j.get('code')}")

# 等待 NiFi 消费
for i in range(14):
    time.sleep(5)
    tagged_ni = '/home/yhz/admin/real_nifi_data/tagged_output/csv_to_json'
    normal_ni = '/home/yhz/admin/real_nifi_data/csv_to_json'
    tf = [f for f in os.listdir(tagged_ni) if 'tagged_nifi' in f.lower() or 'pima' in f.lower()] if os.path.isdir(tagged_ni) else []
    nf = [f for f in os.listdir(normal_ni) if 'tagged_nifi' in f.lower() or 'pima' in f.lower()] if os.path.isdir(normal_ni) else []
    print(f'  {(i+1)*5}s: tagged_output={len(tf)} 普通csv_to_json={len(nf)}')
    if tf or nf:
        rec('admin','nifi','上传csv→json(hasTag)', len(tf)>0,
            f'tagged_output={len(tf)}个, 普通={len(nf)}个')
        break
else:
    rec('admin','nifi','上传csv→json(hasTag)', False, '超时无产物')

# 3.2 NiFi DB导出带标签
body={'db_config':{'type':'sqlite','db_type':'sqlite','path':'/opt/nifi/nifi-current/data/iot/pima_verify.db'},'table':'pima','file_format':'CSV','owner_id':'admin','hasTag':True,'id':f'tagged_db_nifi_{ts2}'}
r = s4.post(f'{BASE}/api/v1/export', json=body)
print(f"  DB导出: code={r.json().get('code')}")
for i in range(14):
    time.sleep(5)
    t_out = '/home/yhz/admin/real_nifi_data/tagged_output/output_csv'
    n_out = '/home/yhz/admin/real_nifi_data/output_csv'
    tf = [f for f in os.listdir(t_out) if 'tagged_db_nifi' in f.lower()] if os.path.isdir(t_out) else []
    nf = [f for f in os.listdir(n_out) if 'tagged_db_nifi' in f.lower()] if os.path.isdir(n_out) else []
    print(f'  {(i+1)*5}s: tagged_output={len(tf)} 普通output_csv={len(nf)}')
    if tf or nf:
        rec('admin','nifi','DB导出(hasTag)', len(tf)>0,
            f'tagged_output={len(tf)}个, 普通={len(nf)}个')
        break
else:
    rec('admin','nifi','DB导出(hasTag)', False, '超时')

# ═══════════════ 汇总 ═══════════════
print("\n" + "="*60)
print("汇总")
print("="*60)
total = len(results)
passed = sum(1 for r in results if r[3])
print(f"{passed}/{total} 通过")
for u, m, t, ok, d in results:
    print(f"  {'✅' if ok else '❌'} [{u}/{m}] {t}: {d}")
