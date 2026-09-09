"""tagged 独立顶层目录专项测试：admin/zzz × Local/NiFi，pima 数据集
验证 hasTag=true 时：源文件、转换产物、DB 导出全部入 tagged_nifi_data / tagged_real_nifi_data
"""
import requests, time, os, glob, json, csv, sqlite3

BASE = 'http://127.0.0.1:8081'
PIMA = '/home/yhz/iot/pima-dataset.csv'
DBPATH = '/home/yhz/real_nifi_data/pima_db.db'
DBPATH_CT = '/opt/nifi/nifi-current/data/iot/pima_db.db'
results = []

def R(user, mode, test, ok, detail):
    results.append((user, mode, test, ok, detail))
    print(f"  {'✅' if ok else '❌'} [{user}/{mode}] {test}: {detail}")

def login(u, p):
    s = requests.Session()
    r = s.post(f'{BASE}/api/v1/auth/login', json={'username': u, 'password': p})
    return s, r.json().get('success')

def set_mode(s, m):
    s.post(f'{BASE}/api/v1/internal/backend-mode', json={'mode': m})
    time.sleep(1)

# 准备 SQLite（若不存在）
if not os.path.exists(DBPATH):
    db = sqlite3.connect(DBPATH)
    db.execute('CREATE TABLE pima(Preg INT,Glucose INT,BP INT,ST INT,Insulin INT,BMI REAL,DPF REAL,Age INT,Outcome INT)')
    with open(PIMA) as f:
        for r in csv.DictReader(f):
            db.execute('INSERT INTO pima VALUES(?,?,?,?,?,?,?,?,?)', (int(r['Pregnancies']),int(r['Glucose']),int(r['BloodPressure']),int(r['SkinThickness']),int(r['Insulin']),float(r['BMI']),float(r['DiabetesPedigreeFunction']),int(r['Age']),int(r['Outcome'])))
    db.commit(); db.close()
    print('已创建 pima_db.db')

# ═══════════ Local 模式 ═══════════
print("="*60)
print("1. LOCAL 模式 — hasTag=true 上传 + DB 导出")
print("="*60)

# admin + Local
s,_ = login('admin','admin'); set_mode(s,'local')
with open(PIMA,'rb') as f:
    r = s.post(f'{BASE}/api/v1/upload/inbox_csv', params={'convertType':'csv_to_json','hasTag':'true','description':'tlt_admin'},
               files={'file':('pima.csv',f.read(),'text/csv')})
d = r.json().get('data',{}) or {}
sp, tp = d.get('sourcePath',''), d.get('targetPath','')
src_ok = '/tagged_nifi_data/inbox_csv/' in sp
tgt_ok = '/tagged_nifi_data/csv_to_json/' in tp if tp else False
R('admin','local','上传源文件→tagged_nifi_data', src_ok, f'src={sp}')
R('admin','local','转换产物→tagged_nifi_data', tgt_ok, f'tgt={tp}')

# zzz + Local
s2,_ = login('zzz','zzz'); set_mode(s2,'local')
with open(PIMA,'rb') as f:
    r = s2.post(f'{BASE}/api/v1/upload/inbox_csv', params={'convertType':'csv_to_json','hasTag':'true','description':'tlt_zzz'},
                files={'file':('p.csv',f.read(),'text/csv')})
d = r.json().get('data',{}) or {}
sp, tp = d.get('sourcePath',''), d.get('targetPath','')
R('zzz','local','上传源文件→tagged_nifi_data', '/tagged_nifi_data/inbox_csv/' in sp, f'src={sp}')
R('zzz','local','转换产物→tagged_nifi_data', '/tagged_nifi_data/csv_to_json/' in tp if tp else False, f'tgt={tp}')

# admin Local DB导出 hasTag=true
r = s.post(f'{BASE}/api/v1/export', json={'db_config':{'type':'sqlite','db_type':'sqlite','path':DBPATH},'table':'pima','file_format':'CSV','owner_id':'admin','hasTag':True,'id':'tlt_db'})
d = r.json().get('data',{}) or {}
tp = d.get('path','') or d.get('filePath','')
tagged_csvs = glob.glob('/home/yhz/admin/tagged_nifi_data/output_csv/*.csv')
R('admin','local','DB导出→tagged_nifi_data/output_csv', len(tagged_csvs)>0, f'tagged_csv={len(tagged_csvs)}')

# ═════════════ NiFi 模式 ═════════════
print("\n"+"="*60)
print("2. NIFI 模式 · hasTag=true 上传 + DB 导出")
print("="*60)

# admin NiFi
s3,_ = login('admin','admin'); set_mode(s3,'nifi'); time.sleep(1)
with open(PIMA,'rb') as f:
    r = s3.post(f'{BASE}/api/v1/upload/inbox_csv', params={'convertType':'csv_to_json','hasTag':'true','description':'tltn_admin'},
                files={'file':('p.csv',f.read(),'text/csv')})
d = r.json().get('data',{}) or {}
sp = d.get('sourcePath','')
R('admin','nifi','源文件→tagged_real_nifi_data', '/tagged_real_nifi_data/inbox_csv/' in sp, f'src={sp}')

# zzz NiFi
s4,_ = login('zzz','zzz'); set_mode(s4,'nifi'); time.sleep(1)
with open(PIMA,'rb') as f:
    r = s4.post(f'{BASE}/api/v1/upload/inbox_csv', params={'convertType':'csv_to_json','hasTag':'true','description':'tltn_zzz'},
                files={'file':('p.csv',f.read(),'text/csv')})
d = r.json().get('data',{}) or {}
sp = d.get('sourcePath','')
R('zzz','nifi','源文件→tagged_real_nifi_data', '/tagged_real_nifi_data/inbox_csv/' in sp, f'src={sp}')

# 等待 NiFi 异步转换完成（最多 90 秒）
# 记录 admin 和 zzz 的上传时间戳，用于匹配实际输出文件名
tsn = time.strftime('%Y%m%d_%H%M%S')
admin_ts = tsn  # 占位，实际用文件名模糊匹配
print("\n等待 NiFi 异步转换...")
for i in range(18):
    time.sleep(5)
    admin_global = glob.glob('/home/yhz/tagged_real_nifi_data/csv_to_json/xform_raw_admin_*csv2json.json')
    zzz_global = glob.glob('/home/yhz/tagged_real_nifi_data/csv_to_json/xform_raw_zzz_*csv2json.json')
    admin_user = glob.glob('/home/yhz/admin/tagged_real_nifi_data/csv_to_json/xform_raw_admin_*csv2json.json')
    zzz_user = glob.glob('/home/yhz/zzz/tagged_real_nifi_data/csv_to_json/xform_raw_zzz_*csv2json.json')
    print(f'  {(i+1)*5}s: admin_global={len(admin_global)} zzz_global={len(zzz_global)} admin_user={len(admin_user)} zzz_user={len(zzz_user)}')
    if admin_user and zzz_user:
        break

# 触发路由（文件列表查询）
for u in ['admin','zzz']:
    ss=requests.Session()
    ss.post(f'{BASE}/api/v1/auth/login',json={'username':u,'password':u})
    ss.get(f'{BASE}/api/v1/files?pageSize=2')
time.sleep(3)

# 验证 NiFi 转换产物（全局 + 路由到用户）
admin_global = glob.glob('/home/yhz/tagged_real_nifi_data/csv_to_json/xform_raw_admin_*csv2json.json')
zzz_global = glob.glob('/home/yhz/tagged_real_nifi_data/csv_to_json/xform_raw_zzz_*csv2json.json')
admin_user = glob.glob('/home/yhz/admin/tagged_real_nifi_data/csv_to_json/xform_raw_admin_*csv2json.json')
zzz_user = glob.glob('/home/yhz/zzz/tagged_real_nifi_data/csv_to_json/xform_raw_zzz_*csv2json.json')
R('admin','nifi','转换产物在全局或用户 tagged_real_nifi_data', len(admin_global)>0 or len(admin_user)>0, f'global={len(admin_global)} user={len(admin_user)}')
R('zzz','nifi','转换产物在全局或用户 tagged_real_nifi_data', len(zzz_global)>0 or len(zzz_user)>0, f'global={len(zzz_global)} user={len(zzz_user)}')
R('admin','nifi','转换产物路由到用户 tagged_real_nifi_data', len(admin_user)>0, f'user={len(admin_user)}')
R('zzz','nifi','转换产物路由到用户 tagged_real_nifi_data', len(zzz_user)>0, f'user={len(zzz_user)}')

# 验证 NiFi DB导出 hasTag=true
r = s3.post(f'{BASE}/api/v1/export', json={'db_config':{'type':'sqlite','db_type':'sqlite','path':DBPATH_CT},'table':'pima','file_format':'CSV','owner_id':'admin','hasTag':True,'id':'tltn_db'})
print(f'  DB导出 code={r.json().get("code")}')
for i in range(14):
    time.sleep(5)
    admin_t = glob.glob('/home/yhz/admin/tagged_real_nifi_data/output_csv/*pima*.csv')
    admin_g = glob.glob('/home/yhz/tagged_real_nifi_data/output_csv/*pima*.csv')
    print(f'  {(i+1)*5}s: admin_user={len(admin_t)} admin_global={len(admin_g)}')
    if admin_t or admin_g:
        R('admin','nifi','DB导出→tagged_real_nifi_data/output_csv', len(admin_t)>0 or len(admin_g)>0, f'user={len(admin_t)} global={len(admin_g)}')
        break

# ═══════════ 汇总 ═══════════
print("\n"+"="*60)
print("汇总")
print("="*60)
total = len(results); passed = sum(1 for r in results if r[3])
print(f"{passed}/{total} 通过")
for u,m,t,ok,d in results:
    print(f"  {'✅' if ok else '❌'} [{u}/{m}] {t}: {d}")
with open('/home/yhz/iot/tagged_toplevel_result.json','w') as fh:
    json.dump({'total':total,'passed':passed,'results':[{'user':u,'mode':m,'test':t,'ok':ok,'detail':d} for u,m,t,ok,d in results]}, fh, ensure_ascii=False, indent=2)