"""datasetName 完整验证：admin/zzz × Local/NiFi，上传源meta + 导出meta"""
import requests, time, os, glob, sys
sys.path.insert(0, 'v1-backend')
from app import meta_json
BASE='http://127.0.0.1:8081'
PIMA='/home/yhz/iot/pima-dataset.csv'
results=[]

def R(u,m,t,ok,d):
    results.append((u,m,t,ok,d)); print(f"  {'✅' if ok else '❌'} [{u}/{m}] {t}: {d}")

def login(u):
    s=requests.Session(); s.post(f'{BASE}/api/v1/auth/login',json={'username':u,'password':u}); return s

def set_mode(s,m):
    s.post(f'{BASE}/api/v1/internal/backend-mode',json={'mode':m}); time.sleep(1)

# === 上传 datasetName（4 场景）===
for user,mode,ds,label in [('admin','local','硬盘故障','al'),('zzz','local','温度预测','zl'),
                            ('admin','nifi','硬盘N','an'),('zzz','nifi','温度N','zn')]:
    s=login(user); set_mode(s,mode)
    with open(PIMA,'rb') as f:
        r=s.post(f'{BASE}/api/v1/upload/inbox_csv',
            params={'convertType':'csv_to_json','hasTag':'true','description':label,'datasetName':ds},
            files={'file':('p.csv',f.read(),'text/csv')})
    d=r.json().get('data',{}) or {}; sp=d.get('sourcePath','')
    got=''
    if sp and os.path.exists(sp):
        try: got=meta_json.read_meta(sp).get('datasetName','')
        except: pass
    R(user,mode,'上传datasetName', got==ds, f'源meta={got!r}')

# ---- 导出 Local（2 场景）----
for user,ds in [('admin','硬盘导出'),('zzz','温度导出')]:
    s=login(user); set_mode(s,'local')
    r=s.post(f'{BASE}/api/v1/export',json={'db_config':{'type':'sqlite','db_type':'sqlite','path':'/home/yhz/real_nifi_data/pima_db.db'},'table':'pima','file_format':'CSV','owner_id':user,'hasTag':True,'id':f'dsf_{user}','datasetName':ds})
    d=f'/home/yhz/{user}/tagged_nifi_data/output_csv'
    files=sorted(glob.glob(d+'/*.csv'),key=os.path.getmtime)
    got=''
    if files:
        try: got=meta_json.read_meta(files[-1]).get('datasetName','')
        except: pass
    R(user,'local','导出datasetName', got==ds, f"got={got!r}")

# ---- 导出 NiFi（2 场景）----
for user,ds in [('admin','硬盘NiFi导出'),('zzz','温度NiFi导出')]:
    s=login(user); set_mode(s,'nifi')
    r=s.post(f'{BASE}/api/v1/export',json={'db_config':{'type':'sqlite','db_type':'sqlite','path':'/opt/nifi/nifi-current/data/iot/pima_db.db'},'table':'pima','file_format':'CSV','owner_id':user,'hasTag':True,'id':f'dsnf_{user}','datasetName':ds})
    d=f'/home/yhz/{user}/tagged_real_nifi_data/output_csv'
    before=len(glob.glob(d+'/*.csv'))
    for i in range(16):
        time.sleep(5)
        s.get(f'{BASE}/api/v1/files?pageSize=2')  # 触发回流
        files=sorted(glob.glob(d+'/*.csv'),key=os.path.getmtime)
        if len(files)>before:
            got=''
            try: got=meta_json.read_meta(files[-1]).get('datasetName','')
            except: pass
            R(user,'nifi','导出datasetName', got==ds, f"got={got!r}")
            break
    else:
        R(user,'nifi','导出datasetName', False, '超时')

# 汇总
print("\n"+"="*40)
t=len(results); p=sum(1 for r in results if r[3])
print(f"{p}/{t} 通过")
for u,m,tt,ok,d in results: print(f"  {'✅' if ok else '❌'} [{u}/{m}] {tt}: {d}")