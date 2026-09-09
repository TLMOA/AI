"""验证 datasetName 字段：admin/zzz × Local/NiFi，上传+导出"""
import requests, time, os, glob, json

BASE = 'http://127.0.0.1:8081'
results = []

def R(u, m, t, ok, d):
    results.append((u, m, t, ok, d))
    print(f"  {'✅' if ok else '❌'} [{u}/{m}] {t}: {d}")

def login(u, p):
    s = requests.Session()
    s.post(f'{BASE}/api/v1/auth/login', json={'username': u, 'password': u})
    return s

def set_mode(s, m):
    s.post(f'{BASE}/api/v1/internal/backend-mode', json={'mode': m})
    time.sleep(1)

# admin + Local 上传带 datasetName
s = login('admin','admin'); set_mode(s,'local')
with open('pima-dataset.csv','rb') as f:
    r = s.post(f'{BASE}/api/v1/upload/inbox_csv',
        params={'convertType':'csv_to_json','hasTag':'true','description':'ds_test','datasetName':'硬盘故障'},
        files={'file':('pima.csv',f.read(),'text/csv')})
d = r.json().get('data',{}) or {}
tp = d.get('targetPath','')
# 检查目标文件的 meta 是否有 datasetName
import os
if tp and os.path.exists(tp):
    meta_p = tp + '.meta.json'
    if os.path.exists(meta_p):
        meta = json.load(open(meta_p))
        ds = meta.get('datasetName','')
        R('admin','local','上传 datasetName 写入 meta', ds=='硬盘故障', f'datasetName={ds}')
    else:
        R('admin','local','上传 datasetName 写入', False, '无 meta.json')
else:
    # 找 tagged_nifi_data 下的产物
    files = glob.glob('/home/yhz/admin/tagged_nifi_data/csv_to_json/*pima*')
    if files:
        meta_p = files[0] + '.meta.json'
        if os.path.exists(meta_p):
            meta = json.load(open(meta_p))
            ds = meta.get('datasetName','')
            R('admin','local','上传 datasetName 写入', ds=='硬盘故障', f'datasetName={ds}')
        else:
            R('admin','local','上传 datasetName 写入', False, '无 meta.json')
    else:
        R('admin','local','上传 datasetName 写入', False, '无产物')

# admin + Local DB导出带 datasetName
r = s.post(f'{BASE}/api/v1/export', json={'db_config':{'type':'sqlite','db_type':'sqlite','path':'/home/yhz/real_nifi_data/pima_db.db'},'table':'pima','file_format':'CSV','owner_id':'admin','hasTag':True,'id':'ds_db','datasetName':'硬盘故障导出'})
d = r.json().get('data',{}) or {}
tp = d.get('path','') or d.get('filePath','')
tagged_csvs = glob.glob('/home/yhz/admin/tagged_nifi_data/output_csv/*.csv')
if tagged_csvs:
    meta_p = tagged_csvs[-1] + '.meta.json'
    ds = ''
    if os.path.exists(meta_p):
        ds = json.load(open(meta_p)).get('datasetName','')
    R('admin','local','DB导出 datasetName 写入', ds=='硬盘故障导出', f'datasetName={ds}')
else:
    R('admin','local','DB导出 datasetName 写入', False, '无产物')

# 汇总
print("\n"+"="*40)
t=len(results); p=sum(1 for r in results if r[3])
print(f"{p}/{t} 通过")