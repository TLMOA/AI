"""验证 tagged 独立顶层目录：admin/zzz × Local/NiFi，hasTag=true 上传"""
import requests, time, os, glob

def check(user, mode, label):
    s = requests.Session()
    s.post('http://127.0.0.1:8081/api/v1/auth/login', json={'username': user, 'password': user})
    s.post('http://127.0.0.1:8081/api/v1/internal/backend-mode', json={'mode': mode})
    time.sleep(1)
    with open('pima-dataset.csv', 'rb') as f:
        r = s.post('http://127.0.0.1:8081/api/v1/upload/inbox_csv',
                   params={'convertType': 'csv_to_json', 'hasTag': 'true', 'description': label},
                   files={'file': ('p.csv', f.read(), 'text/csv')})
    d = r.json().get('data', {}) or {}
    sp = d.get('sourcePath', '')
    tp = d.get('targetPath', '')
    # 源文件路径验证
    if mode == 'local':
        src_ok = f'/tagged_nifi_data/inbox_csv/' in sp
        tgt_ok = f'/tagged_nifi_data/csv_to_json/' in tp if tp else None
    else:
        src_ok = f'/tagged_real_nifi_data/inbox_csv/' in sp
        tgt_ok = f'/tagged_real_nifi_data/csv_to_json/' in tp if tp else None
    print(f'[{user}/{mode}] 源路径含tagged顶层: {src_ok} | 目标含tagged顶层: {tgt_ok}')
    print(f'  源: {sp}')
    print(f'  目标: {tp}')

check('admin', 'local', 'vvl1')
check('zzz', 'local', 'vvl2')
check('admin', 'nifi', 'vvn1')
time.sleep(35)
check('zzz', 'nifi', 'vvn2')
time.sleep(35)
