import requests, time
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

s = requests.Session()
r = s.post('https://localhost:8080/nifi-api/access/token',
    data='username=admin&password=admin@nifi123',
    headers={'Content-Type':'application/x-www-form-urlencoded'}, verify=False)
print('login:', r.status_code)
s.headers.update({'Authorization':'Bearer '+r.text.strip()})

# 先尝试删 connection（如果还存在）
try:
    r3 = s.get('https://localhost:8080/nifi-api/connections/7de1dae5-019e-1000-3737-566e5b0bb198', verify=False)
    if r3.status_code == 200:
        new_ver = r3.json()['revision']['version']
        r4 = s.delete('https://localhost:8080/nifi-api/connections/7de1dae5-019e-1000-3737-566e5b0bb198?version='+str(new_ver)+'&clientId=cl3', verify=False)
        print('delete conn:', r4.status_code)
    else:
        print('conn already gone:', r3.status_code)
except Exception as e:
    print('conn cleanup skip:', e)

time.sleep(1)

# 删除 processor
r5 = s.get('https://localhost:8080/nifi-api/process-groups/root/processors', verify=False)
procs = r5.json().get('processors', [])
print('current procs:', len(procs))
for p in procs:
    pid = p['id']
    name = p['component']['name']
    # 重新获取最新 revision
    new_ver = s.get('https://localhost:8080/nifi-api/processors/'+pid, verify=False).json()['revision']['version']
    r6 = s.delete('https://localhost:8080/nifi-api/processors/'+pid+'?version='+str(new_ver)+'&clientId=cl3', verify=False)
    print('delete', name, ':', r6.status_code)
    if r6.status_code != 200:
        print('  body:', r6.text[:200])

time.sleep(1)
r = s.get('https://localhost:8080/nifi-api/process-groups/root/processors', verify=False)
print('remaining proc:', len(r.json()['processors']))
r = s.get('https://localhost:8080/nifi-api/process-groups/root/connections', verify=False)
print('remaining conn:', len(r.json()['connections']))
