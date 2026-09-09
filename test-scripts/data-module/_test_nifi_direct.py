"""直接向全局 inbox 写入 DB 导出任务，测试 NiFi flow 是否消费并落盘。"""
import json, os, time, sqlite3

# 准备 SQLite 测试库
DBPATH = '/tmp/verify_export_test.db'
if not os.path.exists(DBPATH):
    db = sqlite3.connect(DBPATH)
    db.execute('create table verify_t(a int, b text)')
    db.executemany('insert into verify_t values(?,?)', [(i,'row%d'%i) for i in range(3)])
    db.commit(); db.close()
    print('重建测试库')

# 构造任务 JSON（与 _build_nifi_export_task 格式一致）
ts = int(time.time())
job_id = f"manual_direct_{ts}"
task = {
    "jobId": job_id,
    "username": "admin",
    "ownerId": "admin",
    "dbType": "sqlite",
    "format": "CSV",
    "table": "verify_t",
    "where": "",
    "host": "",
    "port": 0,
    "user": "",
    "password": "",
    "database": "",
    "path": DBPATH,
    "appendToLatest": False,
    "targetDir": "/opt/nifi/nifi-current/data/iot/output_csv",
    "targetRoot": "/opt/nifi/nifi-current/data/iot",
    "submittedAt": time.strftime('%Y-%m-%dT%H:%M:%S+08:00'),
}

# 写入全局 inbox（NiFi 容器挂载点）
global_inbox = '/home/yhz/real_nifi_data/export_jobs/inbox'
os.makedirs(global_inbox, exist_ok=True)
task_path = os.path.join(global_inbox, f'{job_id}.json')
with open(task_path, 'w') as f:
    json.dump(task, f, ensure_ascii=False, indent=2)
print(f'写入全局 inbox: {task_path} ({os.path.getsize(task_path)} bytes)')

# 也写入用户 inbox
user_inbox = '/home/yhz/admin/real_nifi_data/export_jobs/inbox'
os.makedirs(user_inbox, exist_ok=True)
user_task_path = os.path.join(user_inbox, f'{job_id}.json')
with open(user_task_path, 'w') as f:
    json.dump(task, f, ensure_ascii=False, indent=2)
print(f'写入用户 inbox: {user_task_path}')

# 等 NiFi GetFile 消费（polling 30s）
print('\n=== 等待 NiFi 消费（最多 60 秒）===')
global_done = '/home/yhz/real_nifi_data/export_jobs/done'
global_error = '/home/yhz/real_nifi_data/export_jobs/error'
output_csv = '/home/yhz/real_nifi_data/output_csv'

for i in range(12):
    time.sleep(5)
    inbox_files = [f for f in os.listdir(global_inbox) if f.endswith('.json')] if os.path.isdir(global_inbox) else []
    done_files = [f for f in os.listdir(global_done) if f.endswith('.json')] if os.path.isdir(global_done) else []
    error_files = [f for f in os.listdir(global_error) if f.endswith('.json')] if os.path.isdir(global_error) else []
    output_files = [f for f in os.listdir(output_csv) if 'verify' in f.lower()] if os.path.isdir(output_csv) else []
    print(f'  {(i+1)*5}s: inbox={len(inbox_files)} done={len(done_files)} error={len(error_files)} output_csv={len(output_files)}')
    if output_files:
        print(f'    产出: {output_files}')
        for f in output_files:
            p = os.path.join(output_csv, f)
            print(f'    {f} ({os.path.getsize(p)} bytes)')
            if f.endswith('.csv'):
                print(f'      内容: {open(p).read()[:200]}')
        break
    if done_files:
        # 看 done 里的任务状态
        for f in done_files:
            p = os.path.join(global_done, f)
            try:
                d = json.load(open(p))
                print(f'    done {f}: status={d.get("status")} msg={d.get("message","")[:100]}')
            except:
                pass
    if error_files:
        for f in error_files:
            p = os.path.join(global_error, f)
            try:
                d = json.load(open(p))
                print(f'    error {f}: {d.get("message","")[:200]}')
            except:
                pass
        break
