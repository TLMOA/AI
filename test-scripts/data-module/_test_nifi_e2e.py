"""用容器内路径提交 DB 导出任务，验证 NiFi flow 端到端。"""
import json, os, time

# 容器内路径
DBPATH_CONTAINER = '/opt/nifi/nifi-current/data/iot/verify_export_test.db'

ts = int(time.time())
job_id = f"manual_e2e_{ts}"
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
    "path": DBPATH_CONTAINER,
    "appendToLatest": False,
    "targetDir": "/opt/nifi/nifi-current/data/iot/output_csv",
    "targetRoot": "/opt/nifi/nifi-current/data/iot",
    "submittedAt": time.strftime('%Y-%m-%dT%H:%M:%S+08:00'),
}

# 写入全局 inbox
global_inbox = '/home/yhz/real_nifi_data/export_jobs/inbox'
os.makedirs(global_inbox, exist_ok=True)
task_path = os.path.join(global_inbox, f'{job_id}.json')
with open(task_path, 'w') as f:
    json.dump(task, f, ensure_ascii=False, indent=2)
print(f'写入任务: {task_path} ({os.path.getsize(task_path)} bytes)')

# 等 NiFi 消费
print('\n=== 等待 NiFi 消费（最多 60 秒）===')
for i in range(12):
    time.sleep(5)
    inbox_files = [f for f in os.listdir(global_inbox) if f.endswith('.json')]
    done = '/home/yhz/real_nifi_data/export_jobs/done'
    error = '/home/yhz/real_nifi_data/export_jobs/error'
    output = '/home/yhz/real_nifi_data/output_csv'
    done_files = os.listdir(done) if os.path.isdir(done) else []
    error_files = os.listdir(error) if os.path.isdir(error) else []
    output_files = [f for f in os.listdir(output) if 'verify' in f.lower()] if os.path.isdir(output) else []
    print(f'  {(i+1)*5}s: inbox={len(inbox_files)} done={len(done_files)} error={len(error_files)} output={len(output_files)}')
    if output_files:
        print(f'    产出: {output_files}')
        for f in output_files:
            p = os.path.join(output, f)
            print(f'    {f} ({os.path.getsize(p)} bytes)')
            if f.endswith('.csv'):
                print(f'      内容: {open(p).read()[:200]}')
        break
    if error_files:
        for f in error_files:
            p = os.path.join(error, f)
            try:
                d = json.load(open(p))
                print(f'    error {f}: {d.get("message","")[:300]}')
            except:
                print(f'    error {f}: (无法解析)')
        break
    if done_files:
        for f in done_files:
            p = os.path.join(done, f)
            try:
                d = json.load(open(p))
                print(f'    done {f}: status={d.get("status")} path={d.get("resultPath","")}')
            except:
                pass
        break
