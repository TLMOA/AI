"""测试 NiFi 转换 flow 端到端：上传 CSV → 转换为 JSON。"""
import json, os, time, shutil

# 准备一个测试 CSV 文件放到 inbox_csv（容器内路径 /opt/nifi/nifi-current/data/iot/inbox_csv）
src_csv = '/home/yhz/real_nifi_data/inbox_csv/nifi_convert_test.csv'
with open(src_csv, 'w') as f:
    f.write('a,b\n1,hello\n2,world\n')
print(f'创建测试 CSV: {src_csv} ({os.path.getsize(src_csv)} bytes)')

# 容器内路径
container_src = '/opt/nifi/nifi-current/data/iot/inbox_csv/nifi_convert_test.csv'
container_target_csv = '/opt/nifi/nifi-current/data/iot/csv_to_json'

ts = int(time.time())
job_id = f"convert_e2e_{ts}"
task = {
    "jobId": job_id,
    "username": "admin",
    "ownerId": "admin",
    "sourcePath": container_src,
    "sourceFormat": "csv",
    "targetFormats": ["json"],
    "fileName": "nifi_convert_test.csv",
    "targetDir": container_target_csv,
    "targetRoot": "/opt/nifi/nifi-current/data/iot",
    "submittedAt": time.strftime('%Y-%m-%dT%H:%M:%S+08:00'),
}

# 写入全局 convert_jobs/inbox
global_inbox = '/home/yhz/real_nifi_data/convert_jobs/inbox'
os.makedirs(global_inbox, exist_ok=True)
task_path = os.path.join(global_inbox, f'{job_id}.json')
with open(task_path, 'w') as f:
    json.dump(task, f, ensure_ascii=False, indent=2)
print(f'写入转换任务: {task_path} ({os.path.getsize(task_path)} bytes)')

# 等待 NiFi 消费
print('\n=== 等待 NiFi 转换 flow 消费（最多 60 秒）===')
for i in range(12):
    time.sleep(5)
    inbox_files = [f for f in os.listdir(global_inbox) if f.endswith('.json')]
    done = '/home/yhz/real_nifi_data/convert_jobs/done'
    error = '/home/yhz/real_nifi_data/convert_jobs/error'
    output = '/home/yhz/real_nifi_data/csv_to_json'
    done_files = os.listdir(done) if os.path.isdir(done) else []
    error_files = os.listdir(error) if os.path.isdir(error) else []
    output_files = [f for f in os.listdir(output) if 'nifi_convert' in f.lower()] if os.path.isdir(output) else []
    print(f'  {(i+1)*5}s: inbox={len(inbox_files)} done={len(done_files)} error={len(error_files)} output={len(output_files)}')
    if output_files:
        print(f'    产出: {output_files}')
        for f in output_files:
            p = os.path.join(output, f)
            print(f'    {f} ({os.path.getsize(p)} bytes)')
            content = open(p).read()[:200]
            print(f'      内容: {content}')
        break
    if error_files:
        for f in error_files:
            p = os.path.join(error, f)
            try:
                d = json.load(open(p))
                print(f'    error {f}: {d.get("message","")[:300]}')
            except:
                print(f'    error {f}: {open(p).read()[:300]}')
        break
    if done_files:
        for f in done_files:
            p = os.path.join(done, f)
            try:
                d = json.load(open(p))
                print(f'    done {f}: status={d.get("status")} msg={d.get("message","")[:200]}')
            except:
                pass
        break
