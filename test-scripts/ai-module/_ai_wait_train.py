#!/usr/bin/env python3
"""等待 AI 训练任务完成（通过后端 status 接口轮询）"""
import requests, time, sys, json

TASK = sys.argv[1] if len(sys.argv) > 1 else ""
TIMEOUT = int(sys.argv[2]) if len(sys.argv) > 2 else 1800

if not TASK:
    import glob
    fs = sorted(glob.glob("/home/yhz/solo_ai_iot/banckend/storage/tasks/admin/*.json"),
                key=lambda f: __import__('os').path.getmtime(f), reverse=True)
    if not fs:
        print("NO TASK"); sys.exit(1)
    TASK = __import__('os').path.splitext(__import__('os').path.basename(fs[0]))[0]

end = time.time() + TIMEOUT
while time.time() < end:
    try:
        r = __import__('requests').get(f"http://127.0.0.1:8002/api/train/status/{TASK}?user_id=admin", timeout=10).json()
        data = r.get("data", {})
        status = data.get("status", "?")
        stage = data.get("stage", "")
        models = data.get("models", [])
        done = [m for m in models if m.get("status") in ("completed", "failed")]
        print(f"[{time.strftime('%H:%M:%S')}] status={status} stage={stage} models={len(done)}/{len(models)}", flush=True)
        for m in models:
            print(f"    {m.get('model_name')}: f1={m.get('f1_score')} acc={m.get('accuracy')} status={m.get('status')}", flush=True)
        if status in ("completed", "failed"):
            print("=== FINAL ===")
            print(json.dumps(data, ensure_ascii=False, indent=2))
            sys.exit(0 if status == "completed" else 1)
    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] poll error: {e}", flush=True)
    time.sleep(5)
print("TIMEOUT")
sys.exit(2)