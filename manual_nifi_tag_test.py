#!/usr/bin/env python3
import json
import os
import time
import requests

BASE = "http://127.0.0.1:5174"
TEST_USER = f"manualtag_{int(time.time())}"
TEST_PASS = "Test@123"
TEST_FILE = "/home/yhz/iot/test_upload.csv"

def call(session, method, path, **kwargs):
    r = session.request(method, BASE + path, **kwargs)
    try:
        return r.status_code, r.json(), r
    except Exception:
        return r.status_code, r.text, r

# admin login
admin = requests.Session()
code, data, _ = call(admin, "POST", "/api/v1/auth/login", json={"username": "admin", "password": "admin"})
assert code == 200 and data.get("success"), f"admin login failed: {code} {data}"
print("admin login ok")

# register user
code, data, _ = call(requests, "POST", "/api/v1/auth/register", json={"username": TEST_USER, "password": TEST_PASS, "deployment_mode": "public"})
assert code == 200 and data.get("success"), f"register failed: {code} {data}"
print(f"registered {TEST_USER}")

# user login
sess = requests.Session()
code, data, _ = call(sess, "POST", "/api/v1/auth/login", json={"username": TEST_USER, "password": TEST_PASS})
assert code == 200 and data.get("success"), f"user login failed: {code} {data}"
print("user login ok")

# switch to nifi
code, data, _ = call(sess, "POST", "/api/v1/internal/backend-mode", json={"username": TEST_USER, "mode": "nifi", "operator": TEST_USER})
print(f"switch mode: {code} {data}")

# upload
code, data, _ = call(sess, "POST", "/api/v1/files/upload",
                     files={"file": ("test_upload.csv", open(TEST_FILE, "rb"), "text/csv")},
                     data={"auto_convert": "true", "output_format": "CSV"})
print(f"upload: {code} {json.dumps(data, ensure_ascii=False)[:200]}")

# list files
code, data, _ = call(sess, "GET", "/api/v1/files")
rows = data.get("data", {}).get("rows", [])
print(f"files: {len(rows)}")
for r in rows:
    print("  ", r.get("fileId"), r.get("fileName"), r.get("storagePath"))
file_id = rows[0].get("fileId") if rows else None
print(f"using file_id={file_id}")

# auto tag
code, data, _ = call(sess, "POST", "/api/v1/tags/auto", json={"fileId": file_id, "operator": TEST_USER})
print(f"auto_tag: {code} {json.dumps(data, ensure_ascii=False)[:400]}")

# wait a bit and inspect dirs
print("\n--- global dirs ---")
for d in ["/home/yhz/real_nifi_data/tagging_jobs/inbox", "/home/yhz/real_nifi_data/tagging_jobs/done", "/home/yhz/real_nifi_data/tagging_jobs/error", "/home/yhz/real_nifi_data/tagged_output"]:
    print(d)
    if os.path.isdir(d):
        for f in os.listdir(d):
            print("  ", f)

print("\n--- user dirs ---")
user_root = f"/home/yhz/{TEST_USER}"
for root, dirs, files in os.walk(user_root):
    for f in files:
        if "tag" in f.lower() or "tagging" in root:
            print(os.path.join(root, f))
