#!/usr/bin/env python3
"""端到端测试：注册 -> 目录检查 -> 登录 -> 上传 -> 自动标签 -> 普通用户 API"""
import os
import sys
import time
from datetime import datetime
import requests

BASE = "http://127.0.0.1:5174"
ADMIN_USER = "admin"
ADMIN_PASS = "admin"

ts = datetime.now().strftime("%m%d%H%M%S")
TEST_USER = f"testauto_{ts}"
TEST_PASS = "Test@123"
TEST_FILE = "/home/yhz/iot/test_upload.csv"

def call(session, method, path, **kwargs):
    r = session.request(method, BASE + path, **kwargs)
    try:
        data = r.json()
    except Exception:
        data = r.text
    return r.status_code, data, r

# ========================
# 1. 管理员登录
# ========================
admin_session = requests.Session()
code, data, _ = call(admin_session, "POST", "/api/v1/auth/login",
                     json={"username": ADMIN_USER, "password": ADMIN_PASS})
assert code == 200 and isinstance(data, dict) and data.get("success"), \
    f"admin 登录失败: {code} {data}"
print(f"[OK] admin 登录成功")

# 如果测试用户已存在，先删除
code, data, _ = call(admin_session, "GET", "/api/v1/internal/users")
users = []
if isinstance(data, dict) and data.get("code") == 0:
    users = data.get("data", {}).get("users", [])
for u in users:
    if u.get("username") == TEST_USER:
        code, data, _ = call(admin_session, "DELETE",
                             f"/api/v1/internal/users/{TEST_USER}?purge_data=true")
        print(f"[INFO] 删除已存在用户 {TEST_USER}: {code} {data}")
        break

# ========================
# 2. 注册新用户
# ========================
code, data, _ = call(requests, "POST", "/api/v1/auth/register", json={
    "username": TEST_USER,
    "password": TEST_PASS,
    "deployment_mode": "public",
})
assert code == 200 and isinstance(data, dict) and data.get("success"), \
    f"注册失败: {code} {data}"
print(f"[OK] 注册用户 {TEST_USER} 成功")

# ========================
# 3. 检查目录自动创建
# ========================
user_root = f"/home/yhz/{TEST_USER}"
expected_subs = [
    "nifi-data/output_csv", "nifi-data/output_json", "nifi-data/output_tsv",
    "nifi-data/silent_exports", "nifi-data/inbox_csv", "nifi-data/inbox_json",
    "nifi-data/tagged_output", "nifi-data/export_jobs",
    "real_nifi_data/output_csv", "real_nifi_data/output_json", "real_nifi_data/output_tsv",
    "real_nifi_data/silent_exports", "real_nifi_data/inbox_csv", "real_nifi_data/inbox_json",
    "real_nifi_data/tagged_output", "real_nifi_data/export_jobs",
]
missing = [sub for sub in expected_subs if not os.path.isdir(os.path.join(user_root, sub))]
assert not missing, f"目录缺失: {missing}"
print(f"[OK] 用户目录及 {len(expected_subs)} 个子目录已自动创建")

# ========================
# 4. 新用户登录
# ========================
user_session = requests.Session()
code, data, _ = call(user_session, "POST", "/api/v1/auth/login",
                     json={"username": TEST_USER, "password": TEST_PASS})
assert code == 200 and isinstance(data, dict) and data.get("success"), \
    f"{TEST_USER} 登录失败: {code} {data}"
print(f"[OK] {TEST_USER} 登录成功")

# ========================
# 5. 普通用户调用后端 API（验证按钮对应功能）
# ========================
code, data, _ = call(user_session, "GET", "/api/v1/files", params={})
assert code == 200, f"文件列表失败: {code} {data}"
print(f"[OK] 普通用户 加载文件 API 正常")

code, data, _ = call(user_session, "GET", "/api/v1/tags/rules")
assert code == 200, f"标签规则失败: {code} {data}"
print(f"[OK] 普通用户 刷新规则 API 正常")

code, data, _ = call(user_session, "POST", "/api/v1/db/test-connection", json={
    "db_type": "mysql", "host": "127.0.0.1", "port": 3306,
    "username": "root", "password": "", "database": "test"
})
assert code == 200, f"测试连接接口异常: {code} {data}"
print(f"[OK] 普通用户 测试连接 API 可达")

# ========================
# 6. 上传文件
# ========================
assert os.path.isfile(TEST_FILE), f"测试文件不存在: {TEST_FILE}"
with open(TEST_FILE, "rb") as f:
    code, data, _ = call(user_session, "POST", "/api/v1/files/upload",
                         files={"file": ("test_upload.csv", f, "text/csv")},
                         data={"auto_convert": "true", "output_format": "CSV"})
assert code == 200 and isinstance(data, dict) and data.get("code") == 0, \
    f"上传失败: {code} {data}"
print(f"[OK] 文件上传并自动转换成功: {data.get('data', {}).get('fileName')}")

# ========================
# 7. 获取 fileId 并自动标签
# ========================
time.sleep(1)
code, data, _ = call(user_session, "GET", "/api/v1/files", params={})
assert code == 200 and isinstance(data, dict) and data.get("code") == 0, \
    f"获取文件列表失败: {code} {data}"
files = data.get("data", {}).get("rows", [])
assert files, "文件列表为空，上传未生效"
file_id = files[0].get("fileId")
print(f"[INFO] 用于自动标签的 fileId: {file_id}")

code, data, _ = call(user_session, "POST", "/api/v1/tags/auto",
                     json={"fileId": file_id, "outputFormat": "CSV", "operator": TEST_USER})
assert code == 200 and isinstance(data, dict) and data.get("code") == 0, \
    f"自动标签失败: {code} {data}"
print(f"[OK] 自动标签成功: {data.get('data', {})}")

# ========================
# 8. 验证 tagged_output 下出现新文件
# ========================
time.sleep(1)
code, data, _ = call(user_session, "GET", "/api/v1/files", params={})
files_after = data.get("data", {}).get("rows", [])
assert len(files_after) > len(files), \
    f"自动标签后没有生成新文件: before={len(files)} after={len(files_after)}"
print(f"[OK] 自动标签后文件数增加: {len(files)} -> {len(files_after)}")

# ========================
# 9. 清理测试用户
# ========================
code, data, _ = call(admin_session, "DELETE",
                     f"/api/v1/internal/users/{TEST_USER}?purge_data=true")
if code == 200:
    print(f"[OK] 已清理测试用户 {TEST_USER}")
else:
    print(f"[WARN] 清理用户失败: {code} {data}")

print("\n=== 端到端测试全部通过 ===")
