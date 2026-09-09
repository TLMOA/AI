#!/usr/bin/env python3
"""P2 端到端测试：训练中心 + Local/NiFi 双模式切换验证"""
import os
import sys
import time
from datetime import datetime
import requests

BASE = "http://127.0.0.1:5174"
ADMIN_USER = "admin"
ADMIN_PASS = "admin"

ts = datetime.now().strftime("%m%d%H%M%S")
TEST_USER = f"testp2_{ts}"
TEST_PASS = "Test@123"
TEST_FILE = "/home/yhz/iot/test_upload.csv"


def call(session, method, path, **kwargs):
    r = session.request(method, BASE + path, **kwargs)
    try:
        data = r.json()
    except Exception:
        data = r.text
    return r.status_code, data, r


def ensure_test_user_clean(admin_session):
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


def login(username, password):
    sess = requests.Session()
    code, data, _ = call(sess, "POST", "/api/v1/auth/login",
                         json={"username": username, "password": password})
    assert code == 200 and isinstance(data, dict) and data.get("success"), \
        f"{username} 登录失败: {code} {data}"
    return sess


def set_backend_mode(sess, mode):
    code, data, _ = call(sess, "POST", "/api/v1/internal/backend-mode",
                         json={"username": TEST_USER, "mode": mode, "operator": TEST_USER})
    assert code == 200 and isinstance(data, dict) and data.get("code") == 0, \
        f"切换后端模式到 {mode} 失败: {code} {data}"
    print(f"[OK] 后端模式切换为 {mode}")


def upload_file(sess, desc=""):
    assert os.path.isfile(TEST_FILE), f"测试文件不存在: {TEST_FILE}"
    with open(TEST_FILE, "rb") as f:
        code, data, _ = call(sess, "POST", "/api/v1/files/upload",
                             files={"file": ("test_upload.csv", f, "text/csv")},
                             data={"auto_convert": "true", "output_format": "CSV"})
    assert code == 200 and isinstance(data, dict) and data.get("code") == 0, \
        f"{desc} 上传失败: {code} {data}"
    print(f"[OK] {desc} 上传成功: {data.get('data', {}).get('fileName')}")
    return data


def list_files(sess, desc=""):
    code, data, _ = call(sess, "GET", "/api/v1/files", params={})
    assert code == 200 and isinstance(data, dict) and data.get("code") == 0, \
        f"{desc} 加载文件列表失败: {code} {data}"
    rows = data.get("data", {}).get("rows", [])
    print(f"[OK] {desc} 文件列表: {len(rows)} 个文件")
    return rows


def auto_tag(sess, file_id, desc=""):
    code, data, _ = call(sess, "POST", "/api/v1/tags/auto",
                         json={"fileId": file_id, "outputFormat": "CSV", "operator": TEST_USER})
    assert code == 200 and isinstance(data, dict) and data.get("code") == 0, \
        f"{desc} 自动标签失败: {code} {data}"
    print(f"[OK] {desc} 自动标签成功")
    return data


def load_training_files(sess, desc=""):
    code, data, _ = call(sess, "GET", "/api/v1/training/files", params={"page": 1, "size": 100})
    assert code == 200 and isinstance(data, dict) and data.get("code") == 0, \
        f"{desc} 加载训练文件失败: {code} {data}"
    files = data.get("data", {}).get("files", [])
    print(f"[OK] {desc} 训练文件: {len(files)} 个")
    return files


def submit_training(sess, file_ids, desc=""):
    code, data, _ = call(sess, "POST", "/api/v1/training/submit",
                         json={"selectedFileIds": file_ids, "trainingConfig": {"modelName": f"p2_model_{desc}"}})
    assert code == 200 and isinstance(data, dict) and data.get("code") == 0, \
        f"{desc} 提交训练失败: {code} {data}"
    task_id = data.get("data", {}).get("taskId")
    accepted = data.get("data", {}).get("acceptedFiles", [])
    print(f"[OK] {desc} 训练任务提交: {task_id}, 接受 {len(accepted)} 个文件")
    return task_id


def wait_training_complete(sess, task_id, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        code, data, _ = call(sess, "GET", f"/api/v1/training/tasks/{task_id}")
        assert code == 200 and isinstance(data, dict) and data.get("code") == 0, \
            f"查询训练任务失败: {code} {data}"
        task = data.get("data", {})
        status = task.get("status")
        progress = task.get("progress", 0)
        print(f"[INFO] 训练任务 {task_id}: status={status}, progress={progress}%")
        if status in ("completed", "failed", "error"):
            assert status == "completed", f"训练任务失败: {task}"
            return task
        time.sleep(1)
    raise TimeoutError(f"训练任务 {task_id} 未在 {timeout}s 内完成")


def assert_file_in_user_dir(filename, subdir_contains, desc=""):
    user_root = f"/home/yhz/{TEST_USER}"
    found = False
    for root, dirs, files in os.walk(user_root):
        if subdir_contains in root and filename in files:
            found = True
            print(f"[OK] {desc} 文件落在 {root}/{filename}")
            break
    assert found, f"{desc} 未在 {user_root} 下找到 {filename}（预期路径包含 {subdir_contains}）"


def main():
    # 1. admin 登录并清理旧用户
    admin_session = login(ADMIN_USER, ADMIN_PASS)
    print(f"[OK] admin 登录成功")
    ensure_test_user_clean(admin_session)

    # 2. 注册新用户
    code, data, _ = call(requests, "POST", "/api/v1/auth/register", json={
        "username": TEST_USER,
        "password": TEST_PASS,
        "deployment_mode": "public",
    })
    assert code == 200 and isinstance(data, dict) and data.get("success"), \
        f"注册失败: {code} {data}"
    print(f"[OK] 注册用户 {TEST_USER} 成功")

    # 3. 检查目录自动创建
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

    # 4. 新用户登录
    user_session = login(TEST_USER, TEST_PASS)

    # ========================
    # Local 模式流程
    # ========================
    set_backend_mode(user_session, "local")

    upload_file(user_session, "Local 模式")
    files_local = list_files(user_session, "Local 模式")
    assert files_local, "Local 模式文件列表为空"
    local_file_id = files_local[0].get("fileId")

    auto_tag(user_session, local_file_id, "Local 模式")
    time.sleep(1)
    files_local_after = list_files(user_session, "Local 模式 自动标签后")
    assert len(files_local_after) > len(files_local), \
        f"Local 模式自动标签后没有生成新文件: before={len(files_local)} after={len(files_local_after)}"

    # 验证 Local 模式文件落在 nifi-data
    assert_file_in_user_dir(files_local_after[-1].get("fileName"), "/nifi-data/", "Local 模式")

    # 训练中心 Local 模式
    train_files_local = load_training_files(user_session, "Local 模式")
    assert train_files_local, "Local 模式训练文件列表为空"
    train_ids_local = [f.get("fileId") for f in train_files_local[:2]]
    task_local = submit_training(user_session, train_ids_local, "local")
    wait_training_complete(user_session, task_local)
    print("[OK] Local 模式训练任务完成")

    # ========================
    # NiFi 模式流程
    # ========================
    set_backend_mode(user_session, "nifi")

    upload_file(user_session, "NiFi 模式")
    files_nifi = list_files(user_session, "NiFi 模式")
    assert files_nifi, "NiFi 模式文件列表为空"
    nifi_file_id = files_nifi[0].get("fileId")

    auto_tag(user_session, nifi_file_id, "NiFi 模式")
    time.sleep(1)
    files_nifi_after = list_files(user_session, "NiFi 模式 自动标签后")
    assert len(files_nifi_after) > len(files_nifi), \
        f"NiFi 模式自动标签后没有生成新文件: before={len(files_nifi)} after={len(files_nifi_after)}"

    # 验证 NiFi 模式文件落在 real_nifi_data
    assert_file_in_user_dir(files_nifi_after[-1].get("fileName"), "/real_nifi_data/", "NiFi 模式")

    # 训练中心 NiFi 模式：应能扫描到 real_nifi_data 下的文件
    train_files_nifi = load_training_files(user_session, "NiFi 模式")
    assert train_files_nifi, "NiFi 模式训练文件列表为空"
    train_ids_nifi = [f.get("fileId") for f in train_files_nifi[:2]]
    task_nifi = submit_training(user_session, train_ids_nifi, "nifi")
    wait_training_complete(user_session, task_nifi)
    print("[OK] NiFi 模式训练任务完成")

    # 切换回 Local 模式
    set_backend_mode(user_session, "local")

    # 清理
    code, data, _ = call(admin_session, "DELETE",
                         f"/api/v1/internal/users/{TEST_USER}?purge_data=true")
    if code == 200:
        print(f"[OK] 已清理测试用户 {TEST_USER}")
    else:
        print(f"[WARN] 清理用户失败: {code} {data}")

    print("\n=== P2 端到端测试全部通过 ===")


if __name__ == "__main__":
    main()
