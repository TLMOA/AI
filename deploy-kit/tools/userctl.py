#!/usr/bin/env python3
"""IoT 平台用户（工厂/管理员）管理 —— 部署工具包共用组件。

设计目标：
  1) 与线上注册流程产出完全一致（MySQL iot_users + 用户目录 + SQLite 同步）
  2) 优先复用后端自身函数，避免目录结构与代码漂移
  3) 不依赖后端 HTTP 服务是否启动，可直接离线开户

用法:
  userctl.py create <username> <password> [--admin] [--mode public|private] [--ceph PATH]
  userctl.py create <username> <password> --admin --keep-password   # 已存在则不改密码（重跑部署用）
  userctl.py list

环境变量:
  CODE_DIR  代码根目录（默认 /home/yhz/iot）
  DB_HOST / DB_PORT / DB_USER / DB_PASS / DB_NAME
  IN_DATA_BASE_DIR  公有化用户数据根（默认 /home/yhz）
"""
import argparse
import json
import os
import sys
from pathlib import Path

CODE_DIR = os.environ.get("CODE_DIR", "/home/yhz/iot")
BACKEND_DIR = os.path.join(CODE_DIR, "v1-backend")
if os.path.isdir(BACKEND_DIR):
    sys.path.insert(0, BACKEND_DIR)

import bcrypt  # noqa: E402
import sqlalchemy  # noqa: E402
from sqlalchemy import text  # noqa: E402

DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = os.environ.get("DB_PORT", "3306")
DB_USER = os.environ.get("DB_USER", "iot")
DB_PASS = os.environ.get("DB_PASS", "")
DB_NAME = os.environ.get("DB_NAME", "nifi")

SUBDIRS = [
    "inbox_csv", "inbox_json", "inbox_tsv",
    "csv_to_json", "csv_to_tsv",
    "json_to_csv", "json_to_tsv",
    "tsv_to_csv", "tsv_to_json",
    "output_csv", "output_json", "output_tsv",
]
DATA_ROOTS = ["nifi-data", "real_nifi_data", "tagged_nifi_data", "tagged_real_nifi_data"]


def engine():
    url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    return sqlalchemy.create_engine(url)


def create_storage_dirs(username: str, mode: str, ceph: str) -> str:
    """建用户目录。优先复用后端 _create_user_storage_dirs，保证与线上一致。"""
    try:
        from app.auth import _create_user_storage_dirs
        _create_user_storage_dirs(username, mode, ceph)
        return "backend"
    except Exception as e:  # 回退：按同样的结构自建
        print(f"[warn] 复用后端建目录失败({e})，使用内置实现", file=sys.stderr)
        if mode == "private" and ceph:
            root = Path(ceph)
        else:
            root = Path(os.environ.get("IN_DATA_BASE_DIR", "/home/yhz")) / username
        for base in DATA_ROOTS:
            for s in SUBDIRS:
                (root / base / s).mkdir(parents=True, exist_ok=True)
        (root / "meta_backups").mkdir(parents=True, exist_ok=True)
        return "fallback"


def sync_sqlite(username, password_hash, is_admin, mode, ceph, keep_password=False) -> bool:
    """同步到 SQLite，保证内部管理页能列出该用户。"""
    try:
        from app.db_models import IotUser
        from app.auth import SessionLocal
        sess = SessionLocal()
        try:
            u = sess.query(IotUser).filter(IotUser.username == username).first()
            if u:
                # keep_password：已存在用户不改密码（与 MySQL 侧保持一致，
                # 否则两边的哈希会指向不同口令，登录行为就会随登录路径而变）
                if not keep_password:
                    u.password_hash = password_hash
                u.is_admin = is_admin
                u.deployment_mode = mode
                u.ceph_endpoint = ceph
            else:
                sess.add(IotUser(username=username, password_hash=password_hash,
                                 is_admin=is_admin, deployment_mode=mode,
                                 ceph_endpoint=ceph))
            sess.commit()
            return True
        finally:
            sess.close()
    except Exception as e:
        print(f"[warn] SQLite 同步失败（不影响登录，仅影响内部管理页展示）: {e}", file=sys.stderr)
        return False


def cmd_create(args):
    mode = (args.mode or "public").lower()
    if mode not in ("public", "private"):
        print("[err] --mode 只能是 public 或 private", file=sys.stderr)
        sys.exit(2)
    ceph = (args.ceph or "").strip()
    if mode == "private" and not ceph:
        print("[err] 私有化必须提供 --ceph（数据落盘路径）", file=sys.stderr)
        sys.exit(2)

    ph = bcrypt.hashpw(args.password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    is_admin = 1 if args.admin else 0

    eng = engine()
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS iot_users ("
            "id INT AUTO_INCREMENT PRIMARY KEY,"
            "username VARCHAR(128) NOT NULL UNIQUE,"
            "password_hash VARCHAR(256) NOT NULL,"
            "is_admin TINYINT DEFAULT 0,"
            "deployment_mode VARCHAR(32) DEFAULT 'public',"
            "ceph_endpoint VARCHAR(512) DEFAULT '',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        ))
        exists = conn.execute(
            text("SELECT 1 FROM iot_users WHERE username = :u"), {"u": args.username}
        ).fetchone()
        if exists:
            if args.keep_password:
                # 【重跑语义】账号已经建过了 → 密码是"做过的事"，不再动它。
                # 否则每次重跑都会把密码重置成 config.env 里的值，
                # 而现场人员记的是第一次生成的那个密码。
                conn.execute(text(
                    "UPDATE iot_users SET is_admin=:a, deployment_mode=:m, "
                    "ceph_endpoint=:c WHERE username=:u"
                ), {"a": is_admin, "m": mode, "c": ceph, "u": args.username})
                action, pw_action = "updated", "kept"
            else:
                conn.execute(text(
                    "UPDATE iot_users SET password_hash=:p, is_admin=:a, "
                    "deployment_mode=:m, ceph_endpoint=:c WHERE username=:u"
                ), {"p": ph, "a": is_admin, "m": mode, "c": ceph, "u": args.username})
                action, pw_action = "updated", "reset"
        else:
            conn.execute(text(
                "INSERT INTO iot_users (username, password_hash, is_admin, "
                "deployment_mode, ceph_endpoint) VALUES (:u,:p,:a,:m,:c)"
            ), {"u": args.username, "p": ph, "a": is_admin, "m": mode, "c": ceph})
            action, pw_action = "created", "set"

    dir_impl = create_storage_dirs(args.username, mode, ceph)
    sqlite_ok = sync_sqlite(args.username, ph, is_admin, mode, ceph,
                            keep_password=args.keep_password)

    print(json.dumps({
        "username": args.username,
        "action": action,
        "password": pw_action,
        "is_admin": bool(is_admin),
        "deployment_mode": mode,
        "ceph_endpoint": ceph,
        "dir_impl": dir_impl,
        "sqlite_synced": sqlite_ok,
    }, ensure_ascii=False))


def cmd_list(_args):
    eng = engine()
    rows = []
    with eng.connect() as conn:
        for r in conn.execute(text(
            "SELECT username, is_admin, deployment_mode, ceph_endpoint FROM iot_users ORDER BY id"
        )):
            rows.append({
                "username": r[0],
                "is_admin": bool(r[1]),
                "deployment_mode": r[2],
                "ceph_endpoint": r[3],
            })
    print(json.dumps({"users": rows, "total": len(rows)}, ensure_ascii=False, indent=2))


def main():
    p = argparse.ArgumentParser(description="IoT 平台用户管理")
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("create", help="创建/更新用户")
    c.add_argument("username")
    c.add_argument("password")
    c.add_argument("--admin", action="store_true", help="设为管理员（可进内部管理页）")
    c.add_argument("--mode", default="public", help="public | private")
    c.add_argument("--ceph", default="", help="私有化数据落盘路径（本地目录/挂载点）")
    c.add_argument("--keep-password", action="store_true", dest="keep_password",
                   help="用户已存在时【不修改密码】（重跑部署时用；只更新权限/模式）")
    c.set_defaults(func=cmd_create)

    l = sub.add_parser("list", help="列出所有用户")
    l.set_defaults(func=cmd_list)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
