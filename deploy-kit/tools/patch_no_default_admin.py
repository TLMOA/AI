#!/usr/bin/env python3
"""补丁：禁止后端启动时自动创建默认 admin 账号。

为什么需要
----------
main.py 启动时会调用 db_models.init_db()，其中有一段逻辑：
  若 SQLite 里不存在 username='admin' 的用户，就自动创建一个，
  密码取自 IOT_ADMIN_PASSWORD，**默认值为 '123456'**。

这会导致两个问题：
  1) 每套实例本应「只有一个账号」，却凭空多出一个 admin
  2) admin/123456 是弱口令，属于安全隐患

补丁行为：保留原有的表结构迁移逻辑，只去掉「自动插入 admin」这一步。
账号由部署脚本（deploy.sh → userctl.py）显式创建，即本站唯一账号。

幂等，支持 --revert。

用法: patch_no_default_admin.py <code_dir> [--revert]
"""
import shutil
import sys
from pathlib import Path

MARK = "[deploy-kit] no-default-admin"

OLD = '''                conn.execute(text("INSERT INTO iot_users (username, password_hash, is_admin, deployment_mode) VALUES ('admin', :ph, 1, 'public')"), {"ph": ph})'''

NEW = '''                # %s 不自动创建默认 admin（账号由部署脚本创建，每套实例一个账号）
                pass''' % MARK


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    revert = "--revert" in sys.argv
    if not args:
        print("usage: patch_no_default_admin.py <code_dir> [--revert]", file=sys.stderr)
        sys.exit(2)

    target = Path(args[0]) / "v1-backend" / "app" / "db_models.py"
    if not target.exists():
        print(f"[err] 找不到 {target}", file=sys.stderr)
        sys.exit(1)

    if revert:
        bak = target.with_suffix(target.suffix + ".bak")
        if bak.exists():
            shutil.copy2(bak, target)
            print(f"[ok] 已还原 {target}")
        else:
            print("[warn] 未找到 .bak，无法还原")
        return

    src = target.read_text(encoding="utf-8")

    if MARK in src:
        print("[skip] 默认 admin 补丁已存在，跳过")
        return

    if OLD not in src:
        print("[err] 未匹配到目标片段（代码版本可能已变化）", file=sys.stderr)
        sys.exit(1)

    bak = target.with_suffix(target.suffix + ".bak")
    if not bak.exists():
        shutil.copy2(target, bak)
        print(f"[bak] 已备份 -> {bak}")

    target.write_text(src.replace(OLD, NEW, 1), encoding="utf-8")
    print(f"[ok] 已打补丁：禁止自动创建默认 admin -> {target}")


if __name__ == "__main__":
    main()
