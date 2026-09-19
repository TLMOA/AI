#!/usr/bin/env python3
"""给 auth.py 打补丁：实现 IOT_ALLOW_SELF_REGISTER 自助注册开关。

为什么需要
----------
方案一要求「关闭自助注册，只能管理员开户」。deploy.sh 会在 override.conf 里写入
Environment=IOT_ALLOW_SELF_REGISTER=false，但**后端 auth.py 从不读取这个变量**，
所以光配环境不起作用 —— 注册接口仍然对任何人开放。

补丁在 register() 开头插入环境变量判断：
  IOT_ALLOW_SELF_REGISTER=false  -> 注册接口返回 403
  未设置 / true / 1 / yes        -> 保持原行为

另外：管理员开户不受影响（开户走 userctl.py 直连数据库，不经注册接口）。

幂等（已打过会跳过），支持 --revert（依赖 .bak）。

用法: patch_disable_self_register.py <code_dir> [--revert]
"""
import shutil
import sys
from pathlib import Path

MARK = "[deploy-kit] self-register"

OLD = '''def register(req: LoginReq):
    # register regular user
    if req.username.lower() == "admin":'''

NEW = '''def register(req: LoginReq):
    # %s 自助注册开关：关闭后只能由管理员（userctl.py / 内部管理页）开户
    if os.getenv("IOT_ALLOW_SELF_REGISTER", "true").strip().lower() not in ("true", "1", "yes"):
        raise HTTPException(status_code=403, detail="自助注册已关闭，请联系管理员开户")
    # register regular user
    if req.username.lower() == "admin":''' % MARK


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    revert = "--revert" in sys.argv
    if not args:
        print("usage: patch_disable_self_register.py <code_dir> [--revert]", file=sys.stderr)
        sys.exit(2)

    auth_py = Path(args[0]) / "v1-backend" / "app" / "auth.py"
    if not auth_py.exists():
        print(f"[err] 找不到 {auth_py}", file=sys.stderr)
        sys.exit(1)

    if revert:
        bak = auth_py.with_suffix(auth_py.suffix + ".bak")
        if bak.exists():
            shutil.copy2(bak, auth_py)
            print(f"[ok] 已还原 {auth_py}")
        else:
            print("[warn] 未找到 .bak，无法还原")
        return

    src = auth_py.read_text(encoding="utf-8")

    if MARK in src:
        print("[skip] 自助注册开关补丁已存在，跳过")
        return

    if OLD not in src:
        print("[err] 未匹配到 register() 目标片段，请检查代码版本", file=sys.stderr)
        sys.exit(1)
    if "\nimport os\n" not in src and not src.startswith("import os\n"):
        print("[err] auth.py 未 import os，补丁无法工作", file=sys.stderr)
        sys.exit(1)

    bak = auth_py.with_suffix(auth_py.suffix + ".bak")
    if not bak.exists():
        shutil.copy2(auth_py, bak)
        print(f"[bak] 已备份 -> {bak}")

    auth_py.write_text(src.replace(OLD, NEW, 1), encoding="utf-8")
    print(f"[ok] 已打自助注册开关补丁: {auth_py}")


if __name__ == "__main__":
    main()
