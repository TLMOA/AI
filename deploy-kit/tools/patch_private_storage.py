#!/usr/bin/env python3
"""给 main.py 打补丁：让「私有化」用户的数据真正落到其 ceph_endpoint 指定的本地盘路径。

背景
----
线上 main.py 的 _resolve_user_storage_root() 把路径写死为 IN_DATA_BASE_DIR/username，
并标注了 TODO，导致注册时填的 ceph_endpoint 只入库、不生效（数据仍落在平台本地盘）。

补丁行为
--------
 - deployment_mode=private 且 ceph_endpoint 非空  -> 数据根 = ceph_endpoint
 - 其它情况（public / 未配置）                    -> 数据根 = IN_DATA_BASE_DIR/<username>
 - 用户名强制过 _sanitize_filename_component，顺带堵掉路径穿越

幂等：已打过补丁会跳过。支持 --revert 还原（需存在 .bak）。

用法: patch_private_storage.py <code_dir> [--revert]
"""
import re
import shutil
import sys
from pathlib import Path

MARK = "[deploy-kit]"

NEW_FUNC = '''def _resolve_user_storage_root(username: str) -> Path:
    """%s 私有化用户数据落 ceph_endpoint（本地盘/挂载点），公有化落 IN_DATA_BASE_DIR/username。"""
    root = None
    try:
        info = _get_user_info(username) or {}
        mode = (info.get("deployment_mode") or "public").lower()
        ceph = (info.get("ceph_endpoint") or "").strip()
        if mode == "private" and ceph:
            root = Path(ceph)
    except Exception:
        root = None
    if root is None:
        root = IN_DATA_BASE_DIR / _sanitize_filename_component(username)
    (root / "nifi-data").mkdir(parents=True, exist_ok=True)
    (root / "real_nifi_data").mkdir(parents=True, exist_ok=True)
    return root

''' % MARK


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    revert = "--revert" in sys.argv
    if not args:
        print("usage: patch_private_storage.py <code_dir> [--revert]", file=sys.stderr)
        sys.exit(2)

    main_py = Path(args[0]) / "v1-backend" / "app" / "main.py"
    if not main_py.exists():
        print(f"[err] 找不到 {main_py}", file=sys.stderr)
        sys.exit(1)

    if revert:
        bak = main_py.with_suffix(main_py.suffix + ".bak")
        if bak.exists():
            shutil.copy2(bak, main_py)
            print(f"[ok] 已还原 {main_py}")
        else:
            print("[warn] 未找到 .bak，无法还原")
        return

    src = main_py.read_text(encoding="utf-8")

    if MARK in src:
        print("[skip] 私有化补丁已存在，跳过")
        return

    pattern = re.compile(
        r"def _resolve_user_storage_root\(username: str\) -> Path:.*?(?=\ndef )",
        re.S,
    )
    if not pattern.search(src):
        print("[err] 未匹配到 _resolve_user_storage_root 函数，请检查代码版本", file=sys.stderr)
        sys.exit(1)

    bak = main_py.with_suffix(main_py.suffix + ".bak")
    if not bak.exists():
        shutil.copy2(main_py, bak)
        print(f"[bak] 已备份 -> {bak}")

    new_src = pattern.sub(NEW_FUNC, src, count=1)
    main_py.write_text(new_src, encoding="utf-8")
    print(f"[ok] 已打私有化补丁: {main_py}")


if __name__ == "__main__":
    main()
