#!/usr/bin/env python3
"""模板渲染：把 __KEY__ 替换为环境变量 KEY 的值。

比 sed 安全 —— 密码里含 / & | 等特殊字符也不会破坏模板。
用法: render.py <template> <output>
"""
import os
import sys


def main():
    if len(sys.argv) != 3:
        print("usage: render.py <template> <output>", file=sys.stderr)
        sys.exit(2)
    src, dst = sys.argv[1], sys.argv[2]
    with open(src, "r", encoding="utf-8") as f:
        content = f.read()

    # 按顺序替换，长键优先，避免 __DB_PASS__ 被 __DB_PASS_X__ 之类误伤
    for key in sorted(os.environ.keys(), key=len, reverse=True):
        content = content.replace("__%s__" % key, os.environ.get(key, ""))

    os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)
    with open(dst, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"[render] {src} -> {dst}")


if __name__ == "__main__":
    main()
