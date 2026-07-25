#!/usr/bin/env python3
"""便捷展示文件 xattr 元数据（AES-256-GCM 加密）。

用法：
    python show_xattr_meta.py /path/to/file.csv

脚本会自动从以下位置读取 META_XATTR_KEY：
1. 环境变量 META_XATTR_KEY
2. /home/yhz/iot/v1-backend/deploy/iot-backend.service.override.conf
"""

import json
import os
import re
import sys
from pathlib import Path

# 项目路径
PROJECT_ROOT = Path(__file__).resolve().parent
BACKEND_DIR = PROJECT_ROOT / "v1-backend"
OVERRIDE_CONF = BACKEND_DIR / "deploy" / "iot-backend.service.override.conf"


def _load_key_from_conf() -> str:
    if not OVERRIDE_CONF.exists():
        return ""
    text = OVERRIDE_CONF.read_text(encoding="utf-8")
    m = re.search(r'Environment=META_XATTR_KEY=([A-Za-z0-9+/=]+)', text)
    return m.group(1) if m else ""


def main() -> int:
    if len(sys.argv) < 2:
        print(f"用法: {sys.argv[0]} <文件路径>", file=sys.stderr)
        return 1

    file_path = sys.argv[1]
    p = Path(file_path)
    if not p.exists():
        print(f"文件不存在: {file_path}", file=sys.stderr)
        return 1

    # 读取密钥
    key = os.environ.get("META_XATTR_KEY") or _load_key_from_conf()
    if not key:
        print("错误：未找到 META_XATTR_KEY，请设置环境变量或在 override.conf 中配置。", file=sys.stderr)
        return 1
    os.environ["META_XATTR_KEY"] = key

    # 导入项目模块
    sys.path.insert(0, str(BACKEND_DIR))
    try:
        from app import meta_xattr
    except Exception as exc:
        print(f"导入 meta_xattr 失败: {exc}", file=sys.stderr)
        return 1

    print(f"文件: {p.resolve()}")
    print("-" * 60)

    # 原始 xattr 信息
    try:
        attrs = os.listxattr(file_path)
        print(f"扩展属性列表: {attrs}")
        if "user.meta" in attrs:
            raw = os.getxattr(file_path, "user.meta")
            print(f"user.meta 原始长度: {len(raw)} 字节")
        if "user.checksum" in attrs:
            chk = os.getxattr(file_path, "user.checksum").decode("utf-8")
            print(f"user.checksum: {chk}")
    except OSError as exc:
        print(f"读取 xattr 失败: {exc}", file=sys.stderr)
        return 1

    print("-" * 60)

    # 解密后的元数据
    try:
        meta = meta_xattr.read_meta(file_path)
        print("解密后的配置信息（元数据）：")
        print(json.dumps(meta, ensure_ascii=False, indent=2))
    except Exception as exc:
        print(f"解密 xattr 失败: {exc}", file=sys.stderr)
        return 1

    # 同时展示 .meta.json 备份（如果存在）
    backup_path = p.with_suffix("")
    backup_path = Path(f"{backup_path}.meta.json")
    print("-" * 60)
    if backup_path.exists():
        print(f"明文备份文件: {backup_path}")
        try:
            backup_meta = json.loads(backup_path.read_text(encoding="utf-8"))
            print("备份内容与 xattr 一致:", backup_meta == meta)
        except Exception as exc:
            print(f"读取备份失败: {exc}")
    else:
        print(f"未找到明文备份: {backup_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
