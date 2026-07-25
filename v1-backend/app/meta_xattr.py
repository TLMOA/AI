"""meta_xattr.py — V3 完整方案：xattr + AES-256-GCM 加密存储文件元数据。

磁盘上不存在任何独立的配置文件。元数据通过 AES-256-GCM 认证加密后
写入数据文件的 Linux 扩展属性（xattr），一个文件在磁盘上就是它自己。

密钥通过环境变量 META_XATTR_KEY 注入，不落盘。
"""

import base64
import hashlib
import hmac
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

XATTR_META = "user.meta"
XATTR_CHECKSUM = "user.checksum"

# ---------- 密钥管理 ----------

_META_KEY: Optional[bytes] = None
_aesgcm: Optional[AESGCM] = None


def _init_key() -> None:
    global _META_KEY, _aesgcm
    key_b64 = os.environ.get("META_XATTR_KEY", "")
    if not key_b64:
        # 未配置密钥时自动生成临时密钥（仅用于开发，重启后失效）
        import secrets
        key_b64 = base64.b64encode(secrets.token_bytes(32)).decode()
        os.environ["META_XATTR_KEY"] = key_b64
    _META_KEY = base64.b64decode(key_b64)
    _aesgcm = AESGCM(_META_KEY)


def _ensure_key() -> AESGCM:
    global _aesgcm
    if _aesgcm is None:
        _init_key()
    return _aesgcm


def get_current_key_b64() -> str:
    _ensure_key()
    return base64.b64encode(_META_KEY).decode() if _META_KEY else ""


# ---------- 核心读写 ----------

def write_meta(file_path: str, meta: Dict[str, Any]) -> None:
    """AES-256-GCM 加密写入元数据到文件 xattr。

    GCM 模式自带认证标签，无需额外 HMAC。
    不创建任何额外文件，磁盘上只有数据文件本身。
    """
    aesgcm = _ensure_key()
    plaintext = json.dumps(meta, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    nonce = os.urandom(12)
    ciphertext = aesgcm.encrypt(nonce, plaintext, None)
    payload = base64.b64encode(nonce + ciphertext)
    os.setxattr(file_path, XATTR_META, payload)
    os.setxattr(
        file_path,
        XATTR_CHECKSUM,
        hashlib.sha256(plaintext).hexdigest().encode("utf-8"),
    )


def read_meta(file_path: str) -> Dict[str, Any]:
    """读取并解密文件 xattr 中的元数据。

    GCM 解密时自动验证认证标签，校验失败直接抛出异常。
    """
    aesgcm = _ensure_key()
    payload = base64.b64decode(os.getxattr(file_path, XATTR_META))
    nonce = payload[:12]
    ciphertext = payload[12:]

    try:
        plaintext = aesgcm.decrypt(nonce, ciphertext, None)
    except Exception:
        raise ValueError("元数据 GCM 认证失败：文件可能被篡改")

    expected_hash = hashlib.sha256(plaintext).hexdigest()
    actual_hash = os.getxattr(file_path, XATTR_CHECKSUM).decode("utf-8")
    if not hmac.compare_digest(expected_hash, actual_hash):
        raise ValueError("元数据 checksum 校验失败：数据损坏")

    return json.loads(plaintext)


def has_meta(file_path: str) -> bool:
    """检查文件是否已有 xattr 元数据。"""
    try:
        return XATTR_META in os.listxattr(file_path)
    except OSError:
        return False


def delete_meta(file_path: str) -> None:
    """删除文件的 xattr 元数据。"""
    for attr in (XATTR_META, XATTR_CHECKSUM):
        try:
            os.removexattr(file_path, attr)
        except OSError:
            pass


# ---------- 批量迁移 ----------

def migrate_from_json(data_dir: str) -> Dict[str, Any]:
    """将存量 .meta.json 文件迁移到 xattr 存储。

    扫描目录下所有 .meta.json 文件，读取内容 → 加密写入 xattr → 可选删除旧文件。
    """
    base = Path(data_dir)
    converted = 0
    skipped = 0
    errors = 0
    _ensure_key()

    for meta_path in base.rglob("*.meta.json"):
        if not meta_path.is_file():
            continue
        data_path = Path(str(meta_path).replace(".meta.json", ""))
        if not data_path.exists() or not data_path.is_file():
            skipped += 1
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            write_meta(str(data_path), meta)
            # 迁移成功后删除旧 .meta.json
            meta_path.unlink(missing_ok=True)
            converted += 1
        except Exception:
            errors += 1

    return {"converted": converted, "skipped": skipped, "errors": errors}


# ---------- 平台检测 ----------

def is_available() -> bool:
    """检查当前平台是否支持 xattr。"""
    return sys.platform == "linux" and hasattr(os, "setxattr")