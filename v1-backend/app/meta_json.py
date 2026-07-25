"""
meta_json.py  – 标签配置文件读写模块（V3 xattr 集成版）

Linux 生产环境：自动使用 xattr + AES-256-GCM 加密存储，同时写 .meta.json 备份。
macOS/Windows 开发环境：自动降级为纯 .meta.json 文件存储。
所有读写均基于标准文件系统，不依赖任何特殊文件系统特性。
"""

import json
import os
import sys as _sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

# ---------- xattr 集成 ----------

meta_xattr = None  # type: ignore
try:
    from . import meta_xattr as _meta_xattr
    meta_xattr = _meta_xattr
except Exception:
    pass

META_SUFFIX = ".meta.json"

# ---------- 目录常量 ----------

NIFI_BASE_DIR = Path(os.getenv("NIFI_BASE_DIR", "/home/yhz/nifi-data"))  # deprecated — v4 使用 _get_user_nifi_dir(username)
TAGGED_OUTPUT_DIR = Path(os.getenv("TAGGED_OUTPUT_DIR", "/home/yhz/nifi-data/tagged_output"))  # deprecated — v4 使用 _get_user_nifi_dir(username, "tagged_output")

# 确保目录存在（兼容旧路径）
NIFI_BASE_DIR.mkdir(parents=True, exist_ok=True)
TAGGED_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------- 时间工具 ----------

def _now_iso() -> str:
    try:
        tz = ZoneInfo("Asia/Shanghai")
    except Exception:
        from datetime import timezone, timedelta
        tz = timezone(timedelta(hours=8))
    return datetime.now(tz).isoformat(timespec="seconds")


def _now_ts() -> str:
    try:
        tz = ZoneInfo("Asia/Shanghai")
    except Exception:
        from datetime import timezone, timedelta
        tz = timezone(timedelta(hours=8))
    return datetime.now(tz).strftime("%Y%m%d_%H%M%S")


# ---------- 路径工具 ----------

def meta_path(file_path: str) -> str:
    """返回数据文件对应的 .meta.json 配置文件路径。

    规则：去掉数据文件的扩展名（如 .csv/.json/.tsv），追加 .meta.json。
    示例：/path/to/export.csv → /path/to/export.meta.json
    """
    p = Path(file_path)
    # 去掉文件扩展名（只去掉最后一个扩展名）
    stem_path = p.with_suffix("")
    return f"{stem_path}{META_SUFFIX}"


def is_meta_file(file_path: str) -> bool:
    """判断指定路径是否为 .meta.json 配置文件。"""
    return file_path.endswith(META_SUFFIX)


def data_file_from_meta(meta_path: str) -> str:
    """从 .meta.json 路径反推数据文件路径。

    优先从 meta 文件中的 storagePath 字段读取（最准确），
    如果读取失败则尝试通过去掉 .meta.json 后缀得到路径。
    """
    try:
        meta = json.loads(Path(meta_path).read_text(encoding="utf-8"))
        sp = meta.get("storagePath")
        if sp and Path(sp).exists():
            return sp
    except Exception:
        pass
    # 回退：直接去掉 .meta.json 后缀
    if meta_path.endswith(META_SUFFIX):
        return meta_path[: -len(META_SUFFIX)]
    return meta_path


# ---------- 平台与密钥检测 ----------

_using_xattr: bool = False
try:
    if meta_xattr and _sys.platform == "linux" and hasattr(os, "setxattr") and meta_xattr.is_available():
        # 仅当已配置固定密钥时才真正启用 xattr 加密，避免开发环境随机密钥导致数据不可读
        if os.environ.get("META_XATTR_KEY"):
            _using_xattr = True
except Exception:
    pass


# ---------- 版本备份配置 ----------

def _meta_backup_dir_for(file_path: str) -> Path:
    """根据数据文件路径返回对应的 meta 备份目录。

    优先使用用户目录下的 meta_backups/，无法推导时回退到全局目录。
    """
    try:
        p = Path(file_path).resolve()
        parts = p.parts
        # /home/yhz/{username}/... -> /home/yhz/{username}/meta_backups
        if len(parts) >= 4 and parts[0] == "/" and parts[1] == "home" and parts[2] == "yhz":
            user_root = Path(*parts[:4])  # /home/yhz/{username}
            return user_root / "meta_backups"
    except Exception:
        pass
    return Path(os.getenv("META_BACKUP_DIR", str(NIFI_BASE_DIR / "meta_backups")))


MAX_BACKUP_VERSIONS = int(os.getenv("META_BACKUP_MAX_VERSIONS", "5"))


def _backup_meta(file_path: str) -> None:
    """修改 meta 前自动备份旧版本（兼容 xattr 和 .meta.json）。"""
    try:
        old_meta = read_meta(file_path)
    except Exception:
        return
    fid = old_meta.get("fileId", "unknown")
    ts = _now_ts()
    backup_name = f"{fid}_{ts}.meta.json"
    backup_dir = _meta_backup_dir_for(file_path)
    backup_path = backup_dir / backup_name
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path.write_text(json.dumps(old_meta, ensure_ascii=False, indent=2), encoding="utf-8")
    # 清理旧备份，每个 fileId 最多保留 MAX_BACKUP_VERSIONS 个
    try:
        backups = sorted(
            [f for f in backup_dir.iterdir() if f.name.startswith(f"{fid}_")],
            key=lambda f: f.stat().st_mtime,
            reverse=True,
        )
        for old in backups[MAX_BACKUP_VERSIONS:]:
            old.unlink(missing_ok=True)
    except Exception:
        pass


def _find_meta_path(file_path: str) -> Optional[str]:
    """查找数据文件对应的 meta 文件路径（兼容新旧命名）。

    xattr 模式下：只要 xattr 中存在元数据即返回 meta_path（用于兼容性检查）。
    .meta.json 模式下：优先查找新命名，回退查找旧命名。
    """
    if _using_xattr:
        # xattr 模式：直接检查 xattr 是否存在
        if meta_xattr.has_meta(file_path):
            return meta_path(file_path)
        # 回退：检查是否有 .meta.json 文件（迁移前遗留或备份）
        new_path = meta_path(file_path)
        if Path(new_path).exists():
            return new_path
        old_path = f"{file_path}{META_SUFFIX}"
        if Path(old_path).exists():
            return old_path
        return None

    # 纯 .meta.json 模式
    new_path = meta_path(file_path)
    if Path(new_path).exists():
        return new_path
    old_path = f"{file_path}{META_SUFFIX}"
    if Path(old_path).exists():
        return old_path
    return None


def write_meta(file_path: str, meta: Dict[str, Any]) -> None:
    """将元数据写入存储。

    xattr 模式：AES-256-GCM 加密写入 xattr，同时写 .meta.json 备份。
    .meta.json 模式：写入 .meta.json 配置文件。
    """
    # 写入前备份旧版本（兼容旧命名）
    existing = _find_meta_path(file_path)
    if existing:
        _backup_meta(file_path)
        if existing != meta_path(file_path):
            try:
                Path(existing).unlink(missing_ok=True)
            except Exception:
                pass

    if _using_xattr:
        # 主存储：xattr 加密写入
        meta_xattr.write_meta(file_path, meta)

    # 始终写入 .meta.json 作为可读备份与灾难恢复
    path = Path(meta_path(file_path))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def read_meta(file_path: str) -> Dict[str, Any]:
    """读取元数据。

    xattr 模式：优先从 xattr 解密读取，失败则回退到 .meta.json。
    .meta.json 模式：从 .meta.json 文件读取。
    """
    if _using_xattr:
        try:
            return meta_xattr.read_meta(file_path)
        except Exception:
            # xattr 读取失败，尝试回退到 .meta.json
            found = _find_meta_path(file_path)
            if found and Path(found).exists():
                return json.loads(Path(found).read_text(encoding="utf-8"))
            raise FileNotFoundError(f"配置文件不存在: {meta_path(file_path)}")

    found = _find_meta_path(file_path)
    if not found:
        raise FileNotFoundError(f"配置文件不存在: {meta_path(file_path)}")
    return json.loads(Path(found).read_text(encoding="utf-8"))


def is_xattr_enabled() -> bool:
    """返回当前是否启用了 xattr（Linux + 已配置固定密钥）。"""
    return _using_xattr


def has_meta(file_path: str) -> bool:
    """检查文件是否存在元数据。

    xattr 模式：优先检查 xattr，回退到 .meta.json。
    .meta.json 模式：检查 .meta.json 文件。
    """
    if _using_xattr:
        if meta_xattr.has_meta(file_path):
            return True
        # 回退：检查 .meta.json 文件
        return _find_meta_path(file_path) is not None

    return _find_meta_path(file_path) is not None


def delete_meta(file_path: str) -> None:
    """删除元数据（同时删除 xattr 和 .meta.json）。"""
    if _using_xattr:
        try:
            meta_xattr.delete_meta(file_path)
        except Exception:
            pass
    # 同时删除 .meta.json 文件（含旧命名）
    for candidate in [meta_path(file_path), f"{file_path}{META_SUFFIX}"]:
        try:
            Path(candidate).unlink(missing_ok=True)
        except Exception:
            pass


# ---------- 工具函数 ----------

def _coerce_list(value: Any) -> List[str]:
    """将任意输入统一转为字符串列表（去空、保留顺序）。"""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, (tuple, set)):
        return [str(x).strip() for x in value if str(x).strip()]
    s = str(value)
    # 尝试解析 JSON 字符串
    s_stripped = s.strip()
    if s_stripped.startswith("[") and s_stripped.endswith("]"):
        try:
            parsed = json.loads(s_stripped)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            pass
    # 兼容逗号、分号、换行、竖线分隔
    import re
    parts = re.split(r"[,;\n|]", s)
    return [p.strip() for p in parts if p and p.strip()]


def _normalize_tag_payload(
    tag_range: Any,
    failed_tag: Any,
) -> Tuple[List[str], List[str]]:
    """归一化 tagRange / failedTag，确保两者最终都写成 JSON 列表。"""
    tr = _coerce_list(tag_range)
    ft = _coerce_list(failed_tag)
    return tr, ft


# ---------- 合并写入（保留已有字段） ----------

def merge_and_write_meta(file_path: str, extra: Dict[str, Any]) -> Dict[str, Any]:
    """读取已有 meta 并与新字段合并后写入，返回合并后的完整 meta。"""
    base = {}
    if has_meta(file_path):
        try:
            base = read_meta(file_path)
        except Exception:
            pass
    base.update({k: v for k, v in extra.items() if v is not None})
    base["updatedAt"] = _now_iso()
    write_meta(file_path, base)
    return base


# ---------- 完整 meta 构建 ----------

def build_meta(
    file_path: str,
    source_type: str,
    has_tag: bool,
    tag_column: Optional[str] = None,
    tag_name: Optional[str] = None,
    tag_range: Optional[List[str]] = None,
    failed_tag: Optional[str] = None,
    tag_strategy: Optional[str] = None,
    tag_rule: Optional[Dict[str, Any]] = None,
    category_id: Optional[str] = None,
    category_name: Optional[str] = None,
    description: Optional[str] = None,
    columns: Optional[List[str]] = None,
    row_count: Optional[int] = None,
    file_size: Optional[int] = None,
) -> Dict[str, Any]:
    """构建一个完整的 meta 字典，遵循 schema 定义。

    参数:
        file_path: 数据文件绝对路径
        source_type: 来源类型 (db_export / upload / manual_tag / auto_tag)
        has_tag: 是否含标签
        tag_column: 标签列名（hasTag=true 时必填）
        tag_name: 标签名（hasTag=true 时必填）
        tag_range: 候选标签范围（hasTag=false 时必填）
        failed_tag: 故障标签（hasTag=false 时必填）
        tag_strategy: 打标策略 (column_tag / manual_range / auto_rule)
        tag_rule: 自动打标规则配置
        category_id: 分类 ID
        category_name: 分类名称
        description: 描述
        columns: 列名列表
        row_count: 行数
        file_size: 文件大小（字节）
    """
    path = Path(file_path)
    now = _now_iso()

    # 根据 has_tag 和落盘目录判断 storageType
    storage_type = "tagged" if has_tag or _is_tagged_dir(str(path)) else "plain"

    # 生成 fileId
    file_id = f"file_{uuid.uuid4().hex[:10]}"

    meta: Dict[str, Any] = {
        "fileId": file_id,
        "fileName": path.name,
        "sourceType": source_type,
        "storageType": storage_type,
        "storagePath": str(path),
        "hasTag": has_tag,
        "createdAt": now,
        "updatedAt": now,
    }

    # 条件字段 - 无论 has_tag 为何值，都写入所有字段
    # tagRange 和 failedTag 在 JSON 配置文件中统一以列表形式存储
    norm_tag_range, norm_failed_tag = _normalize_tag_payload(tag_range, failed_tag)
    if has_tag:
        meta["tagColumn"] = tag_column or "tag"
        meta["tagName"] = tag_name or ""
        meta["tagRange"] = norm_tag_range
        meta["failedTag"] = norm_failed_tag
    else:
        meta["tagRange"] = norm_tag_range
        meta["failedTag"] = norm_failed_tag
        meta["tagColumn"] = tag_column or ""
        meta["tagName"] = tag_name or ""

    # 可选字段
    if tag_strategy:
        meta["tagStrategy"] = tag_strategy
    if tag_rule:
        meta["tagRule"] = tag_rule
    if category_id:
        meta["categoryId"] = category_id
    if category_name:
        meta["categoryName"] = category_name
    if description:
        meta["description"] = description
    if columns:
        meta["columns"] = columns
    if row_count is not None:
        meta["rowCount"] = row_count
    if file_size is not None:
        meta["fileSize"] = file_size

    # 默认字段
    meta.setdefault("trainable", True)

    return meta


# ---------- 校验 ----------

def validate_meta(meta: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """校验 meta 字典是否符合 schema 规则。

    返回 (is_valid, error_message)。
    """
    has_tag = meta.get("hasTag", False)

    if has_tag:
        if not meta.get("tagColumn"):
            return False, "hasTag=true 时 tagColumn 不能为空"
        if not meta.get("tagName"):
            return False, "hasTag=true 时 tagName 不能为空"
    else:
        tag_range = meta.get("tagRange")
        if not tag_range or not isinstance(tag_range, list) or len(tag_range) == 0:
            return False, "hasTag=false 时 tagRange 不能为空"
        failed_tag = meta.get("failedTag")
        if not failed_tag:
            return False, "hasTag=false 时 failedTag 不能为空"
        # 兼容 failedTag 为列表或字符串
        if isinstance(failed_tag, list):
            failed_tags = [str(t).strip() for t in failed_tag if str(t).strip()]
        else:
            failed_tags = [t.strip() for t in str(failed_tag).split(",") if t.strip()]
        for ft in failed_tags:
            if ft not in tag_range:
                return False, f"故障标签 '{ft}' 不在允许的标签范围 {tag_range} 中"

    return True, None


# ---------- 目录判断 ----------

def _is_tagged_dir(file_path: str) -> bool:
    """判断文件是否在标签目录中。"""
    tagged_str = str(TAGGED_OUTPUT_DIR.resolve())
    return str(Path(file_path).resolve()).startswith(tagged_str)


def is_tagged_file(file_path: str) -> bool:
    """判断文件是否属于标签文件（路径在标签目录中）。"""
    return _is_tagged_dir(file_path)


# 别名，保持兼容
is_tagged_path = is_tagged_file


def target_dir_for_has_tag(has_tag: bool, base_dir: Optional[Path] = None) -> Path:
    """根据 hasTag 决定目标目录。

    - hasTag=true  → TAGGED_OUTPUT_DIR（标签目录）
    - hasTag=false → NIFI_BASE_DIR / output_csv（普通目录）
    v4: 可通过 base_dir 指定用户存储根目录的 nifi-data 路径。
    """
    if has_tag:
        return (base_dir / "tagged_output") if base_dir else TAGGED_OUTPUT_DIR
    return (base_dir / "output_csv") if base_dir else NIFI_BASE_DIR / "output_csv"


# ---------- 摘要提取 ----------

def meta_summary(meta: Dict[str, Any]) -> Dict[str, Any]:
    """从完整 meta 中提取 API 返回用的摘要（不含 storagePath）。"""
    return {
        "fileId": meta.get("fileId"),
        "fileName": meta.get("fileName"),
        "sourceType": meta.get("sourceType"),
        "storageType": meta.get("storageType"),
        "hasTag": meta.get("hasTag", False),
        "tagColumn": meta.get("tagColumn"),
        "tagName": meta.get("tagName"),
        "tagRange": meta.get("tagRange"),
        "failedTag": meta.get("failedTag"),
        "tagStrategy": meta.get("tagStrategy"),
        "tagRule": meta.get("tagRule"),
        "trainable": meta.get("trainable", True),
        "categoryId": meta.get("categoryId"),
        "categoryName": meta.get("categoryName"),
        "description": meta.get("description"),
        "columns": meta.get("columns"),
        "rowCount": meta.get("rowCount"),
        "fileSize": meta.get("fileSize"),
        "createdAt": meta.get("createdAt"),
        "updatedAt": meta.get("updatedAt"),
    }


# ---------- 目录遍历 ----------

def iter_data_files(base_dir: str):
    """遍历目录下所有数据文件及其配套 meta。

    Yields (file_path, meta_dict) 元组。
    """
    for file_path in Path(base_dir).rglob("*"):
        if not file_path.is_file():
            continue
        if str(file_path).endswith(META_SUFFIX):
            continue
        if not has_meta(str(file_path)):
            continue
        try:
            yield file_path, read_meta(str(file_path))
        except Exception:
            continue


# ---------- 迁移脚本 ----------

def migrate_all(
    scan_dirs: Optional[List[Path]] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """为所有数据文件批量补齐元数据。

    Linux + xattr 模式下会写入扩展属性；其他平台继续写 sidecar `.meta.json`。
    遍历指定目录下所有数据文件（CSV/JSON/TSV），为缺少元数据的文件创建基础记录。

    Args:
        scan_dirs: 需要扫描的目录列表，默认扫描所有已知数据目录。
        dry_run: 若为 True，只统计不实际写入。

    Returns:
        {"created": int, "skipped": int, "errors": int, "details": [...]}
    """
    if scan_dirs is None:
        scan_dirs = [
            NIFI_BASE_DIR / "inbox_csv",
            NIFI_BASE_DIR / "inbox_json",
            NIFI_BASE_DIR / "inbox_tsv",
            NIFI_BASE_DIR / "csv_to_json",
            NIFI_BASE_DIR / "json_to_csv",
            NIFI_BASE_DIR / "csv_to_tsv",
            NIFI_BASE_DIR / "tsv_to_csv",
            NIFI_BASE_DIR / "json_to_tsv",
            NIFI_BASE_DIR / "tsv_to_json",
            TAGGED_OUTPUT_DIR,
            NIFI_BASE_DIR / "output_csv",
            NIFI_BASE_DIR / "output_json",
            NIFI_BASE_DIR / "output_tsv",
        ]

    created = 0
    skipped = 0
    errors = 0
    details: List[Dict[str, Any]] = []

    for base_dir in scan_dirs:
        if not base_dir.exists():
            continue
        for file_path in base_dir.rglob("*"):
            if not file_path.is_file():
                continue
            if str(file_path).endswith(META_SUFFIX):
                continue
            if has_meta(str(file_path)):
                skipped += 1
                continue

            # 判断文件格式
            suffix = file_path.suffix.lower()
            fmt_map = {".csv": "csv", ".json": "json", ".jsonl": "json", ".ndjson": "json", ".tsv": "tsv"}
            fmt = fmt_map.get(suffix, "unknown")
            if fmt == "unknown":
                continue

            # 判断是否在标签目录
            has_tag = is_tagged_file(str(file_path))

            # 尝试读取文件获取列信息和行数
            columns: Optional[List[str]] = None
            row_count: Optional[int] = None
            try:
                text = file_path.read_text(encoding="utf-8")
                if fmt == "csv":
                    import csv
                    reader = csv.DictReader(text.splitlines())
                    columns = reader.fieldnames
                    row_count = sum(1 for _ in reader)
                elif fmt == "json":
                    data = json.loads(text)
                    if isinstance(data, list):
                        row_count = len(data)
                        if data:
                            columns = sorted({k for o in data if isinstance(o, dict) for k in o.keys()})
                    elif isinstance(data, dict):
                        row_count = 1
                        columns = sorted(data.keys())
                elif fmt == "tsv":
                    lines = text.splitlines()
                    if lines:
                        columns = lines[0].split("\t")
                        row_count = len(lines) - 1
            except Exception:
                pass

            try:
                file_size = file_path.stat().st_size
                meta = build_meta(
                    file_path=str(file_path),
                    source_type="migration",
                    has_tag=has_tag,
                    columns=columns,
                    row_count=row_count,
                    file_size=file_size,
                )
                if not dry_run:
                    write_meta(str(file_path), meta)
                created += 1
                details.append({
                    "fileId": meta.get("fileId"),
                    "fileName": meta.get("fileName"),
                    "status": "dry_run" if dry_run else "created",
                })
            except Exception as e:
                errors += 1
                details.append({
                    "fileName": file_path.name,
                    "status": "error",
                    "error": str(e),
                })

    return {"created": created, "skipped": skipped, "errors": errors, "details": details}


# ---------- 完整性校验 ----------

def check_integrity(base_dir: Optional[Path] = None) -> Dict[str, Any]:
    """校验数据文件与元数据的完整性。

    检测：
    - 有数据文件但没有可读元数据的（孤儿文件）
    - 遗留 `.meta.json` 存在但数据文件不存在的（孤儿 meta）
    - 元数据存在但无法读取/解密的（损坏或密钥不匹配）

    Args:
        base_dir: 要扫描的目录，默认 NIFI_BASE_DIR。

    Returns:
        {
            "orphan_files": [...],
            "orphan_metas": [...],
            "unreadable_meta": [...],
            "total_data_files": int,
            "total_meta_files": int,
            "xattr_enabled": bool,
            "xattr_files": int,
        }
    """
    if base_dir is None:
        base_dir = NIFI_BASE_DIR

    orphan_files: List[str] = []
    orphan_metas: List[str] = []
    unreadable_meta: List[Dict[str, str]] = []
    total_data_files = 0
    total_meta_files = 0
    xattr_files = 0

    for file_path in base_dir.rglob("*"):
        if not file_path.is_file():
            continue
        # 跳过 meta_backups 目录中的备份文件
        if "meta_backups" in file_path.parts:
            continue
        if is_meta_file(str(file_path)):
            total_meta_files += 1
            data_fp = data_file_from_meta(str(file_path))
            if not Path(data_fp).exists():
                orphan_metas.append(str(file_path))
        else:
            total_data_files += 1
            if not has_meta(str(file_path)):
                orphan_files.append(str(file_path))
                continue
            if _using_xattr:
                xattr_files += 1
            try:
                read_meta(str(file_path))
            except Exception as exc:
                unreadable_meta.append({"file": str(file_path), "error": str(exc)})

    return {
        "orphan_files": orphan_files,
        "orphan_metas": orphan_metas,
        "unreadable_meta": unreadable_meta,
        "total_data_files": total_data_files,
        "total_meta_files": total_meta_files,
        "xattr_enabled": _using_xattr,
        "xattr_files": xattr_files,
    }


# ---------- storagePath 泄露检测 ----------

SECRET_PATTERNS = [
    "/home/", "/root/", "/etc/", "/var/", "/tmp/", "/opt/",
    "nifi-data", "password", "token", "secret", "credential",
]


def check_storage_path_leak(meta: Dict[str, Any]) -> List[str]:
    """检测 meta 中是否包含敏感路径信息。

    Returns:
        泄露问题列表，空列表表示安全。
    """
    issues: List[str] = []
    sp = meta.get("storagePath", "")
    if sp:
        for pattern in SECRET_PATTERNS:
            if pattern in sp:
                issues.append(f"storagePath 包含敏感路径: {pattern}")
    return issues


def sanitize_meta_for_api(meta: Dict[str, Any]) -> Dict[str, Any]:
    """移除 meta 中不适合通过 API 返回的字段（如 storagePath）。"""
    safe = {k: v for k, v in meta.items() if k not in ("storagePath",)}
    # 同时检查并警告
    leaks = check_storage_path_leak(meta)
    if leaks:
        safe["_security_warnings"] = leaks
    return safe
