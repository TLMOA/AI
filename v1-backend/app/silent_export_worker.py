"""Silent export executor.

Manifest-driven approach:
- When silent export is enabled, every manual table export automatically
  registers that table's (db_config, table) pair into a manifest.
- The background scheduler iterates the manifest and exports each entry.
- Output: /home/yhz/nifi-data/silent_exports/<tenant>/<db_key>/<table>_silent_export.csv
- First run = full snapshot; later runs = incremental append via marker column.
- Schema changes roll a new dated file and keep old files intact.
- Each table keeps a .meta.json file with execution metadata.
"""
from pathlib import Path
import csv
import hashlib
import json
from datetime import datetime
import time
import fcntl
from typing import Optional, Dict, Any, List, Tuple
import shutil
import os

from .engine_factory import engine_from_config
import sqlalchemy


BASE_DIR = Path(__file__).resolve().parent.parent
GENERATED = BASE_DIR / "data" / "generated"
MANIFEST = GENERATED / "silent_export_manifest.json"
CONFIG = GENERATED / "silent_export_config.json"
NIFI_SILENT_DIR = Path("/home/yhz/nifi-data") / "silent_exports"  # deprecated — v4 使用 _get_silent_export_dir
SILENT_EXPORT_TMP_DIRNAME = "tmp"
DEFAULT_SCHEDULE = os.getenv("SILENT_EXPORT_SCHEDULE", "daily")
IN_DATA_BASE_DIR = Path(os.getenv("IN_DATA_BASE_DIR", "/home/yhz"))


def _normalize_username(value: Optional[str]) -> str:
    """v4: 标准化用户名。"""
    v = (value or "").strip().lower()
    return v if v else "admin"


def _get_silent_export_dir(username: str) -> Path:
    """v4: 返回用户 silent_exports 目录，自动创建，支持私有化 ceph_endpoint。"""
    from .db_models import IotUser, get_engine
    from sqlalchemy.orm import sessionmaker
    db_path = Path(__file__).resolve().parent.parent / "data" / "app.db"
    engine = get_engine(db_path)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    try:
        user = session.query(IotUser).filter(IotUser.username == username).first()
        if user and user.deployment_mode == "private" and user.ceph_endpoint:
            base = Path(user.ceph_endpoint)
        else:
            base = IN_DATA_BASE_DIR / _normalize_username(username)
    finally:
        session.close()
    root = base / "nifi-data" / "silent_exports"
    root.mkdir(parents=True, exist_ok=True)
    return root


# ── manifest ──────────────────────────────────────────────────────────

def _load_manifest() -> dict:
    if not MANIFEST.exists():
        return {}
    try:
        return json.loads(MANIFEST.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_manifest(data: dict):
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _is_silent_export_enabled(tenant: str) -> bool:
    """Check if silent export is enabled for a tenant."""
    if not CONFIG.exists():
        return False
    try:
        cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
        tenants = cfg.get("tenants", {})
        tcfg = tenants.get(tenant, {})
        return bool(tcfg.get("enabled"))
    except Exception:
        return False


def _db_key(db_conf: Dict[str, Any]) -> str:
    """Generate a unique identifier for a database configuration."""
    db_type = (db_conf.get("db_type") or "mysql").lower()
    if db_type == "sqlite":
        path = db_conf.get("path") or db_conf.get("database") or "unknown"
        name = Path(path).stem
        return f"sqlite_{name}"
    host = db_conf.get("host") or "127.0.0.1"
    port = db_conf.get("port") or 0
    database = db_conf.get("database") or db_conf.get("path") or "unknown"
    return f"{db_type}_{host}_{port}_{database}"


def _manifest_key(db_conf: Dict[str, Any], table: str) -> str:
    return f"{_db_key(db_conf)}|{table}"


def register_table(db_conf: Dict[str, Any], table: str, tenant: str):
    """Register a table into the silent export manifest.
    Called from the export handler after a successful manual export.
    No-op if silent export is not enabled for this tenant.
    """
    if not _is_silent_export_enabled(tenant):
        return

    key = _manifest_key(db_conf, table)
    manifest = _load_manifest()
    if key not in manifest:
        manifest[key] = {
            "db": db_conf,
            "table": table,
            "tenant": tenant,
            "registered_at": datetime.utcnow().isoformat(),
        }
        _save_manifest(manifest)
        print(f"silent export: registered {key} for tenant={tenant}")


# ── helpers ────────────────────────────────────────────────────────────

def _schema_hash(columns: List[str]) -> str:
    return hashlib.sha256("|".join(columns).encode("utf-8")).hexdigest()[:16]


def _file_name_for_table(table: str, suffix: str = "") -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"export_system_{table}_{ts}{suffix}.csv"


def _acquire_lock(fp):
    try:
        fcntl.flock(fp.fileno(), fcntl.LOCK_EX)
        return True
    except Exception:
        return False


def _release_lock(fp):
    try:
        fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass


def _append_atomic(final_path: Path, tmp_path: Path):
    final_path.parent.mkdir(parents=True, exist_ok=True)
    with open(final_path, "a+b") as f_final:
        if not _acquire_lock(f_final):
            raise RuntimeError("cannot lock final file")
        try:
            with tmp_path.open("rb") as f_tmp:
                shutil.copyfileobj(f_tmp, f_final)
                f_final.flush()
                os.fsync(f_final.fileno())
        finally:
            _release_lock(f_final)


def _read_table_rows(conn, table: str, marker_col: Optional[str], marker_value: Any = None) -> Tuple[List[str], List[Dict[str, Any]]]:
    # Use dialect-appropriate identifier quoting (e.g. backticks for MySQL)
    from sqlalchemy.sql import quoted_name
    import sqlalchemy as sa
    qt = quoted_name(table, True)
    if marker_col is None or marker_value is None:
        q = sa.text(f'SELECT * FROM {qt}')
        params = {}
    else:
        qm = quoted_name(marker_col, True)
        q = sa.text(f'SELECT * FROM {qt} WHERE {qm} > :m')
        params = {"m": marker_value}
    res = conn.execute(q, params)
    cols = list(res.keys())
    rows = [dict(r._mapping) for r in res.fetchall()]
    return cols, rows


def _write_csv(path: Path, columns: List[str], rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(columns)
        for row in rows:
            writer.writerow(["" if row.get(c) is None else str(row.get(c)) for c in columns])
        fh.flush()
        os.fsync(fh.fileno())


# ── per-table export ───────────────────────────────────────────────────

def _export_table(engine, tenant: str, db_key: str, table: str,
                  marker_col: Optional[str], last_marker) -> Optional[dict]:
    out_dir = _get_silent_export_dir(tenant) / db_key
    tmp_dir = out_dir / SILENT_EXPORT_TMP_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    meta_file = out_dir / f"{table}_silent_export.csv.meta.json"
    meta: Dict[str, Any] = {}
    if meta_file.exists():
        try:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
        except Exception:
            meta = {}

    with engine.connect() as conn:
        columns_all, full_rows = _read_table_rows(conn, table, None)
        current_schema = _schema_hash(columns_all)
        previous_schema = meta.get("schema_hash")
        schema_changed = bool(previous_schema and previous_schema != current_schema)

        if schema_changed:
            suffix = f"_{datetime.utcnow().strftime('%Y%m%d')}"
            final_file = out_dir / _file_name_for_table(table, suffix)
        else:
            final_file = out_dir / _file_name_for_table(table)

        effective_marker = last_marker if last_marker is not None else meta.get("last_export_marker")
        if effective_marker is None:
            effective_marker_by_table = meta.get("last_export_marker_by_table") or {}
            effective_marker = effective_marker_by_table.get(table)

        if effective_marker is None or not marker_col:
            columns, rows = columns_all, full_rows
            trigger_reason = "initial_full_export" if not final_file.exists() else "full_refresh"
        else:
            columns, rows = _read_table_rows(conn, table, marker_col, effective_marker)
            trigger_reason = "incremental_append"

        tmp_file = tmp_dir / f"{table}.{int(time.time())}.csv"
        _write_csv(tmp_file, columns, rows)

        if not final_file.exists() or schema_changed:
            tmp_file.replace(final_file)
            rows_exported = len(rows)
        else:
            _append_atomic(final_file, tmp_file)
            rows_exported = len(rows)
            tmp_file.unlink(missing_ok=True)

        new_marker = effective_marker
        if marker_col and rows:
            last_row = rows[-1]
            new_marker = last_row.get(marker_col, effective_marker)

        current_meta = {
            "tenant": tenant,
            "db_key": db_key,
            "table": table,
            "traceId": f"silent-{int(time.time())}",
            "jobId": f"silent-{tenant}-{db_key}-{table}",
            "operator": "scheduler",
            "triggerReason": trigger_reason,
            "last_export_marker": new_marker,
            "rows_exported": rows_exported,
            "timestamp": datetime.utcnow().isoformat(),
            "schema_hash": current_schema,
            "schema_changed": schema_changed,
            "source_file": str(final_file),
        }
        meta.update(current_meta)
        meta.setdefault("last_export_marker_by_table", {})[table] = new_marker
        meta_file.write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

        return {"rows_exported": rows_exported, "new_marker": new_marker, "schema_changed": schema_changed}


# ── main processing loop ───────────────────────────────────────────────

def _get_marker_column_from_config(tenant: str) -> Optional[str]:
    if not CONFIG.exists():
        return None
    try:
        cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
        tenants = cfg.get("tenants", {})
        tcfg = tenants.get(tenant, {})
        return tcfg.get("incremental_marker_column", "updated_at")
    except Exception:
        return "updated_at"


def process_once(tenant_filter: Optional[str] = None):
    manifest = _load_manifest()
    if not manifest:
        print("no entries in silent export manifest")
        return 0

    # group entries by tenant
    entries_by_tenant: Dict[str, List[Dict[str, Any]]] = {}
    for key, entry in manifest.items():
        tenant = entry.get("tenant", "unknown")
        if tenant_filter and tenant != tenant_filter:
            continue
        entries_by_tenant.setdefault(tenant, []).append(entry)

    if not entries_by_tenant:
        print("no enabled silent export tenants")
        return 0

    processed = 0
    for tenant, entries in entries_by_tenant.items():
        # only process if silent export is still enabled for this tenant
        if not _is_silent_export_enabled(tenant):
            continue

        marker_col = _get_marker_column_from_config(tenant)
        print(f"processing silent export for tenant={tenant}, {len(entries)} table(s)")

        for entry in entries:
            db_conf = entry.get("db") or {}
            table = entry.get("table")
            db_key = _db_key(db_conf)

            if not db_conf or not table:
                print(f"skipping malformed entry: {entry}")
                continue

            try:
                engine = engine_from_config(db_conf)
            except Exception as e:
                print(f"failed to connect for db_key={db_key}: {e}")
                continue

            try:
                res = _export_table(engine, tenant, db_key, table, marker_col, None)
                print(f"exported {db_key}/{table} rows={res['rows_exported'] if res else 0}")
            except Exception as e:
                print(f"failed exporting {db_key}/{table}: {e}")

        processed += 1

    return processed


def process_loop_once():
    try:
        return process_once()
    except Exception as exc:
        print(f"silent export scheduler run failed: {exc}")
        return 0


if __name__ == "__main__":
    process_once()