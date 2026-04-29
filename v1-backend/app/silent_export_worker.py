"""Silent export executor.

Implements the方案 requirements at a pragmatic level:
- per-tenant config stored in `data/generated/silent_export_config.json`
- first run emits a full table snapshot
- later runs append incrementally by marker column
- schema changes roll a new dated file and keep old files intact
- each table keeps a `.meta.json` file with execution metadata
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
REQUESTS = GENERATED / "silent_export_requests.ndjson"
CONFIG = GENERATED / "silent_export_config.json"
NIFI_SILENT_DIR = Path("/home/yhz/iot/nifi_data") / "silent_exports"
SILENT_EXPORT_TMP_DIRNAME = "tmp"
DEFAULT_SCHEDULE = os.getenv("SILENT_EXPORT_SCHEDULE", "daily")


def _load_config() -> dict:
    if not CONFIG.exists():
        return {"tenants": {}}
    try:
        return json.loads(CONFIG.read_text(encoding="utf-8"))
    except Exception:
        return {"tenants": {}}


def _save_config(data: dict):
    CONFIG.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _schema_hash(columns: List[str]) -> str:
    return hashlib.sha256("|".join(columns).encode("utf-8")).hexdigest()[:16]


def _file_name_for_table(table: str, suffix: str = "") -> str:
    return f"{table}_silent_export{suffix}.csv"


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
    q = f'SELECT * FROM "{table}"' if marker_col is None or marker_value is None else f'SELECT * FROM "{table}" WHERE "{marker_col}" > :m'
    res = conn.execute(sqlalchemy.text(q), {"m": marker_value} if marker_col is not None and marker_value is not None else {})
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


def _export_table(engine, tenant: str, table: str, marker_col: Optional[str], last_marker) -> Optional[dict]:
    out_dir = NIFI_SILENT_DIR / tenant
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
            "table": table,
            "traceId": f"silent-{int(time.time())}",
            "jobId": f"silent-{tenant}-{table}",
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


def _iter_enabled_tenants(cfg: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    tenants = cfg.get("tenants", {}) if isinstance(cfg, dict) else {}
    items = []
    for tenant, tcfg in tenants.items():
        if isinstance(tcfg, dict) and tcfg.get("enabled"):
            items.append((tenant, tcfg))
    return items


def _get_table_list(engine) -> List[str]:
    with engine.connect() as conn:
        try:
            tbls = [r[0] for r in conn.execute(sqlalchemy.text("SHOW TABLES")).fetchall()]
        except Exception:
            tbls = [r[0] for r in conn.execute(sqlalchemy.text("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'" )).fetchall()]
    return tbls


def process_once(tenant_filter: Optional[str] = None):
    cfg = _load_config()
    tenants_cfg = cfg.get("tenants", {})
    targets = _iter_enabled_tenants(cfg)
    if tenant_filter:
        targets = [(t, c) for t, c in targets if t == tenant_filter]

    if not targets:
        print("no enabled silent export tenants")
        return 0

    processed = 0
    for tenant, tcfg in targets:
        print(f"processing silent export for tenant={tenant}")
        db_conf = tcfg.get("db") or {}
        if not db_conf:
            db_conf = {
                "db_type": "mysql",
                "user": tcfg.get("db_user") or "root",
                "password": tcfg.get("db_password") or "root",
                "host": tcfg.get("db_host") or "127.0.0.1",
                "port": int(tcfg.get("db_port") or 3306),
                "database": tcfg.get("db_name") or "test",
            }

        try:
            engine = engine_from_config(db_conf)
            tbls = _get_table_list(engine)
        except Exception as e:
            print(f"failed to init/list for tenant={tenant}: {e}")
            continue

        marker_col = tcfg.get("incremental_marker_column")
        last_marker_by_table = tcfg.get("last_export_marker_by_table") or {}
        for table in tbls:
            try:
                res = _export_table(engine, tenant, table, marker_col, last_marker_by_table.get(table))
                if res and res.get("new_marker") is not None:
                    per_table = tenants_cfg.setdefault(tenant, {}).setdefault("last_export_marker_by_table", {})
                    per_table[table] = res.get("new_marker")
                    cfg["tenants"] = tenants_cfg
                    _save_config(cfg)
                print(f"exported table={table} rows={res['rows_exported'] if res else 0}")
            except Exception as e:
                print(f"failed exporting table={table} for tenant={tenant}: {e}")
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
