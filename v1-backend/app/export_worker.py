import csv
import json
import os
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import inspect, text

from .engine_factory import engine_from_config

# avoid importing main to prevent circular imports; read base dir from env
DEFAULT_NIFI_BASE = Path(os.getenv("NIFI_BASE_DIR", "/home/yhz/nifi-data"))


def now_ts():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_export_dir(customer_id: str, job_id: str) -> Path:
    base = Path(os.getenv("NIFI_BASE_DIR", str(DEFAULT_NIFI_BASE)))
    # Keep files directly under job_id as requested: .../{customer_id}/{job_id}/job_xxx_yyy.csv
    dest = base / "exports" / str(customer_id or "unknown") / str(job_id)
    dest.mkdir(parents=True, exist_ok=True)
    return dest


def _safe_ident(name: str) -> str:
    """Allow simple SQL identifiers (letters/digits/underscore, optional one dot for schema.table)."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?", name or ""):
        raise ValueError(f"unsafe identifier: {name}")
    return name


def _state_path(dest_dir: Path) -> Path:
    return dest_dir / ".state.json"


def _audit_path(dest_dir: Path) -> Path:
    return dest_dir / "audit.log"


def _load_state(dest_dir: Path) -> Dict[str, Any]:
    sp = _state_path(dest_dir)
    if not sp.exists():
        return {}
    try:
        return json.loads(sp.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(dest_dir: Path, state: Dict[str, Any]) -> None:
    sp = _state_path(dest_dir)
    sp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _append_audit(dest_dir: Path, event: Dict[str, Any]) -> None:
    payload = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        **event,
    }
    with _audit_path(dest_dir).open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, default=str))
        f.write("\n")


def _is_integer_column(col_type: Any) -> bool:
    name = col_type.__class__.__name__.lower()
    return any(x in name for x in ("int", "integer", "bigint", "smallint"))


def _resolve_schema_table(table_name: str) -> Tuple[Optional[str], str]:
    if "." in table_name:
        schema, tbl = table_name.split(".", 1)
        return schema, tbl
    return None, table_name


def _auto_increment_key(engine, table_name: str) -> str:
    schema, tbl = _resolve_schema_table(table_name)
    insp = inspect(engine)
    columns = insp.get_columns(tbl, schema=schema)
    pk = insp.get_pk_constraint(tbl, schema=schema) or {}
    pk_cols = pk.get("constrained_columns") or []

    col_map = {c.get("name"): c for c in columns}

    # 1) single-column integer PK is preferred
    if len(pk_cols) == 1:
        c = col_map.get(pk_cols[0])
        if c and _is_integer_column(c.get("type")):
            return pk_cols[0]

    # 2) conventional id names with integer types
    name_priority = ["id"] + [c.get("name") for c in columns if str(c.get("name", "")).endswith("_id")]
    for name in name_priority:
        c = col_map.get(name)
        if c and _is_integer_column(c.get("type")):
            return name

    # 3) first integer column
    for c in columns:
        if _is_integer_column(c.get("type")):
            return c.get("name")

    raise ValueError("no stable incremental integer key detected")


def _query_incremental_rows(engine, table_name: str, key_col: str, last_key: Optional[int]) -> Tuple[List[str], List[Dict[str, Any]]]:
    table = _safe_ident(table_name)
    key = _safe_ident(key_col)
    conn = engine.connect()
    try:
        if last_key is None:
            q = text(f"SELECT * FROM {table} ORDER BY {key} ASC")
            res = conn.execute(q)
        else:
            q = text(f"SELECT * FROM {table} WHERE {key} > :last_key ORDER BY {key} ASC")
            res = conn.execute(q, {"last_key": last_key})
        cols = list(res.keys())
        rows = [dict(r._mapping) for r in res.fetchall()]
        return cols, rows
    finally:
        conn.close()


def _append_csv(path: Path, columns: List[str], rows: List[Dict[str, Any]]) -> None:
    write_header = (not path.exists()) or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(columns)
        for r in rows:
            writer.writerow(["" if r.get(c) is None else str(r.get(c)) for c in columns])


def _append_tsv(path: Path, columns: List[str], rows: List[Dict[str, Any]]) -> None:
    write_header = (not path.exists()) or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="\t")
        if write_header:
            writer.writerow(columns)
        for r in rows:
            writer.writerow(["" if r.get(c) is None else str(r.get(c)) for c in columns])


def _append_json_lines(path: Path, rows: List[Dict[str, Any]]) -> None:
    with path.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, default=str))
            f.write("\n")


def _append_rows(path: Path, file_format: str, columns: List[str], rows: List[Dict[str, Any]]) -> None:
    fmt = (file_format or "csv").lower()
    if fmt == "csv":
        _append_csv(path, columns, rows)
        return
    if fmt == "tsv":
        _append_tsv(path, columns, rows)
        return
    if fmt == "json":
        _append_json_lines(path, rows)
        return
    raise ValueError(f"unsupported format: {fmt}")


def run_export_job(job_record: dict) -> dict:
    """job_record expects keys: job_name, owner_id, db_config, file_format, destination"""
    job_id = job_record.get("id") or uuid.uuid4().hex[:8]
    factory_id = job_record.get("factory_id") or os.getenv("DEFAULT_FACTORY_ID", "factory-001")
    customer = job_record.get("owner_id") or "unknown"
    dest_dir = ensure_export_dir(customer, job_id)
    fmt = (job_record.get("file_format") or "csv").lower()
    state = _load_state(dest_dir)
    filename = state.get("output_file") or f"job_{job_id}_{now_ts()}.{fmt}"
    out_path = dest_dir / filename

    db_conf = job_record.get("db_config") or {}
    table = None
    payload = job_record.get("payload") or {}
    if isinstance(payload, dict):
        table = payload.get("table")
    if not table and isinstance(db_conf, dict):
        table = db_conf.get("table")

    if not table:
        # no table provided - create a tiny demo file
        with out_path.open("w", encoding="utf-8") as f:
            f.write("id,ts\n")
            for i in range(1, 6):
                f.write(f"{i},{now_ts()}\n")
        _append_audit(dest_dir, {"status": "demo_written", "factory_id": factory_id, "path": str(out_path)})
        return {"status": "demo_written", "path": str(out_path)}

    # attempt real incremental export
    try:
        engine = engine_from_config(db_conf)
        key_col = state.get("incremental_key") or _auto_increment_key(engine, table)
        last_key = state.get("last_exported_key")
        cols, rows = _query_incremental_rows(engine, table, key_col, last_key)

        if not rows:
            _append_audit(dest_dir, {
                "status": "no_change",
                "factory_id": factory_id,
                "table": table,
                "incremental_key": key_col,
                "last_exported_key": last_key,
                "path": str(out_path),
            })
            return {"status": "no_change", "path": str(out_path), "rows": 0, "incremental_key": key_col}

        _append_rows(out_path, fmt, cols, rows)

        new_last = rows[-1].get(key_col)
        state.update({
            "output_file": filename,
            "table": table,
            "incremental_key": key_col,
            "last_exported_key": int(new_last) if new_last is not None else last_key,
            "last_run": datetime.now().isoformat(timespec="seconds"),
            "format": fmt,
        })
        _save_state(dest_dir, state)
        _append_audit(dest_dir, {
            "status": "ok",
            "factory_id": factory_id,
            "table": table,
            "incremental_key": key_col,
            "from_key": last_key,
            "to_key": state.get("last_exported_key"),
            "rows": len(rows),
            "path": str(out_path),
        })

        return {
            "status": "ok",
            "path": str(out_path),
            "rows": len(rows),
            "incremental_key": key_col,
            "last_exported_key": state.get("last_exported_key"),
        }
    except Exception as e:
        _append_audit(dest_dir, {
            "status": "error",
            "factory_id": factory_id,
            "table": table,
            "error": str(e),
        })
        return {"status": "error", "error": str(e)}
