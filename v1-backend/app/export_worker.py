import csv
import os
from pathlib import Path
from datetime import datetime
from sqlalchemy import text
from .engine_factory import engine_from_config
# avoid importing main to prevent circular imports; read base dir from env
DEFAULT_NIFI_BASE = Path(os.getenv("NIFI_BASE_DIR", "/home/yhz/nifi-data"))
import uuid


def now_ts():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_export_dir(customer_id: str, job_id: str) -> Path:
    base = Path(os.getenv("NIFI_BASE_DIR", str(DEFAULT_NIFI_BASE)))
    dest = base / "exports" / str(customer_id or "unknown") / str(job_id) / now_ts()
    dest.mkdir(parents=True, exist_ok=True)
    return dest


def export_table_to_csv(db_conf: dict, table_name: str, out_path: Path, limit: int = 1000) -> dict:
    """Simple exporter: SELECT * FROM table LIMIT n and write CSV."""
    engine = engine_from_config(db_conf)
    conn = engine.connect()
    try:
        # use text to avoid SQLAlchemy reflection
        q = text(f"SELECT * FROM {table_name} LIMIT :limit")
        res = conn.execute(q, {"limit": limit})
        cols = res.keys()
        rows = res.fetchall()
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            for r in rows:
                writer.writerow([str(x) if x is not None else "" for x in r])
        return {"filePath": str(out_path), "rows": len(rows)}
    finally:
        conn.close()


def run_export_job(job_record: dict) -> dict:
    """job_record expects keys: job_name, owner_id, db_config, file_format, destination"""
    job_id = job_record.get("id") or uuid.uuid4().hex[:8]
    customer = job_record.get("owner_id") or "unknown"
    dest_dir = ensure_export_dir(customer, job_id)
    fmt = (job_record.get("file_format") or "csv").lower()
    filename = f"job_{job_id}_{now_ts()}.{fmt}"
    out_path = dest_dir / filename

    db_conf = job_record.get("db_config") or {}
    table = None
    payload = job_record.get("payload") or {}
    if isinstance(payload, dict):
        table = payload.get("table")

    if not table:
        # no table provided - create a tiny demo file
        with out_path.open("w", encoding="utf-8") as f:
            f.write("id,ts\n")
            for i in range(1, 6):
                f.write(f"{i},{now_ts()}\n")
        return {"status": "demo_written", "path": str(out_path)}

    # attempt real export
    try:
        result = export_table_to_csv(db_conf, table, out_path)
        return {"status": "ok", "path": str(out_path), "rows": result.get("rows")}
    except Exception as e:
        return {"status": "error", "error": str(e)}
