"""Worker to process silent export requests.

Features:
- Reads `data/generated/silent_export_requests.ndjson` for queued triggers.
- Loads per-tenant configuration from `data/generated/silent_export_config.json`.
- Connects to target DB using `engine_from_config` and exports each table to
  `nifi_data/silent_exports/<tenant>/<table>_silent_export.csv`.
- Supports incremental export using `incremental_marker_column` (timestamp or numeric).
"""
from pathlib import Path
import json
from datetime import datetime
import time
import fcntl
from typing import Optional
import shutil
import os

from .engine_factory import engine_from_config
import sqlalchemy


BASE_DIR = Path(__file__).resolve().parent.parent
GENERATED = BASE_DIR / "data" / "generated"
REQUESTS = GENERATED / "silent_export_requests.ndjson"
CONFIG = GENERATED / "silent_export_config.json"
NIFI_SILENT_DIR = BASE_DIR / "nifi_data" / "silent_exports"


def _load_config() -> dict:
    if not CONFIG.exists():
        return {"tenants": {}}
    try:
        return json.loads(CONFIG.read_text(encoding="utf-8"))
    except Exception:
        return {"tenants": {}}


def _save_config(data: dict):
    CONFIG.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


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
    # Append tmp_path contents to final_path atomically by using file-level lock
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


def _export_table(engine, tenant: str, table: str, marker_col: Optional[str], last_marker) -> Optional[dict]:
    """Export either full table (if last_marker is None) or incremental rows.

    Returns dict with rows_exported and new_last_marker.
    """
    out_dir = NIFI_SILENT_DIR / tenant
    out_dir.mkdir(parents=True, exist_ok=True)
    final_file = out_dir / f"{table}_silent_export.csv"
    tmp_file = out_dir / f".{table}.append.tmp"

    with engine.connect() as conn:
        # get columns
        try:
            try:
                res0 = conn.execute(sqlalchemy.text(f"SELECT * FROM `{table}` LIMIT 1"))
                cols = [c[0] for c in res0.cursor.description] if getattr(res0, 'cursor', None) else []
            except Exception:
                cols = []
        except Exception:
            cols = []
        # build query
        if last_marker is None:
            q = f"SELECT * FROM `{table}`"
            params = {}
        else:
            if marker_col:
                q = f"SELECT * FROM `{table}` WHERE `{marker_col}` > :m"
                params = {"m": last_marker}
            else:
                # no marker col, do full export (conservative)
                q = f"SELECT * FROM `{table}`"
                params = {}
        res = conn.execute(sqlalchemy.text(q), params)
        rows = res.fetchall()
        if not rows:
            return {"rows_exported": 0, "new_marker": last_marker}
        # write tmp CSV
        import csv
        tmp_file.unlink(missing_ok=True)
        with tmp_file.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            # write header only if final_file not exists
            if not final_file.exists():
                # headers from cursor
                cursor = getattr(res, 'cursor', None)
                if cursor is not None and getattr(cursor, 'description', None):
                    headers = [d[0] for d in cursor.description]
                else:
                    headers = []
                writer.writerow(headers)
            for r in rows:
                writer.writerow(list(r))
        # fsync tmp
        with tmp_file.open("rb") as ftmp:
            ftmp.flush()
        # append to final atomically
        # If final doesn't exist, rename tmp to final
        if not final_file.exists():
            tmp_file.rename(final_file)
        else:
            # append
            with open(final_file, "ab") as f_final, open(tmp_file, "rb") as f_tmp:
                if _acquire_lock(f_final):
                    try:
                        shutil.copyfileobj(f_tmp, f_final)
                        f_final.flush()
                        try:
                            import os

                            os.fsync(f_final.fileno())
                        except Exception:
                            pass
                    finally:
                        _release_lock(f_final)
            tmp_file.unlink(missing_ok=True)

        # compute new marker
        new_marker = last_marker
        cursor = getattr(res, 'cursor', None)
        if marker_col and cursor is not None and getattr(cursor, 'description', None):
            # try to derive last marker from last row
            try:
                last_row = rows[-1]
                # find index
                idx = [d[0] for d in cursor.description].index(marker_col)
                new_marker = last_row[idx]
            except Exception:
                pass

        return {"rows_exported": len(rows), "new_marker": new_marker}


def process_once():
    # read queued requests
    if not REQUESTS.exists():
        print("no pending requests")
        return
    lines = REQUESTS.read_text(encoding="utf-8").strip().splitlines()
    if not lines:
        print("no pending requests")
        return
    # load config
    cfg = _load_config()
    tenants_cfg = cfg.get("tenants", {})

    remaining = []
    for ln in lines:
        try:
            job = json.loads(ln)
        except Exception:
            continue
        if job.get("status") != "queued":
            continue
        tenant = job.get("tenant") or "factory-001"
        tcfg = tenants_cfg.get(tenant, {})
        if not tcfg or not tcfg.get("enabled"):
            print(f"silent export disabled for tenant={tenant}")
            continue

        print(f"processing silent export for tenant={tenant}")
        # build DB engine config: allow per-tenant db conf under tcfg['db'] or fallback to env via engine_from_config
        db_conf = tcfg.get("db") or {}
        if not db_conf:
            # try env defaults
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
        except Exception as e:
            print(f"failed to init engine for tenant={tenant}: {e}")
            continue

        # list tables (try MySQL-ish, fall back to sqlite)
        try:
            with engine.connect() as conn:
                try:
                    tbls = [r[0] for r in conn.execute(sqlalchemy.text("SHOW TABLES")).fetchall()]
                except Exception:
                    # sqlite fallback
                    try:
                        tbls = [r[0] for r in conn.execute(sqlalchemy.text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()]
                    except Exception as e:
                        raise
        except Exception as e:
            print(f"failed to list tables for tenant={tenant}: {e}")
            continue

        marker_col = tcfg.get("incremental_marker_column")
        last_marker = tcfg.get("last_export_marker")

        for table in tbls:
            try:
                res = _export_table(engine, tenant, table, marker_col, last_marker)
                print(f"exported table={table} rows={res['rows_exported']}")
                if res and res.get("new_marker") is not None:
                    # update last_marker per table: store under tenants.<tenant>.last_export_marker_by_table
                    per_table = tenants_cfg.setdefault(tenant, {}).setdefault("last_export_marker_by_table", {})
                    per_table[table] = res.get("new_marker")
                    # persist
                    cfg["tenants"] = tenants_cfg
                    _save_config(cfg)
            except Exception as e:
                print(f"failed exporting table={table} for tenant={tenant}: {e}")

    # clear requests after processing
    try:
        REQUESTS.unlink()
    except Exception:
        pass


if __name__ == "__main__":
    process_once()
