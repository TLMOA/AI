import csv
import json
import os
import re
import uuid
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import inspect, text

from .engine_factory import engine_from_config

# Add Hadoop/HDFS related imports
try:
    from hdfs import InsecureClient
except ImportError:
    InsecureClient = None

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
    # For Hadoop services, we don't use incremental key logic
    # Return a default value or raise an exception to indicate that 
    # incremental export is not supported for Hadoop services
    if hasattr(engine, '__class__'):
        class_name = engine.__class__.__name__
        if class_name in ['Connection', 'InsecureClient']:  # Hive, HBase, or HDFS connection
            raise ValueError("Incremental export not supported for Hadoop services")
    
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


def _export_from_hive(db_conf: Dict, table: str, output_path: Path, file_format: str = "csv") -> Dict[str, Any]:
    """Export data from Hive table to specified format"""
    try:
        from pyhive import hive
    except ImportError:
        return {
            "status": "error",
            "error": "pyhive is required for Hive connections. Please install it with: pip install pyhive"
        }
    
    host = db_conf.get("host", "127.0.0.1")
    port = db_conf.get("port", 10000)
    user = db_conf.get("user", "hive")
    password = db_conf.get("password", "")
    database = db_conf.get("database", "default")
    auth = db_conf.get("auth", "NOSASL")
    
    try:
        # Connect to Hive
        conn = hive.Connection(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
            auth=auth
        )
        cursor = conn.cursor()
        
        # Execute query to fetch all data from the table
        query = f"SELECT * FROM {table}"
        cursor.execute(query)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        # Convert to dictionary format for consistent processing
        dict_rows = []
        for row in rows:
            dict_row = {}
            for i, col in enumerate(columns):
                dict_row[col] = row[i] if i < len(row) else None
            dict_rows.append(dict_row)
        
        # Write data to output file based on format
        if file_format.lower() == "csv":
            with output_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                for row in rows:
                    writer.writerow(["" if val is None else str(val) for val in row])
        elif file_format.lower() == "tsv":
            with output_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter="\t")
                writer.writerow(columns)
                for row in rows:
                    writer.writerow(["" if val is None else str(val) for val in row])
        elif file_format.lower() == "json":
            with output_path.open("w", encoding="utf-8") as f:
                for dict_row in dict_rows:
                    f.write(json.dumps(dict_row, ensure_ascii=False, default=str))
                    f.write("\n")
        
        cursor.close()
        conn.close()
        
        return {
            "status": "ok",
            "path": str(output_path),
            "rows": len(rows),
            "columns": len(columns)
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


def _export_from_hbase(db_conf: Dict, table: str, output_path: Path, file_format: str = "csv") -> Dict[str, Any]:
    """Export data from HBase table to specified format"""
    try:
        import happybase
    except ImportError:
        return {
            "status": "error",
            "error": "happybase is required for HBase connections. Please install it with: pip install happybase"
        }
    
    host = db_conf.get("host", "127.0.0.1")
    port = db_conf.get("port", 9090)
    user = db_conf.get("user", "")
    
    try:
        # Connect to HBase
        connection = happybase.Connection(host=host, port=port)
        hbase_table = connection.table(table)
        
        # Scan all rows in the table
        rows = []
        columns_list = set()  # To track all unique columns
        
        # Handle potential empty scan results
        scanner = hbase_table.scan()
        for key, data in scanner:
            row_dict = {"row_key": key.decode('utf-8', errors='replace')}  # Add row key as a special column
            for col_family_qual, value in data.items():
                col_name = col_family_qual.decode('utf-8', errors='replace')
                row_dict[col_name] = value.decode('utf-8', errors='replace')
                columns_list.add(col_name)
            rows.append(row_dict)
        
        # Get all unique columns
        all_columns = ["row_key"] + sorted(list(columns_list))
        
        # Write data to output file based on format
        if file_format.lower() == "csv":
            with output_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=all_columns)
                writer.writeheader()
                for row in rows:
                    # Ensure all columns are present in each row
                    safe_row = {}
                    for col in all_columns:
                        safe_row[col] = row.get(col, "")
                    writer.writerow(safe_row)
        elif file_format.lower() == "tsv":
            with output_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=all_columns, delimiter="\t")
                writer.writeheader()
                for row in rows:
                    # Ensure all columns are present in each row
                    safe_row = {}
                    for col in all_columns:
                        safe_row[col] = row.get(col, "")
                    writer.writerow(safe_row)
        elif file_format.lower() == "json":
            with output_path.open("w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row, ensure_ascii=False, default=str))
                    f.write("\n")
        
        connection.close()
        
        return {
            "status": "ok",
            "path": str(output_path),
            "rows": len(rows),
            "columns": len(all_columns)
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


def _export_from_hdfs(db_conf: Dict, hdfs_path: str, output_path: Path, file_format: str = "csv") -> Dict[str, Any]:
    """Export data from HDFS to specified format"""
    try:
        from hdfs import InsecureClient
    except ImportError:
        return {
            "status": "error",
            "error": "hdfs is required for HDFS connections. Please install it with: pip install hdfs"
        }
    
    host = db_conf.get("host", "127.0.0.1")
    port = db_conf.get("port", 9870)
    user = db_conf.get("user", "hdfs")
    
    hdfs_url = f"http://{host}:{port}"
    
    try:
        # Connect to HDFS
        client = InsecureClient(hdfs_url, user=user)
        
        # Download file from HDFS to local
        client.download(hdfs_path, str(output_path))
        
        return {
            "status": "ok",
            "path": str(output_path),
            "message": f"Downloaded {hdfs_path} from HDFS to {output_path}"
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


def run_export_job(job_record: dict) -> dict:
    """job_record expects keys: job_name, owner_id, db_config, file_format, destination"""
    job_id = job_record.get("id") or uuid.uuid4().hex[:8]
    factory_id = job_record.get("factory_id") or os.getenv("DEFAULT_FACTORY_ID", "factory-001")
    customer = job_record.get("owner_id") or "unknown"
    # mirror/export directory (per-job archive)
    dest_dir = ensure_export_dir(customer, job_id)
    fmt = (job_record.get("file_format") or "csv").lower()
    state = _load_state(dest_dir)
    filename = state.get("output_file") or f"job_{job_id}_{now_ts()}.{fmt}"

    # Determine primary output directory:
    # 1) if job_record.destination.path provided, use it
    # 2) otherwise default to NIFI_BASE_DIR/output_<format>
    destination = job_record.get("destination") or {}
    primary_dir = None
    if isinstance(destination, dict):
        p = destination.get("path")
        if p:
            primary_dir = Path(p)
    if primary_dir is None:
        # use NIFI base dir as root and create format-specific folder
        nifi_root = Path(os.getenv("NIFI_BASE_DIR", str(DEFAULT_NIFI_BASE)))
        primary_dir = nifi_root / f"output_{fmt}"
    primary_dir.mkdir(parents=True, exist_ok=True)

    out_path = primary_dir / filename

    db_conf = job_record.get("db_config") or {}
    table = None
    payload = job_record.get("payload") or {}
    if isinstance(payload, dict):
        table = payload.get("table")
    if not table and isinstance(db_conf, dict):
        table = db_conf.get("table")

    if not table:
        # no table provided - create a tiny demo file in primary output and mirror to exports
        with out_path.open("w", encoding="utf-8") as f:
            f.write("id,ts\n")
            for i in range(1, 6):
                f.write(f"{i},{now_ts()}\n")
        # mirror to per-job export archive
        try:
            mirror_path = dest_dir / filename
            shutil.copy2(str(out_path), str(mirror_path))
        except Exception:
            mirror_path = None
        _append_audit(dest_dir, {"status": "demo_written", "factory_id": factory_id, "primary_path": str(out_path), "mirror_path": str(mirror_path) if mirror_path else None})
        return {"status": "demo_written", "path": str(out_path)}

    # Get database type to determine export method
    db_type = (db_conf.get("db_type") or "mysql").lower()
    
    # Handle different database types
    if db_type == "hive":
        return _export_from_hive(db_conf, table, out_path, fmt)
    elif db_type == "hbase":
        return _export_from_hbase(db_conf, table, out_path, fmt)
    elif db_type == "hdfs":
        # For HDFS, the "table" parameter represents the file path in HDFS
        hdfs_path = table
        return _export_from_hdfs(db_conf, hdfs_path, out_path, fmt)
    else:
        # Original logic for traditional databases
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

            # append rows to primary output
            _append_rows(out_path, fmt, cols, rows)

            # mirror/copy the primary output to per-job export archive
            try:
                mirror_path = dest_dir / filename
                shutil.copy2(str(out_path), str(mirror_path))
            except Exception:
                mirror_path = None

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
                "primary_path": str(out_path),
                "mirror_path": str(mirror_path) if mirror_path else None,
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