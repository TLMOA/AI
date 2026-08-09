#!/usr/bin/env python3
"""NiFi-invoked multi-database export worker.

Supports 8 data sources via dbType field in task JSON:
  mysql, postgres/postgresql, sqlserver, oracle, sqlite, hive, hdfs, hbase

Executed by NiFi ExecuteStreamCommand. Reads task JSON from stdin,
connects using the appropriate driver, fetches data, writes output.
"""

import csv
import json
import os
import re
import sys
import tempfile
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

_IMPORT_ERRORS: Dict[str, Any] = {}

for _mod_name in ("pymysql", "psycopg2", "pymssql", "pyhive", "hdfs", "happybase"):
    try:
        __import__(_mod_name)
    except Exception as _exc:
        _IMPORT_ERRORS[_mod_name] = _exc


SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SAFE_WHERE = re.compile(r"^[A-Za-z0-9_`'\"\s.=<>!%()+\-*/,:]+$")


def now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def load_task() -> Dict[str, Any]:
    if len(sys.argv) > 1 and sys.argv[1].strip():
        return json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    content = sys.stdin.read().strip()
    if not content:
        raise ValueError("empty task json input")
    return json.loads(content)


def safe_ident(value: str, field_name: str) -> str:
    value = str(value or "").strip()
    if not SAFE_IDENT.fullmatch(value):
        raise ValueError(f"unsafe {field_name}: {value}")
    return value


def safe_where(value: str) -> str:
    value = str(value or "").strip()
    if not value:
        return ""
    if ";" in value or "--" in value or "/*" in value or "*/" in value:
        raise ValueError("unsafe where clause")
    if not SAFE_WHERE.fullmatch(value):
        raise ValueError("unsafe where clause")
    return value


def normalized_format(task: Dict[str, Any]) -> str:
    fmt = str(task.get("format") or "CSV").strip().lower()
    if fmt not in {"csv", "json", "tsv"}:
        raise ValueError(f"unsupported format: {fmt}")
    return fmt


def output_path(task: Dict[str, Any], fmt: str) -> Path:
    target_dir = Path(str(task.get("targetDir") or "")).expanduser()
    if not str(target_dir):
        root = Path(str(task.get("targetRoot") or "/opt/nifi/nifi-current/data/iot"))
        target_dir = root / f"output_{fmt}"
    target_dir.mkdir(parents=True, exist_ok=True)
    if task.get("targetFile"):
        return target_dir / str(task.get("targetFile"))
    owner = safe_ident(str(task.get("ownerId") or task.get("owner") or "unknown"), "owner")
    table = safe_ident(str(task.get("table") or "table"), "table")
    filename = f"export_{owner}_{table}_{now_ts()}.{fmt}"
    return target_dir / filename


def atomic_write(path: Path, writer) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            writer(f)
            f.flush()
            os.fsync(f.fileno())
        tmp_path.replace(path)
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise


def write_rows(path: Path, fmt: str, columns: List[str], rows: Iterable[Dict[str, Any]]) -> None:
    rows_list = list(rows)

    def write_csv_file(f):
        writer = csv.writer(f)
        writer.writerow(columns)
        for row in rows_list:
            writer.writerow(["" if row.get(col) is None else str(row.get(col)) for col in columns])

    def write_tsv_file(f):
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(columns)
        for row in rows_list:
            writer.writerow(["" if row.get(col) is None else str(row.get(col)) for col in columns])

    def write_json_file(f):
        for row in rows_list:
            f.write(json.dumps(row, ensure_ascii=False, default=str))
            f.write("\n")

    if fmt == "csv":
        atomic_write(path, write_csv_file)
    elif fmt == "tsv":
        atomic_write(path, write_tsv_file)
    else:
        atomic_write(path, write_json_file)


def status_dirs(task: Dict[str, Any]) -> Tuple[Path, Path]:
    root = Path(str(task.get("targetRoot") or "/opt/nifi/nifi-current/data/iot")) / "export_jobs"
    done = root / "done"
    error = root / "error"
    done.mkdir(parents=True, exist_ok=True)
    error.mkdir(parents=True, exist_ok=True)
    return done, error


def write_status(task: Dict[str, Any], payload: Dict[str, Any], failed: bool = False) -> Path:
    done, error = status_dirs(task)
    job_id = str(task.get("jobId") or f"export_{uuid.uuid4().hex[:8]}")
    status_path = (error if failed else done) / f"{job_id}.json"
    status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return status_path


def _default_gateway_ip() -> str:
    try:
        with open("/proc/net/route", "r", encoding="utf-8") as fh:
            for line in fh:
                parts = line.strip().split()
                if len(parts) >= 3 and parts[1] == "00000000":
                    gw = parts[2]
                    if len(gw) == 8:
                        raw = bytes.fromhex(gw)
                        gateway = raw[::-1]
                        return ".".join(str(b) for b in gateway)
    except Exception:
        pass
    return ""


def _resolve_database_hosts(task: Dict[str, Any]) -> list[str]:
    host = str(task.get("host") or "127.0.0.1").strip()
    candidates = [host]
    if host in {"127.0.0.1", "localhost"}:
        candidates.extend(["host.docker.internal", _default_gateway_ip(), "172.17.0.1", "172.18.0.1", "172.19.0.1"])
    # keep unique, preserve order
    seen = set()
    unique = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            unique.append(candidate)
    return unique


def connect_db(task: Dict[str, Any]):
    db_type = str(task.get("dbType") or "").strip().lower()
    hosts = _resolve_database_hosts(task)
    errors: list[str] = []

    def _connect(host_value: str):
        if db_type == "mysql":
            import pymysql
            return pymysql.connect(
                host=host_value,
                port=int(task.get("port") or 3306),
                user=str(task.get("user") or "root"),
                password=str(task.get("password") or ""),
                database=str(task.get("database") or ""),
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
            )
        if db_type in ("postgres", "postgresql"):
            import psycopg2
            import psycopg2.extras
            return psycopg2.connect(
                host=host_value,
                port=int(task.get("port") or 5432),
                user=str(task.get("user") or "postgres"),
                password=str(task.get("password") or ""),
                dbname=str(task.get("database") or ""),
                cursor_factory=psycopg2.extras.RealDictCursor,
            )
        if db_type == "sqlserver":
            import pymssql
            return pymssql.connect(
                server=host_value,
                port=int(task.get("port") or 1433),
                user=str(task.get("user") or "sa"),
                password=str(task.get("password") or ""),
                database=str(task.get("database") or ""),
            )
        if db_type == "oracle":
            import oracledb
            dsn = str(task.get("dsn") or f"{host_value}:{task.get('port',1521)}/{task.get('database','')}")
            return oracledb.connect(
                user=str(task.get("user") or ""),
                password=str(task.get("password") or ""),
                dsn=dsn,
            )
        if db_type == "sqlite":
            import sqlite3
            path = str(task.get("path") or task.get("database") or ":memory:")
            conn = sqlite3.connect(path)
            conn.row_factory = sqlite3.Row
            return conn
        if db_type == "hive":
            import pyhive.hive
            return pyhive.hive.connect(
                host=host_value,
                port=int(task.get("port") or 10000),
                username=str(task.get("user") or ""),
                database=str(task.get("database") or "default"),
            )
        if db_type == "hdfs":
            import hdfs
            url = str(task.get("dsn") or f"http://{host_value}:{task.get('port',50070)}")
            return hdfs.InsecureClient(url, user=str(task.get("user") or ""))
        if db_type == "hbase":
            import happybase
            return happybase.Connection(
                host=host_value,
                port=int(task.get("port") or 9090),
            )
        raise ValueError(f"unsupported dbType: {db_type}")

    for host_value in hosts:
        try:
            return _connect(host_value)
        except Exception as exc:
            errors.append(f"{host_value}:{str(exc)}")
    raise RuntimeError(f"failed to connect to {db_type} on hosts {hosts}: {'; '.join(errors)}")


def fetch_sql_data(task: Dict[str, Any]) -> Tuple[List[str], List[Dict[str, Any]]]:
    db_type = str(task.get("dbType") or "").strip().lower()
    database = str(task.get("database") or "")
    table = safe_ident(str(task.get("table") or ""), "table")
    where = safe_where(str(task.get("where") or ""))

    if db_type in ("sqlite",):
        table_name = safe_ident(table, "table")
        query = f'SELECT * FROM "{table_name}"'
    elif db_type in ("oracle",):
        database_ident = safe_ident(database, "database") if database else table
        query = f'SELECT * FROM "{database_ident}"."{table}"'
    elif db_type in ("postgres", "postgresql"):
        query = f'SELECT * FROM "{database}"."{table}"'
    else:
        query = f"SELECT * FROM `{database}`.`{table}`"

    if where:
        query += f" WHERE {where}"

    conn = connect_db(task)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = []
        if db_type in ("sqlite",):
            cols = [desc[0] for desc in (cursor.description or [])]
            for row in cursor.fetchall():
                rows.append({cols[i]: row[i] for i in range(len(cols))})
            columns = cols
        elif db_type in ("postgres", "postgresql",):
            rows = [dict(r) for r in cursor.fetchall()]
            columns = list(rows[0].keys()) if rows else [desc[0] for desc in (cursor.description or [])]
        elif db_type in ("mysql",):
            rows = list(cursor.fetchall())
            columns = list(rows[0].keys()) if rows else [desc[0] for desc in (cursor.description or [])]
        elif db_type in ("oracle",):
            cols = [desc[0] for desc in (cursor.description or [])]
            for row in cursor.fetchall():
                rows.append({cols[i]: row[i] for i in range(len(cols))})
            columns = cols
        elif db_type in ("sqlserver",):
            rows = [dict(zip([d[0] for d in (cursor.description or [])], r)) for r in cursor.fetchall()]
            columns = [d[0] for d in (cursor.description or [])]
        elif db_type in ("hive",):
            cols = [desc[0] for desc in (cursor.description or [])]
            for row in cursor.fetchall():
                rows.append({cols[i]: row[i] for i in range(len(cols))})
            columns = cols
        else:
            rows = list(cursor.fetchall())
            columns = list(rows[0].keys()) if rows else [desc[0] for desc in (cursor.description or [])]
        return columns, rows
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        conn.close()


def fetch_hdfs_data(task: Dict[str, Any]) -> Tuple[List[str], List[Dict[str, Any]]]:
    import hdfs as hdfs_mod
    path = str(task.get("path") or "/")
    filter_pattern = str(task.get("where") or "*")
    url = str(task.get("dsn") or f"http://{task.get('host','127.0.0.1')}:{task.get('port',50070)}")
    client = hdfs_mod.InsecureClient(url, user=str(task.get("user") or ""))

    files = []
    for f in client.list(path):
        if filter_pattern == "*" or filter_pattern == "1=1" or re.match(filter_pattern.replace("*", ".*"), f):
            files.append(f)

    rows = []
    for fname in files:
        fpath = f"{path.rstrip('/')}/{fname}"
        with client.read(fpath) as reader:
            content = reader.read().decode("utf-8")
        rows.append({"file": fname, "path": fpath, "content": content})

    return ["file", "path", "content"], rows


def fetch_hbase_data(task: Dict[str, Any]) -> Tuple[List[str], List[Dict[str, Any]]]:
    import happybase
    table_name = str(task.get("table") or "")
    prefix = str(task.get("row_key_prefix") or "")
    conn = happybase.Connection(
        host=str(task.get("host") or "127.0.0.1"),
        port=int(task.get("port") or 9090),
    )
    try:
        tbl = conn.table(table_name)
        rows = []
        for key, data in tbl.scan(row_prefix=prefix.encode() if prefix else None):
            row = {"row_key": key.decode("utf-8")}
            for cf, cols in data.items():
                for col, val in cols.items():
                    row[f"{cf.decode('utf-8')}:{col.decode('utf-8')}"] = val.decode("utf-8")
            rows.append(row)
        if rows:
            columns = list(rows[0].keys())
        else:
            columns = ["row_key"]
        return columns, rows
    finally:
        conn.close()


def fetch_data(task: Dict[str, Any]) -> Tuple[List[str], List[Dict[str, Any]]]:
    db_type = str(task.get("dbType") or "").strip().lower()
    if db_type == "hdfs":
        return fetch_hdfs_data(task)
    elif db_type == "hbase":
        return fetch_hbase_data(task)
    else:
        return fetch_sql_data(task)


def main() -> int:
    task: Dict[str, Any] = {}
    try:
        task = load_task()
        fmt = normalized_format(task)
        columns, rows = fetch_data(task)
        out_path = output_path(task, fmt)
        write_rows(out_path, fmt, columns, rows)
        owner_id = str(task.get("ownerId") or task.get("owner") or task.get("factoryId") or "unknown")
        _has_tag = bool(task.get("hasTag", False))
        _dataset_name = str(task.get("datasetName") or "")
        status = {
            "jobId": task.get("jobId"),
            "status": "SUCCEEDED",
            "filePath": str(out_path.resolve()),
            "rows": len(rows),
            "message": f"export completed via {task.get('dbType','unknown')} worker",
            "hasTag": _has_tag,
            "datasetName": _dataset_name,
            "ownerId": owner_id,
            "factoryId": owner_id,
            "username": owner_id,
            "finishedAt": now_iso(),
        }
        status_path = write_status(task, status, failed=False)
        print(json.dumps({**status, "statusPath": str(status_path)}, ensure_ascii=False, default=str))
        return 0
    except Exception as exc:
        owner_id = str(task.get("ownerId") or task.get("owner") or task.get("factoryId") or "") if isinstance(task, dict) else ""
        payload = {
            "jobId": task.get("jobId") if isinstance(task, dict) else "",
            "status": "FAILED",
            "filePath": "",
            "rows": 0,
            "message": str(exc),
            "errorTrace": traceback.format_exc(limit=5),
            "ownerId": owner_id,
            "factoryId": owner_id,
            "username": owner_id,
            "finishedAt": now_iso(),
        }
        try:
            status_path = write_status(task, payload, failed=True)
            payload["statusPath"] = str(status_path)
        except Exception:
            pass
        print(json.dumps(payload, ensure_ascii=False, default=str), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())