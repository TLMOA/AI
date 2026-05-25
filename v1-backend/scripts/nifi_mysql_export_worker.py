#!/usr/bin/env python3
"""NiFi-invoked MySQL export worker.

This script is designed to be executed by a NiFi ExecuteStreamCommand processor.
It accepts either:
- a task JSON file path as argv[1], or
- task JSON content from stdin.

It executes the SQL export, writes the output file to targetDir, and writes a
status JSON into export_jobs/done or export_jobs/error under targetRoot.
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

try:
    import pymysql
except Exception as exc:  # pragma: no cover - runtime dependency in NiFi container
    pymysql = None
    _PYMYSQL_IMPORT_ERROR = exc
else:
    _PYMYSQL_IMPORT_ERROR = None


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


def connect_mysql(task: Dict[str, Any]):
    if pymysql is None:
        raise RuntimeError(f"pymysql import failed: {_PYMYSQL_IMPORT_ERROR}")
    return pymysql.connect(
        host=str(task.get("host") or "127.0.0.1"),
        port=int(task.get("port") or 3306),
        user=str(task.get("user") or "root"),
        password=str(task.get("password") or ""),
        db=str(task.get("database") or ""),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def fetch_rows(task: Dict[str, Any]) -> Tuple[List[str], List[Dict[str, Any]]]:
    database = safe_ident(str(task.get("database") or ""), "database")
    table = safe_ident(str(task.get("table") or ""), "table")
    where = safe_where(str(task.get("where") or ""))
    query = f"SELECT * FROM `{database}`.`{table}`"
    if where:
        query += f" WHERE {where}"
    conn = connect_mysql(task)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = list(cur.fetchall())
            columns = list(rows[0].keys()) if rows else [desc[0] for desc in (cur.description or [])]
            return columns, rows
    finally:
        conn.close()


def normalized_format(task: Dict[str, Any]) -> str:
    fmt = str(task.get("format") or "CSV").strip().lower()
    if fmt not in {"csv", "json", "tsv"}:
        raise ValueError(f"unsupported format: {fmt}")
    return fmt


def output_path(task: Dict[str, Any], fmt: str) -> Path:
    target_dir = Path(str(task.get("targetDir") or "")).expanduser()
    if not str(target_dir):
        root = Path(str(task.get("targetRoot") or "/home/yhz/iot/real_nifi_data"))
        target_dir = root / f"output_{fmt}"
    target_dir.mkdir(parents=True, exist_ok=True)
    job_id = safe_ident(str(task.get("jobId") or f"export_{uuid.uuid4().hex[:8]}").replace("-", "_"), "jobId")
    table = safe_ident(str(task.get("table") or "table"), "table")
    filename = f"{table}_{job_id}_{now_ts()}.{fmt}"
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
    root = Path(str(task.get("targetRoot") or "/home/yhz/iot/real_nifi_data")) / "export_jobs"
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


def main() -> int:
    task: Dict[str, Any] = {}
    try:
        task = load_task()
        if str(task.get("dbType") or "mysql").lower() != "mysql":
            raise ValueError("only mysql export is supported by this worker")
        fmt = normalized_format(task)
        columns, rows = fetch_rows(task)
        out_path = output_path(task, fmt)
        write_rows(out_path, fmt, columns, rows)
        status = {
            "jobId": task.get("jobId"),
            "status": "SUCCEEDED",
            "filePath": str(out_path.resolve()),
            "rows": len(rows),
            "message": "export completed by nifi mysql worker",
            "finishedAt": now_iso(),
        }
        status_path = write_status(task, status, failed=False)
        print(json.dumps({**status, "statusPath": str(status_path)}, ensure_ascii=False, default=str))
        return 0
    except Exception as exc:
        payload = {
            "jobId": task.get("jobId") if isinstance(task, dict) else "",
            "status": "FAILED",
            "filePath": "",
            "rows": 0,
            "message": str(exc),
            "errorTrace": traceback.format_exc(limit=5),
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
