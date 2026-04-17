import csv
import json
import os
import shutil
import sys
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Header, HTTPException, Query, Request, UploadFile, File as UploadFileField, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field, field_validator


from .executors import MockExecutor, NiFiExecutor
from .db_connect import router as db_router
from .auth import router as auth_router
from .auth import _get_current_user_from_token
from . import db_models
from .export_worker import run_export_job
from .scheduler import start_scheduler, stop_scheduler, schedule_job, remove_scheduled
import pymysql
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo


    def _init_csv_field_size_limit() -> int:
        # Python csv 默认字段限制为 131072，较大单列内容会触发解析错误。
        configured = int(os.getenv("CSV_FIELD_SIZE_LIMIT", "20971520"))
        limit = max(configured, 131072)
        while limit >= 131072:
            try:
                return csv.field_size_limit(limit)
            except OverflowError:
                # 某些平台 C long 上限更小，逐步降低直到可接受范围。
                limit = limit // 10
        return csv.field_size_limit(min(sys.maxsize, 131072))


    CSV_FIELD_SIZE_LIMIT = _init_csv_field_size_limit()

BASE_DIR = Path(__file__).resolve().parent.parent
GENERATED_DIR = BASE_DIR / "data" / "generated"
GENERATED_DIR.mkdir(parents=True, exist_ok=True)
NIFI_FLOW_MAPPING_PATH = Path(__file__).resolve().parent / "nifi_flow_mapping.template.json"

if NIFI_FLOW_MAPPING_PATH.exists():
    NIFI_FLOW_MAPPING = json.loads(NIFI_FLOW_MAPPING_PATH.read_text(encoding="utf-8"))
else:
    NIFI_FLOW_MAPPING = {}

EXECUTOR_MODE = os.getenv("APP_EXECUTOR_MODE", "mock").lower()
EXECUTOR = NiFiExecutor(NIFI_FLOW_MAPPING) if EXECUTOR_MODE == "nifi" else MockExecutor()
NIFI_OUTPUT_DIR = Path(os.getenv("NIFI_OUTPUT_DIR", "/home/yhz/nifi-data/output_csv"))
# ensure main NiFi output dir exists
NIFI_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
# optional legacy/original NiFi inbox directories for CSV/JSON
INBOX_CSV_DIR = Path(os.getenv("INBOX_CSV_DIR", "/home/yhz/nifi-data/inbox_csv"))
INBOX_JSON_DIR = Path(os.getenv("INBOX_JSON_DIR", "/home/yhz/nifi-data/inbox_json"))
OUTPUT_TSV_DIR = Path(os.getenv("OUTPUT_TSV_DIR", "/home/yhz/nifi-data/output_tsv"))
INBOX_TSV_DIR = Path(os.getenv("INBOX_TSV_DIR", "/home/yhz/nifi-data/inbox_tsv"))

# ensure inbox dirs exist when writing demo outputs
INBOX_CSV_DIR.mkdir(parents=True, exist_ok=True)
INBOX_JSON_DIR.mkdir(parents=True, exist_ok=True)
CSV_TO_JSON_DIR = Path(os.getenv("CSV_TO_JSON_DIR", "/home/yhz/nifi-data/csv_to_json"))
JSON_TO_CSV_DIR = Path(os.getenv("JSON_TO_CSV_DIR", "/home/yhz/nifi-data/json_to_csv"))
TSV_TO_JSON_DIR = Path(os.getenv("TSV_TO_JSON_DIR", "/home/yhz/nifi-data/tsv_to_json"))
JSON_TO_TSV_DIR = Path(os.getenv("JSON_TO_TSV_DIR", "/home/yhz/nifi-data/json_to_tsv"))
CSV_TO_TSV_DIR = Path(os.getenv("CSV_TO_TSV_DIR", "/home/yhz/nifi-data/csv_to_tsv"))
TSV_TO_CSV_DIR = Path(os.getenv("TSV_TO_CSV_DIR", "/home/yhz/nifi-data/tsv_to_csv"))
TAGGED_OUTPUT_DIR = Path(os.getenv("TAGGED_OUTPUT_DIR", "/home/yhz/nifi-data/tagged_output"))
NIFI_BASE_DIR = Path(os.getenv("NIFI_BASE_DIR", "/home/yhz/nifi-data"))
NIFI_OUTPUT_JSON_DIR = Path(os.getenv("NIFI_OUTPUT_JSON_DIR", "/home/yhz/nifi-data/output_json"))
IN_DATA_BASE_DIR = Path(os.getenv("IN_DATA_BASE_DIR", "/home/yhz/in_data"))
DEFAULT_FACTORY_ID = os.getenv("DEFAULT_FACTORY_ID", "factory-001")
FACTORY_REPORT_FILE = GENERATED_DIR / "factory_reports.ndjson"
OUTPUT_TSV_DIR.mkdir(parents=True, exist_ok=True)
INBOX_TSV_DIR.mkdir(parents=True, exist_ok=True)
CSV_TO_JSON_DIR.mkdir(parents=True, exist_ok=True)
JSON_TO_CSV_DIR.mkdir(parents=True, exist_ok=True)
TSV_TO_JSON_DIR.mkdir(parents=True, exist_ok=True)
JSON_TO_TSV_DIR.mkdir(parents=True, exist_ok=True)
CSV_TO_TSV_DIR.mkdir(parents=True, exist_ok=True)
TSV_TO_CSV_DIR.mkdir(parents=True, exist_ok=True)
TAGGED_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
NIFI_BASE_DIR.mkdir(parents=True, exist_ok=True)
NIFI_OUTPUT_JSON_DIR.mkdir(parents=True, exist_ok=True)
IN_DATA_BASE_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="AI Module V1 Backend", version="0.1.0")
app.include_router(db_router)
app.include_router(auth_router)


def _require_admin(request: Request):
    cookie = request.cookies.get("access_token")
    user = _get_current_user_from_token(cookie)
    if not user or not user.get("is_admin"):
        raise HTTPException(status_code=403, detail="管理员权限不足")


def _operation_from_path(path: str) -> str:
    mapping = {
        "/api/v1/export/mysql": "export_mysql",
        "/api/v1/upload/inbox_csv": "upload_csv",
        "/api/v1/upload/inbox_json": "upload_json",
        "/api/v1/upload/inbox_tsv": "upload_tsv",
        "/api/v1/tags/manual-table": "manual_table_edit",
    }
    return mapping.get(path, "")


@app.middleware("http")
async def observability_middleware(request: Request, call_next):
    trace_id = make_trace_id(request.headers.get("x-trace-id"))
    request.state.trace_id = trace_id
    started = perf_counter()
    operation = _operation_from_path(request.url.path)
    if operation:
        _emit_structured_log(trace_id=trace_id, operation=operation, status="STARTED", phase="execute")
    try:
        response = await call_next(request)
        obs = getattr(request.state, "observation", {}) or {}
        if operation:
            duration_ms = int((perf_counter() - started) * 1000)
            _emit_structured_log(
                trace_id=trace_id,
                operation=obs.get("operation", operation),
                status=obs.get("status", "SUCCEEDED"),
                source_path=obs.get("sourcePath", ""),
                target_path=obs.get("targetPath", ""),
                duration_ms=duration_ms,
                error_code=obs.get("errorCode", ""),
                error_message=obs.get("errorMessage", ""),
                phase=obs.get("phase", "execute"),
            )
        return response
    except Exception as exc:
        if operation:
            duration_ms = int((perf_counter() - started) * 1000)
            _emit_structured_log(
                trace_id=trace_id,
                operation=operation,
                status="FAILED",
                duration_ms=duration_ms,
                error_code="5000000",
                error_message=str(exc),
                phase="execute",
            )
        origin = request.headers.get("origin") or "*"
        return JSONResponse(
            status_code=500,
            content=err(5000000, str(exc), trace_id),
            headers={
                "Access-Control-Allow-Origin": origin,
                "Access-Control-Allow-Credentials": "true",
                "Access-Control-Allow-Methods": "*",
                "Access-Control-Allow-Headers": "*",
                "Vary": "Origin",
            },
        )


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

jobs: Dict[str, Dict[str, Any]] = {}
files: Dict[str, Dict[str, Any]] = {}
schedules: Dict[str, Dict[str, Any]] = {}
tag_rules = [
    {
        "ruleId": "NIFI_RULE_ID_V5",
        "ruleName": "ID parity tagging",
        "ruleVersion": "v1",
        "enabled": True,
        "description": "Auto tag by odd/even id",
    }
]

# initialize DB after BASE_DIR exists
DB_PATH = BASE_DIR / "data" / "app.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
SessionLocal = db_models.init_db(DB_PATH)


class SourceConfig(BaseModel):
    sourceType: str = "MYSQL_TABLE"
    dbSourceId: Optional[str] = None
    tableName: Optional[str] = None
    where: Optional[str] = ""


class TargetConfig(BaseModel):
    format: str = "CSV"
    outputDir: Optional[str] = "/data/jobs"


class TagConfig(BaseModel):
    mode: str = "AUTO"
    ruleId: Optional[str] = "NIFI_RULE_ID_V5"


class JobCreateReq(BaseModel):
    jobType: str = Field(default="CONVERT")
    source: SourceConfig
    target: TargetConfig
    tagConfig: Optional[TagConfig] = None
    copyFormats: List[str] = Field(default_factory=list)
    cron: Optional[str] = ""
    runBy: Optional[str] = "user-001"
    remark: Optional[str] = ""


class ManualTagReq(BaseModel):
    fileId: str
    records: List[Dict[str, str]]
    operator: str

    @field_validator("records")
    @classmethod
    def validate_records(cls, value: List[Dict[str, str]]) -> List[Dict[str, str]]:
        if not value:
            raise ValueError("records must not be empty")
        for item in value:
            if not item.get("rowId") or not item.get("label"):
                raise ValueError("each record must contain rowId and label")
        return value


class ManualTableEditReq(BaseModel):
    fileId: str
    operator: str
    changes: List[Dict[str, str]]
    renameColumns: List[Dict[str, str]] = Field(default_factory=list)

    @field_validator("changes")
    @classmethod
    def validate_changes(cls, value: List[Dict[str, str]]) -> List[Dict[str, str]]:
        for item in value:
            if not item.get("rowId") or not item.get("column"):
                raise ValueError("each change must contain rowId and column")
        return value


class AutoTagReq(BaseModel):
    fileId: str
    ruleId: str = "NIFI_RULE_ID_V5"
    outputFormat: str = "CSV"
    operator: str


class ScheduleReq(BaseModel):
    name: str
    jobTemplate: Dict[str, Any]
    cron: str
    visibility: str = "USER_VISIBLE"


class SchedulePatchReq(BaseModel):
    status: str


def now_iso() -> str:
    # use China timezone
    tz = ZoneInfo("Asia/Shanghai")
    return datetime.now(tz).isoformat(timespec="seconds")


def now_ts() -> str:
    tz = ZoneInfo("Asia/Shanghai")
    return datetime.now(tz).strftime("%Y%m%d_%H%M%S")


def ok(data: Any, trace_id: str) -> Dict[str, Any]:
    return {"code": 0, "message": "OK", "data": data, "traceId": trace_id}


def err(code: int, message: str, trace_id: str) -> Dict[str, Any]:
    return {"code": code, "message": message, "data": None, "traceId": trace_id}


def make_trace_id(incoming: Optional[str]) -> str:
    return incoming or str(uuid.uuid4())


def _with_observation(
    data: Dict[str, Any],
    *,
    operation: str,
    source_path: Optional[str],
    target_path: Optional[str],
    status: str,
    duration_ms: int,
    error_code: str = "",
    error_message: str = "",
) -> Dict[str, Any]:
    enriched = dict(data)
    enriched.update(
        {
            "operation": operation,
            "sourcePath": source_path or "",
            "targetPath": target_path or "",
            "status": status,
            "errorCode": error_code,
            "errorMessage": error_message,
            "durationMs": duration_ms,
        }
    )
    return enriched


def _emit_structured_log(
    *,
    trace_id: str,
    operation: str,
    status: str,
    source_path: Optional[str] = None,
    target_path: Optional[str] = None,
    duration_ms: Optional[int] = None,
    error_code: str = "",
    error_message: str = "",
    phase: str = "execute",
) -> None:
    record = {
        "traceId": trace_id,
        "operation": operation,
        "status": status,
        "sourcePath": source_path or "",
        "targetPath": target_path or "",
        "durationMs": duration_ms,
        "errorCode": error_code,
        "errorMessage": error_message,
        "phase": phase,
        "ts": now_iso(),
    }
    print(json.dumps(record, ensure_ascii=False, default=str))


def _normalize_factory_id(value: Optional[str]) -> str:
    return (value or "").strip() or DEFAULT_FACTORY_ID


def _safe_relative_to(path: Path, root: Path) -> Optional[Path]:
    try:
        return path.resolve().relative_to(root.resolve())
    except Exception:
        return None


def _mirror_to_in_data(file_path: Path, factory_id: str = DEFAULT_FACTORY_ID) -> Optional[Path]:
    """Mirror a source file into the in_data archive tree.

    Current project convention keeps one factory per deployment, so we mirror
    NIFI_BASE_DIR content into /in_data/<factory_id>/... while preserving the
    original relative path under NIFI_BASE_DIR.
    """
    if not file_path.exists() or not file_path.is_file():
        return None

    in_data_root = IN_DATA_BASE_DIR.resolve()
    try:
        resolved = file_path.resolve()
    except Exception:
        return None

    if str(resolved).startswith(str(in_data_root)):
        return resolved

    rel = _safe_relative_to(resolved, NIFI_BASE_DIR)
    if rel is None:
        rel = Path(file_path.name)

    target = IN_DATA_BASE_DIR / factory_id / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(resolved, target)
    return target


def _append_factory_report(report: Dict[str, Any]) -> None:
    FACTORY_REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with FACTORY_REPORT_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(report, ensure_ascii=False, default=str))
        f.write("\n")


def _read_factory_reports() -> List[Dict[str, Any]]:
    if not FACTORY_REPORT_FILE.exists():
        return []
    out: List[Dict[str, Any]] = []
    with FACTORY_REPORT_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out


def _build_export_runtime_payload(row: db_models.ExportJobModel) -> Dict[str, Any]:
    payload = {}
    if isinstance(row.db_config, dict) and row.db_config.get("table"):
        payload = {"table": row.db_config.get("table")}
    return {
        "id": row.id,
        "job_name": row.job_name,
        "factory_id": _normalize_factory_id(row.factory_id),
        "owner_id": row.owner_id,
        "db_config": row.db_config,
        "file_format": row.file_format,
        "destination": row.destination,
        "payload": payload,
    }


def _persist_to_in_data(factory_id: str, job_id: int, src_path: str) -> str:
    """Copy fetched file to local in_data directory and return the persisted path."""
    if not src_path:
        return ""
    src = Path(src_path)
    if not src.exists() or not src.is_file():
        return src_path
    target_dir = IN_DATA_BASE_DIR / factory_id / str(job_id)
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / src.name
    shutil.copy2(src, target)
    return str(target)


def _collect_local_files(root: Path, source_type: str) -> List[Dict[str, Any]]:
    if not root.exists() or not root.is_dir():
        return []
    out: List[Dict[str, Any]] = []
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        try:
            stat = p.stat()
        except Exception:
            continue
        out.append({
            "name": p.name,
            "path": str(p),
            "size": stat.st_size,
            "format": (_guess_file_format(p) or "FILE").upper(),
            "sourceType": source_type,
            "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
            "mtime_ts": stat.st_mtime,
        })
    return out


def _infer_nifi_source_type(top_level_dir: str) -> str:
    name = (top_level_dir or "").strip().lower()
    if name in {"output_csv", "output_json", "output_tsv", "exports"}:
        return "manual_db_export"
    if name in {"inbox_csv", "inbox_json", "inbox_tsv"}:
        return "uploaded_raw"
    if name in {
        "csv_to_json",
        "json_to_csv",
        "tsv_to_json",
        "json_to_tsv",
        "csv_to_tsv",
        "tsv_to_csv",
        "tagged_output",
    }:
        return "uploaded_converted"
    return "manual_db_export"


def _build_filesystem_tree(root: Path, label: Optional[str] = None, max_depth: int = 6) -> Optional[Dict[str, Any]]:
    if not root.exists() or not root.is_dir():
        return None

    def walk(path: Path, depth: int) -> Dict[str, Any]:
        node = {
            "name": label if depth == 0 and label else path.name,
            "path": str(path),
            "type": "directory",
            "children": [],
        }
        if depth >= max_depth:
            return node
        try:
            entries = sorted(path.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
        except Exception:
            return node
        for entry in entries:
            if entry.name.startswith("."):
                continue
            if entry.is_dir():
                node["children"].append(walk(entry, depth + 1))
        return node

    return walk(root, 0)


def _copy_path_to_in_data(src: Path, factory_id: str, base_name: Optional[str] = None) -> List[str]:
    if not src.exists():
        raise HTTPException(status_code=404, detail="source path not found")
    copied: List[str] = []
    target_base = IN_DATA_BASE_DIR / factory_id
    target_base.mkdir(parents=True, exist_ok=True)

    if src.is_file():
        rel_name = base_name or src.name
        target = target_base / rel_name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, target)
        copied.append(str(target))
        return copied

    for file_path in src.rglob("*"):
        if not file_path.is_file():
            continue
        try:
            rel = file_path.relative_to(src)
        except Exception:
            rel = Path(file_path.name)
        # When syncing the whole NIFI_BASE_DIR root, keep its children directly
        # under the factory archive root instead of nesting an extra nifi-data/.
        if src.resolve() == NIFI_BASE_DIR.resolve() and not base_name:
            target = target_base / rel
        else:
            target = target_base / (base_name or src.name) / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file_path, target)
        copied.append(str(target))
    return copied


def create_demo_file(job_id: str, file_format: str = "CSV") -> Dict[str, Any]:
    ext = file_format.lower()
    file_id = f"file_{uuid.uuid4().hex[:10]}"
    file_name = f"{job_id}_output.{ext}"
    file_path = GENERATED_DIR / file_name

    if ext == "csv":
        with file_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "device_id", "value", "auto_tag", "ts"])
            for i in range(1, 11):
                writer.writerow(
                    [
                        i,
                        f"dev-{i}",
                        20 + i,
                        "EVEN_ID" if i % 2 == 0 else "ODD_ID",
                        now_iso(),
                    ]
                )
    elif ext == "json":
        rows = [
            {
                "id": i,
                "device_id": f"dev-{i}",
                "value": 20 + i,
                "auto_tag": "EVEN_ID" if i % 2 == 0 else "ODD_ID",
                "ts": now_iso(),
            }
            for i in range(1, 11)
        ]
        file_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        with file_path.open("w", encoding="utf-8") as f:
            for i in range(1, 11):
                tag = "EVEN_ID" if i % 2 == 0 else "ODD_ID"
                f.write(f"{i}\tdev-{i}\t{20+i}\t{tag}\t{now_iso()}\n")

    file_meta = {
        "fileId": file_id,
        "fileName": file_name,
        "fileFormat": file_format.upper(),
        "fileSize": file_path.stat().st_size,
        "storageType": "LOCAL",
        "storagePath": str(file_path),
        "createdAt": now_iso(),
        "jobId": job_id,
    }
    files[file_id] = file_meta
    # Optionally write demo outputs into NiFi inbox dirs so NiFi can pick them up
    try:
        if os.getenv("WRITE_DEMO_TO_INBOX", "false").lower() == "true":
            if ext == "csv":
                inbox_dest = INBOX_CSV_DIR / file_name
            elif ext == "json":
                inbox_dest = INBOX_JSON_DIR / file_name
            else:
                inbox_dest = INBOX_CSV_DIR / file_name
            # copy bytes to inbox
            inbox_dest.write_bytes(file_path.read_bytes())
            # update storagePath to reflect inbox when appropriate
            file_meta["storagePath"] = str(inbox_dest)
            file_meta["storageType"] = "NIFI_INBOX"
            file_meta["fileSize"] = inbox_dest.stat().st_size
    except Exception:
        pass
    return file_meta
    
@app.on_event("startup")
def _startup():
    # start background scheduler and recover enabled jobs from DB
    try:
        start_scheduler()
        session = SessionLocal()
        try:
            rows = session.query(db_models.ExportJobModel).all()
            for r in rows:
                if not r.enabled or not r.schedule:
                    continue
                payload = {}
                if isinstance(r.db_config, dict) and r.db_config.get("table"):
                    payload = {"table": r.db_config.get("table")}
                schedule_job(str(r.id), r.schedule, run_export_job, args=[{
                    "id": r.id,
                    "job_name": r.job_name,
                    "factory_id": _normalize_factory_id(r.factory_id),
                    "owner_id": r.owner_id,
                    "db_config": r.db_config,
                    "file_format": r.file_format,
                    "destination": r.destination,
                    "payload": payload,
                }])
        finally:
            session.close()
    except Exception:
        pass

@app.on_event("shutdown")
def _shutdown():
    try:
        stop_scheduler()
    except Exception:
        pass


@app.post("/api/export-jobs/trigger")
@app.post("/api/v1/export-jobs/trigger")
def api_trigger_export(job: Dict[str, Any], request: Request):
    """Trigger an export job (simple synchronous) - expects job dict in body."""
    trace_id = make_trace_id(request.headers.get("x-trace-id"))
    res = run_export_job(job)
    if res.get("status") == "error":
        raise HTTPException(status_code=500, detail=res.get("error"))
    return ok(res, trace_id)
    
@app.post("/api/export-jobs")
@app.post("/api/v1/export-jobs")
def api_create_export(job: Dict[str, Any], request: Request):
    trace_id = make_trace_id(request.headers.get("x-trace-id"))
    session = SessionLocal()
    try:
        ej = db_models.ExportJobModel(
            job_name=job.get("job_name") or job.get("name") or "unnamed",
            factory_id=_normalize_factory_id(job.get("factory_id")),
            owner_id=job.get("owner_id"),
            schedule=job.get("schedule"),
            file_format=job.get("file_format", "csv"),
            destination=job.get("destination", {}),
            mode=job.get("mode", "visible"),
            enabled=1 if job.get("enabled") else 0,
            db_config=job.get("db_config", {}),
        )
        session.add(ej)
        session.commit()
        session.refresh(ej)
        # schedule if enabled
        if ej.enabled and ej.schedule:
            schedule_job(str(ej.id), ej.schedule, run_export_job, args=[{
                "id": ej.id,
                "job_name": ej.job_name,
                "factory_id": _normalize_factory_id(ej.factory_id),
                "owner_id": ej.owner_id,
                "db_config": ej.db_config,
                "file_format": ej.file_format,
                "destination": ej.destination,
                "payload": job.get("payload", {}),
            }])
        return ok({"id": ej.id}, trace_id)
    finally:
        session.close()

@app.get("/api/export-jobs")
@app.get("/api/v1/export-jobs")
def api_list_exports(request: Request, factory_id: Optional[str] = Query(default=None)):
    trace_id = make_trace_id(request.headers.get("x-trace-id"))
    session = SessionLocal()
    try:
        q = session.query(db_models.ExportJobModel)
        resolved_factory = _normalize_factory_id(factory_id) if factory_id is not None else None
        if resolved_factory:
            q = q.filter(
                (db_models.ExportJobModel.factory_id == resolved_factory)
                | (db_models.ExportJobModel.factory_id.is_(None))
            )
        rows = q.all()
        out = []
        for r in rows:
            out.append({
                "id": r.id,
                "job_name": r.job_name,
                "factory_id": _normalize_factory_id(r.factory_id),
                "owner_id": r.owner_id,
                "schedule": r.schedule,
                "file_format": r.file_format,
                "mode": r.mode,
                "enabled": bool(r.enabled),
                "created_at": r.created_at.isoformat() if r.created_at else None,
            })
        return ok(out, trace_id)
    finally:
        session.close()

@app.get("/api/export-jobs/{job_id}")
@app.get("/api/v1/export-jobs/{job_id}")
def api_get_export(job_id: int, request: Request):
    trace_id = make_trace_id(request.headers.get("x-trace-id"))
    session = SessionLocal()
    try:
        r = session.query(db_models.ExportJobModel).filter(db_models.ExportJobModel.id == job_id).first()
        if not r:
            raise HTTPException(status_code=404, detail="not found")
        return ok({
            "id": r.id,
            "job_name": r.job_name,
            "factory_id": _normalize_factory_id(r.factory_id),
            "owner_id": r.owner_id,
            "schedule": r.schedule,
            "file_format": r.file_format,
            "destination": r.destination,
            "mode": r.mode,
            "enabled": bool(r.enabled),
            "db_config": r.db_config,
            "last_run": r.last_run.isoformat() if r.last_run else None,
        }, trace_id)
    finally:
        session.close()

@app.patch("/api/export-jobs/{job_id}")
@app.patch("/api/v1/export-jobs/{job_id}")
def api_patch_export(job_id: int, patch: Dict[str, Any], request: Request):
    trace_id = make_trace_id(request.headers.get("x-trace-id"))
    session = SessionLocal()
    try:
        r = session.query(db_models.ExportJobModel).filter(db_models.ExportJobModel.id == job_id).first()
        if not r:
            raise HTTPException(status_code=404, detail="not found")
        # simple fields
        for k in ("job_name", "schedule", "file_format", "mode", "destination", "db_config", "factory_id"):
            if k in patch:
                if k == "factory_id":
                    setattr(r, k, _normalize_factory_id(patch[k]))
                else:
                    setattr(r, k, patch[k])
        if "enabled" in patch:
            r.enabled = 1 if patch["enabled"] else 0
        session.commit()
        # manage scheduling
        if r.enabled and r.schedule:
            payload = patch.get("payload", {})
            if (not payload) and isinstance(r.db_config, dict) and r.db_config.get("table"):
                payload = {"table": r.db_config.get("table")}
            schedule_job(str(r.id), r.schedule, run_export_job, args=[{
                "id": r.id,
                "job_name": r.job_name,
                "factory_id": _normalize_factory_id(r.factory_id),
                "owner_id": r.owner_id,
                "db_config": r.db_config,
                "file_format": r.file_format,
                "destination": r.destination,
                "payload": payload,
            }])
        else:
            remove_scheduled(str(r.id))
        return ok({"id": r.id}, trace_id)
    finally:
        session.close()

@app.delete("/api/export-jobs/{job_id}")
@app.delete("/api/v1/export-jobs/{job_id}")
def api_delete_export(job_id: int, request: Request):
    trace_id = make_trace_id(request.headers.get("x-trace-id"))
    session = SessionLocal()
    try:
        r = session.query(db_models.ExportJobModel).filter(db_models.ExportJobModel.id == job_id).first()
        if not r:
            raise HTTPException(status_code=404, detail="not found")
        remove_scheduled(str(r.id))
        session.delete(r)
        session.commit()
        return ok({"deleted": True}, trace_id)
    finally:
        session.close()


@app.post("/api/internal/factory-reports")
@app.post("/api/v1/internal/factory-reports")
def api_ingest_factory_report(payload: Dict[str, Any], request: Request):
    _require_admin(request)
    trace_id = make_trace_id(request.headers.get("x-trace-id"))
    record = {
        "factory_id": _normalize_factory_id(payload.get("factory_id")),
        "job_id": str(payload.get("job_id") or ""),
        "batch_id": str(payload.get("batch_id") or ""),
        "status": str(payload.get("status") or "unknown"),
        "rows": int(payload.get("rows") or 0),
        "file_path": str(payload.get("file_path") or ""),
        "message": str(payload.get("message") or ""),
        "reported_at": payload.get("reported_at") or now_iso(),
        "received_at": now_iso(),
    }
    _append_factory_report(record)
    return ok({"accepted": True}, trace_id)


@app.get("/api/internal/factory-reports")
@app.get("/api/v1/internal/factory-reports")
def api_list_factory_reports(request: Request, factory_id: Optional[str] = Query(default=None), limit: int = Query(default=50, ge=1, le=500)):
    _require_admin(request)
    trace_id = make_trace_id(request.headers.get("x-trace-id"))
    items = _read_factory_reports()
    if factory_id is not None:
        resolved_factory = _normalize_factory_id(factory_id)
        items = [x for x in items if _normalize_factory_id(x.get("factory_id")) == resolved_factory]
    items = list(reversed(items))[:limit]
    return ok(items, trace_id)


@app.get("/api/internal/factory-jobs")
@app.get("/api/v1/internal/factory-jobs")
def api_list_factory_jobs(request: Request, factory_id: Optional[str] = Query(default=None)):
    _require_admin(request)
    trace_id = make_trace_id(request.headers.get("x-trace-id"))
    resolved_factory = _normalize_factory_id(factory_id)
    session = SessionLocal()
    try:
        rows = (
            session.query(db_models.ExportJobModel)
            .filter(
                (db_models.ExportJobModel.factory_id == resolved_factory)
                | (db_models.ExportJobModel.factory_id.is_(None))
            )
            .all()
        )
        out = []
        for r in rows:
            table_name = None
            if isinstance(r.db_config, dict):
                table_name = r.db_config.get("table")
            out.append({
                "id": r.id,
                "job_name": r.job_name,
                "factory_id": _normalize_factory_id(r.factory_id),
                "table": table_name,
                "schedule": r.schedule,
                "file_format": r.file_format,
                "enabled": bool(r.enabled),
                "last_run": r.last_run.isoformat() if r.last_run else None,
                "status": r.status,
            })
        return ok(out, trace_id)
    finally:
        session.close()


@app.post("/api/internal/factory-jobs/{job_id}/fetch")
@app.post("/api/v1/internal/factory-jobs/{job_id}/fetch")
def api_fetch_factory_job(job_id: int, request: Request, factory_id: Optional[str] = Query(default=None)):
    _require_admin(request)
    trace_id = make_trace_id(request.headers.get("x-trace-id"))
    resolved_factory = _normalize_factory_id(factory_id)
    session = SessionLocal()
    try:
        row = session.query(db_models.ExportJobModel).filter(db_models.ExportJobModel.id == job_id).first()
        if not row:
            raise HTTPException(status_code=404, detail="factory job not found")
        if _normalize_factory_id(row.factory_id) != resolved_factory:
            raise HTTPException(status_code=404, detail="factory job not found")

        # backfill legacy records so subsequent filtering is explicit and stable
        if not row.factory_id:
            row.factory_id = resolved_factory

        runtime_payload = _build_export_runtime_payload(row)
        result = run_export_job(runtime_payload)

        persisted_path = _persist_to_in_data(resolved_factory, row.id, str(result.get("path") or ""))
        if persisted_path:
            result["path"] = persisted_path

        row.last_run = datetime.utcnow()
        row.status = result.get("status") or "unknown"
        session.commit()

        _append_factory_report({
            "factory_id": resolved_factory,
            "job_id": str(row.id),
            "batch_id": f"manual_{now_ts()}",
            "status": str(result.get("status") or "unknown"),
            "rows": int(result.get("rows") or 0),
            "file_path": str(result.get("path") or ""),
            "message": str(result.get("error") or "manual_fetch"),
            "reported_at": now_iso(),
            "received_at": now_iso(),
        })

        if result.get("status") == "error":
            raise HTTPException(status_code=500, detail=result.get("error") or "fetch failed")

        return ok({
            "job_id": row.id,
            "factory_id": resolved_factory,
            **result,
        }, trace_id)
    finally:
        session.close()


@app.get("/api/internal/factory-assets")
@app.get("/api/v1/internal/factory-assets")
def api_list_factory_assets(
    request: Request,
    factory_id: Optional[str] = Query(default=None),
    path: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
):
    trace_id = make_trace_id(request.headers.get("x-trace-id"))
    resolved_factory = _normalize_factory_id(factory_id)

    def _infer_in_data_source_type(file_path: Path) -> str:
        try:
            rel_parts = file_path.relative_to(IN_DATA_BASE_DIR).parts
        except Exception:
            rel_parts = file_path.parts
        for part in rel_parts:
            inferred = _infer_nifi_source_type(part)
            if inferred != "manual_db_export" or part.lower() in {
                "output_csv",
                "output_json",
                "output_tsv",
                "exports",
                "inbox_csv",
                "inbox_json",
                "inbox_tsv",
                "csv_to_json",
                "json_to_csv",
                "tsv_to_json",
                "json_to_tsv",
                "csv_to_tsv",
                "tsv_to_csv",
                "tagged_output",
            }:
                return inferred
        return "scheduled_fetch"

    assets: List[Dict[str, Any]] = []
    if IN_DATA_BASE_DIR.exists() and IN_DATA_BASE_DIR.is_dir():
        for file_path in IN_DATA_BASE_DIR.rglob("*"):
            if not file_path.is_file():
                continue
            try:
                stat = file_path.stat()
            except Exception:
                continue
            existing = next((meta for meta in files.values() if str(meta.get("storagePath", "")) == str(file_path)), None)
            if existing is None:
                existing = register_existing_file(file_path, _guess_file_format(file_path) or "FILE")
            item = dict(existing)
            item.update({
                "path": item.get("storagePath", str(file_path)),
                "name": item.get("fileName", file_path.name),
                "size": item.get("fileSize", stat.st_size),
                "format": item.get("fileFormat", (_guess_file_format(file_path) or "FILE").upper()),
                "sourceType": _infer_in_data_source_type(file_path),
                "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
                "mtime_ts": stat.st_mtime,
            })
            assets.append(item)

    if path:
        normalized_path = str(Path(path).expanduser())
        assets = [x for x in assets if str(x.get("path", "")).startswith(normalized_path)]

    assets.sort(key=lambda x: x.get("mtime_ts") or 0, reverse=True)
    assets = assets[:limit]
    for item in assets:
        item.pop("mtime_ts", None)

    return ok({
        "factory_id": resolved_factory,
        "total": len(assets),
        "items": assets,
    }, trace_id)


@app.get("/api/internal/factory-tree")
@app.get("/api/v1/internal/factory-tree")
def api_get_factory_tree(request: Request, factory_id: Optional[str] = Query(default=None)):
    _require_admin(request)
    trace_id = make_trace_id(request.headers.get("x-trace-id"))
    resolved_factory = _normalize_factory_id(factory_id)
    roots = []
    tree = _build_filesystem_tree(IN_DATA_BASE_DIR, label="in_data", max_depth=6)
    if tree:
        roots.append(tree)
    return ok({"factory_id": resolved_factory, "roots": roots}, trace_id)


@app.post("/api/internal/factory-tree/fetch")
@app.post("/api/v1/internal/factory-tree/fetch")
def api_fetch_factory_tree(request: Request, payload: Optional[Dict[str, Any]] = Body(default=None)):
    _require_admin(request)
    trace_id = make_trace_id(request.headers.get("x-trace-id"))
    body = payload or {}
    resolved_factory = _normalize_factory_id(body.get("factory_id"))
    source_raw = str(body.get("path") or "").strip()
    source_path = Path(source_raw).expanduser() if source_raw else NIFI_BASE_DIR
    if not source_path.exists():
        raise HTTPException(status_code=404, detail="source path not found")

    allowed_roots = [NIFI_BASE_DIR.resolve(), IN_DATA_BASE_DIR.resolve()]
    try:
        resolved_source = source_path.resolve()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid path")

    if not any(str(resolved_source).startswith(str(root)) for root in allowed_roots):
        raise HTTPException(status_code=403, detail="path not allowed")

    target_root = (IN_DATA_BASE_DIR / resolved_factory).resolve()
    if str(resolved_source).startswith(str(target_root)):
        # Guard against nesting copies like in_data/factory-001 -> in_data/factory-001/factory-001/...
        return ok({
            "factory_id": resolved_factory,
            "copied": [],
            "count": 0,
            "skipped": True,
            "message": "source path is already target in_data factory root",
        }, trace_id)

    copied = _copy_path_to_in_data(resolved_source, resolved_factory, base_name=body.get("alias"))
    record = {
        "factory_id": resolved_factory,
        "job_id": str(payload.get("job_id") or ""),
        "batch_id": str(payload.get("batch_id") or f"tree_{now_ts()}"),
        "status": "ok",
        "rows": len(copied),
        "file_path": str(IN_DATA_BASE_DIR / resolved_factory),
        "message": f"copied {len(copied)} files",
        "reported_at": now_iso(),
        "received_at": now_iso(),
    }
    _append_factory_report(record)
    return ok({"factory_id": resolved_factory, "copied": copied, "count": len(copied)}, trace_id)


@app.post("/api/internal/factory-tree/refresh")
@app.post("/api/v1/internal/factory-tree/refresh")
def api_refresh_factory_tree(request: Request, payload: Optional[Dict[str, Any]] = Body(default=None)):
    _require_admin(request)
    trace_id = make_trace_id(request.headers.get("x-trace-id"))
    body = payload or {}
    resolved_factory = _normalize_factory_id(body.get("factory_id"))

    # Refresh means rescan the known NiFi archive roots and mirror missing/new files
    # into the current factory archive root, without creating an extra nifi_data layer.
    before_known = set(str(meta.get("storagePath", "")) for meta in files.values() if meta.get("storagePath"))
    _sync_nifi_files()
    after_known = set(str(meta.get("storagePath", "")) for meta in files.values() if meta.get("storagePath"))
    created = len(after_known - before_known)

    return ok({
        "factory_id": resolved_factory,
        "created": created,
        "message": "refresh completed",
    }, trace_id)


def _write_ndjson(path: Path, columns: List[str], rows: List[Dict[str, Any]], append: bool = False) -> None:
    mode = "a" if append and path.exists() else "w"
    with path.open(mode, encoding="utf-8") as f:
        for r in rows:
            # default=str handles datetime/Decimal and keeps export robust for NDJSON
            f.write(json.dumps(r, ensure_ascii=False, default=str))
            f.write("\n")


def _write_csv(path: Path, columns: List[str], rows: List[Dict[str, Any]], append: bool = False) -> None:
    mode = "a" if append and path.exists() else "w"
    with path.open(mode, encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        if not (append and path.exists()):
            writer.writerow(columns)
        for r in rows:
            writer.writerow([r.get(c, "") for c in columns])


def _write_tsv(path: Path, columns: List[str], rows: List[Dict[str, Any]], append: bool = False) -> None:
    mode = "a" if append and path.exists() else "w"
    with path.open(mode, encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        if not (append and path.exists()):
            writer.writerow(columns)
        for r in rows:
            writer.writerow([r.get(c, "") for c in columns])


def _write_csv_atomic(path: Path, columns: List[str], rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        _write_csv(tmp_path, columns, rows, append=False)
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def _write_ndjson_atomic(path: Path, columns: List[str], rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        _write_ndjson(tmp_path, columns, rows, append=False)
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def _write_tsv_atomic(path: Path, columns: List[str], rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        _write_tsv(tmp_path, columns, rows, append=False)
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def _read_file_records(file_path: Path, fmt: str) -> Tuple[List[str], List[Dict[str, Any]]]:
    fmt = fmt.upper()
    if fmt == "CSV":
        text = file_path.read_text(encoding="utf-8")
        reader = csv.DictReader(text.splitlines())
        rows = [dict(r) for r in reader]
        columns = list(rows[0].keys()) if rows else (reader.fieldnames or [])
        return columns, rows

    if fmt == "JSON":
        text = file_path.read_text(encoding="utf-8")
        rows: List[Dict[str, Any]] = []
        try:
            data = json.loads(text)
            if isinstance(data, list):
                rows = [x if isinstance(x, dict) else {"_value": x} for x in data]
            elif isinstance(data, dict):
                rows = [data]
            else:
                rows = [{"_value": data}]
        except json.JSONDecodeError:
            for ln in text.splitlines():
                ln = ln.strip()
                if not ln:
                    continue
                item = json.loads(ln)
                rows.append(item if isinstance(item, dict) else {"_value": item})
        columns = sorted({k for r in rows for k in r.keys()}) if rows else []
        return columns, rows

    if fmt == "TSV":
        lines = file_path.read_text(encoding="utf-8").splitlines()
        if not lines:
            return [], []
        header = lines[0].split("\t")
        rows = []
        for line in lines[1:]:
            values = line.split("\t")
            rows.append({header[i]: (values[i] if i < len(values) else "") for i in range(len(header))})
        return header, rows

    raise ValueError(f"unsupported file format for tagging: {fmt}")


def _parse_columns_arg(columns: Optional[str]) -> List[str]:
    if not columns:
        return []
    return [c.strip() for c in str(columns).split(",") if c.strip()]


def _infer_tag_source_name(file_name: str) -> str:
    stem = Path(file_name).stem
    for suffix in ["_export_latest", "_export", "_json", "_csv", "_output"]:
        if suffix in stem:
            stem = stem.split(suffix)[0]
            break
    return stem or "source"


def _compute_auto_tag(row: Dict[str, Any], index: int) -> str:
    value = row.get("id", row.get("rowId", index + 1))
    try:
        return "EVEN_ID" if int(value) % 2 == 0 else "ODD_ID"
    except Exception:
        return "ROW_EVEN" if (index + 1) % 2 == 0 else "ROW_ODD"


def _build_auto_tagged_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    tagged_rows: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        tagged = dict(row)
        tagged["auto_tag"] = _compute_auto_tag(row, idx)
        tagged_rows.append(tagged)
    return tagged_rows


def _build_manual_tagged_rows(rows: List[Dict[str, Any]], records: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    label_map = {str(item.get("rowId")): item.get("label", "") for item in records if item.get("rowId")}
    tagged_rows: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        tagged = dict(row)
        row_id = str(row.get("rowId", row.get("id", idx + 1)))
        tagged["label"] = label_map.get(row_id, "")
        tagged_rows.append(tagged)
    return tagged_rows


def _write_tagged_output(source_meta: Dict[str, Any], rows: List[Dict[str, Any]], tag_field: str, columns: List[str]) -> Dict[str, Any]:
    source_name = _infer_tag_source_name(source_meta.get("fileName", "source"))
    ts = now_ts()
    source_fmt = source_meta.get("fileFormat", "CSV").upper()
    out_columns = list(columns)
    if tag_field not in out_columns:
        out_columns.append(tag_field)

    if source_fmt == "JSON":
        out_path = TAGGED_OUTPUT_DIR / f"{source_name}_tagged_{ts}.json"
        _write_ndjson_atomic(out_path, out_columns, rows)
        return _register_and_return_meta(out_path, "json")

    if source_fmt == "TSV":
        out_path = TAGGED_OUTPUT_DIR / f"{source_name}_tagged_{ts}.tsv"
        _write_tsv_atomic(out_path, out_columns, rows)
        return _register_and_return_meta(out_path, "tsv")

    out_path = TAGGED_OUTPUT_DIR / f"{source_name}_tagged_{ts}.csv"
    _write_csv_atomic(out_path, out_columns, rows)
    return _register_and_return_meta(out_path, "csv")


def _write_edited_output(source_meta: Dict[str, Any], rows: List[Dict[str, Any]], columns: List[str]) -> Dict[str, Any]:
    source_name = _infer_tag_source_name(source_meta.get("fileName", "source"))
    ts = now_ts()
    source_fmt = source_meta.get("fileFormat", "CSV").upper()

    if source_fmt == "JSON":
        out_path = TAGGED_OUTPUT_DIR / f"{source_name}_tagged_{ts}.json"
        _write_ndjson_atomic(out_path, columns, rows)
        return _register_and_return_meta(out_path, "json")

    if source_fmt == "TSV":
        out_path = TAGGED_OUTPUT_DIR / f"{source_name}_tagged_{ts}.tsv"
        _write_tsv_atomic(out_path, columns, rows)
        return _register_and_return_meta(out_path, "tsv")

    out_path = TAGGED_OUTPUT_DIR / f"{source_name}_tagged_{ts}.csv"
    _write_csv_atomic(out_path, columns, rows)
    return _register_and_return_meta(out_path, "csv")


def _connect_mysql(host: str, port: int, user: str, password: str, db: str):
    return pymysql.connect(host=host, port=port, user=user, password=password, db=db, charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor)


def _export_table_to_rows(conn, table: str, where: str = "") -> Tuple[List[str], List[Dict[str, Any]]]:
    with conn.cursor() as cur:
        q = f"SELECT * FROM `{table}`"
        if where:
            q += f" WHERE {where}"
        cur.execute(q)
        rows = cur.fetchall()
        columns = list(rows[0].keys()) if rows else []
        return columns, rows


def _register_and_return_meta(path: Path, fmt: str, job_id: Optional[str] = None) -> Dict[str, Any]:
    meta = register_existing_file(path, fmt)
    if job_id and job_id in jobs:
        jobs[job_id].setdefault("outputs", []).append(meta["fileId"])
    return meta



async def run_job(job_id: str) -> None:
    job = jobs[job_id]
    await EXECUTOR.execute(job, create_demo_file, resolve_nifi_output_file, now_iso)
    for file_id in job.get("outputs", []):
        if file_id in files:
            files[file_id]["jobId"] = job_id


def register_existing_file(file_path: Path, file_format: str) -> Dict[str, Any]:
    resolved_path = file_path.resolve()
    existing_meta = next((meta for meta in files.values() if str(meta.get("storagePath", "")) == str(resolved_path)), None)
    if existing_meta is not None:
        existing_meta.update({
            "fileName": resolved_path.name,
            "fileFormat": file_format.upper(),
            "fileSize": resolved_path.stat().st_size,
            "storageType": "LOCAL",
            "storagePath": str(resolved_path),
        })
        try:
            _mirror_to_in_data(resolved_path, DEFAULT_FACTORY_ID)
        except Exception:
            pass
        return existing_meta

    file_id = f"file_{uuid.uuid4().hex[:10]}"
    file_meta = {
        "fileId": file_id,
        "fileName": resolved_path.name,
        "fileFormat": file_format.upper(),
        "fileSize": resolved_path.stat().st_size,
        "storageType": "LOCAL",
        "storagePath": str(resolved_path),
        "createdAt": now_iso(),
        "jobId": "",
    }
    files[file_id] = file_meta
    # persist to DB
    try:
        db = SessionLocal()
        fm = db_models.FileModel(
            file_id=file_meta["fileId"],
            file_name=file_meta["fileName"],
            file_format=file_meta["fileFormat"],
            file_size=file_meta["fileSize"],
            storage_type=file_meta["storageType"],
            storage_path=file_meta["storagePath"],
        )
        db.add(fm)
        db.commit()
    except Exception:
        pass

    # If the source file originates from the local NiFi archive area, mirror it
    # into in_data so the internal page always reflects the latest artifact set.
    try:
        _mirror_to_in_data(resolved_path, DEFAULT_FACTORY_ID)
    except Exception:
        pass

    return file_meta


def _guess_file_format(file_path: Path) -> Optional[str]:
    ext = file_path.suffix.lower()
    if ext == ".csv":
        return "CSV"
    if ext == ".json":
        return "JSON"
    if ext == ".tsv":
        return "TSV"
    if ext:
        return ext.lstrip(".").upper()
    return "FILE"


def _sync_nifi_files() -> None:
    # Merge files from disk into runtime registry so UI can show complete directory/file data.
    # Build set of actual files on disk under tracked roots
    disk_paths = set()
    for root in [NIFI_BASE_DIR, IN_DATA_BASE_DIR]:
        if not root.exists() or not root.is_dir():
            continue
        for p in root.rglob("*"):
            if not p.is_file():
                continue
            disk_paths.add(str(p.resolve()))

    # Remove registry entries that point to files no longer present on disk
    for fid, meta in list(files.items()):
        sp = str(meta.get("storagePath", ""))
        if not sp:
            continue
        # only consider NiFi / in_data managed paths for removal
        if sp.startswith(str(NIFI_BASE_DIR)) or sp.startswith(str(IN_DATA_BASE_DIR)):
            if sp not in disk_paths:
                try:
                    del files[fid]
                except Exception:
                    pass

    # Register any new files found on disk
    known_paths = set(str(meta.get("storagePath", "")) for meta in files.values() if meta.get("storagePath"))
    for p_str in disk_paths:
        if p_str in known_paths:
            continue
        p = Path(p_str)
        fmt = _guess_file_format(p)
        register_existing_file(p, fmt)


@app.on_event("startup")
def _startup_sync_nifi_files() -> None:
    _sync_nifi_files()


def resolve_nifi_output_file(expected_format: str) -> Optional[Dict[str, Any]]:
    """Search NiFi output and legacy inbox directories for the latest file of given format.

    Search order:
    1. `NIFI_OUTPUT_DIR`
    2. `INBOX_CSV_DIR` (if format is csv)
    3. `INBOX_JSON_DIR` (if format is json)
    4. any fallback directory among the above
    """
    ext = expected_format.lower()
    search_dirs = []
    if NIFI_OUTPUT_DIR.exists() and NIFI_OUTPUT_DIR.is_dir():
        search_dirs.append(NIFI_OUTPUT_DIR)
    if ext == "csv" and INBOX_CSV_DIR.exists() and INBOX_CSV_DIR.is_dir():
        search_dirs.append(INBOX_CSV_DIR)
    if ext == "json" and INBOX_JSON_DIR.exists() and INBOX_JSON_DIR.is_dir():
        search_dirs.append(INBOX_JSON_DIR)
    # fallback: include both inbox dirs if none qualified
    if not search_dirs:
        for d in [NIFI_OUTPUT_DIR, INBOX_CSV_DIR, INBOX_JSON_DIR]:
            if d.exists() and d.is_dir():
                search_dirs.append(d)

    candidates = []
    for d in search_dirs:
        try:
            candidates.extend(list(d.glob(f"*.{ext}")))
        except Exception:
            continue

    if not candidates:
        return None
    latest = sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)[0]
    return register_existing_file(latest, expected_format)


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/v1/system/executor")
def get_executor_info(x_trace_id: Optional[str] = Header(default=None)):
    trace_id = make_trace_id(x_trace_id)
    return ok({"mode": EXECUTOR_MODE, "flowMappingSize": len(NIFI_FLOW_MAPPING)}, trace_id)


@app.post("/api/v1/jobs")
async def create_job(req: JobCreateReq, x_trace_id: Optional[str] = Header(default=None)):
    trace_id = make_trace_id(x_trace_id)
    job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:4]}"
    job = {
        "jobId": job_id,
        "jobType": req.jobType,
        "status": "PENDING",
        "progress": 0,
        "source": req.source.model_dump(),
        "target": req.target.model_dump(),
        "errorCode": "",
        "errorMessage": "",
        "nifiFlowId": "nifi-mock-flow-v1",
        "createdAt": now_iso(),
        "startedAt": None,
        "finishedAt": None,
        "outputs": [],
    }
    jobs[job_id] = job
    import asyncio

    # persist job to DB
    try:
        db = SessionLocal()
        jm = db_models.JobModel(job_id=job_id, job_type=job["jobType"], status=job["status"], progress=job["progress"], payload=job)
        db.add(jm)
        db.commit()
    except Exception:
        pass
    asyncio.create_task(run_job(job_id))
    return ok(
        {
            "jobId": job_id,
            "status": job["status"],
            "createdAt": job["createdAt"],
        },
        trace_id,
    )


@app.get("/api/v1/jobs/{job_id}")
def get_job(job_id: str, x_trace_id: Optional[str] = Header(default=None)):
    trace_id = make_trace_id(x_trace_id)
    job = jobs.get(job_id)
    if not job:
        return err(1001404, "job not found", trace_id)
    return ok(job, trace_id)


@app.get("/api/v1/jobs")
def list_jobs(
    status: Optional[str] = Query(default=None),
    jobType: Optional[str] = Query(default=None),
    pageNo: int = Query(default=1),
    pageSize: int = Query(default=20),
    x_trace_id: Optional[str] = Header(default=None),
):
    trace_id = make_trace_id(x_trace_id)
    data = list(jobs.values())
    if status:
        data = [j for j in data if j["status"] == status]
    if jobType:
        data = [j for j in data if j["jobType"] == jobType]
    total = len(data)
    start = max((pageNo - 1) * pageSize, 0)
    rows = data[start : start + pageSize]
    return ok({"total": total, "pageNo": pageNo, "pageSize": pageSize, "rows": rows}, trace_id)


@app.post("/api/v1/jobs/{job_id}/cancel")
def cancel_job(job_id: str, x_trace_id: Optional[str] = Header(default=None)):
    trace_id = make_trace_id(x_trace_id)
    job = jobs.get(job_id)
    if not job:
        return err(1001404, "job not found", trace_id)
    if job["status"] in {"SUCCEEDED", "FAILED"}:
        return err(1001409, f"job already {job['status']}", trace_id)
    job["status"] = "CANCELED"
    job["finishedAt"] = now_iso()
    return ok({"jobId": job_id, "status": job["status"], "canceledAt": job["finishedAt"]}, trace_id)


@app.get("/api/v1/jobs/{job_id}/outputs")
def get_job_outputs(job_id: str, x_trace_id: Optional[str] = Header(default=None)):
    trace_id = make_trace_id(x_trace_id)
    job = jobs.get(job_id)
    if not job:
        return err(1001404, "job not found", trace_id)
    output_rows = [files[fid] for fid in job.get("outputs", []) if fid in files]
    return ok(output_rows, trace_id)


@app.get("/api/v1/files/{file_id}/download")
def download_file(file_id: str):
    file_meta = files.get(file_id)
    if not file_meta:
        raise HTTPException(status_code=404, detail="file not found")
    file_path = Path(file_meta["storagePath"])
    return FileResponse(file_path, filename=file_meta["fileName"], media_type="application/octet-stream")


@app.get("/api/v1/files/{file_id}/preview")
def preview_file(file_id: str, offset: int = 0, limit: int = 100, x_trace_id: Optional[str] = Header(default=None)):
    trace_id = make_trace_id(x_trace_id)
    if offset < 0 or limit <= 0:
        return err(1002401, "offset must be >= 0 and limit must be > 0", trace_id)
    file_meta = files.get(file_id)
    if not file_meta:
        return err(1002404, "file not found", trace_id)

    file_path = Path(file_meta["storagePath"])
    fmt = file_meta["fileFormat"].upper()

    if fmt == "JSON":
        text = file_path.read_text(encoding="utf-8")
        try:
            data = json.loads(text)
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                records = [data]
            else:
                return err(1002402, "invalid json content", trace_id)
        except json.JSONDecodeError:
            # fallback: NDJSON (one JSON object per line)
            records = []
            for ln in text.splitlines():
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    item = json.loads(ln)
                except json.JSONDecodeError:
                    return err(1002402, "invalid json content", trace_id)
                if isinstance(item, dict):
                    records.append(item)
                else:
                    records.append({"_value": item})

        columns: List[str] = []
        for item in records:
            for key in item.keys():
                if key not in columns:
                    columns.append(key)
        rows = [[str(item.get(col, "")) for col in columns] for item in records]
        sliced = rows[offset : offset + limit]
        return ok({"columns": columns, "rows": sliced, "total": len(rows)}, trace_id)

    if fmt == "TSV":
        lines = file_path.read_text(encoding="utf-8").splitlines()
        parsed = [line.split("\t") for line in lines if line != ""]
        if not parsed:
            return ok({"columns": [], "rows": [], "total": 0}, trace_id)
        columns = parsed[0]
        data_rows = parsed[1:]
        sliced = data_rows[offset : offset + limit]
        return ok({"columns": columns, "rows": sliced, "total": len(data_rows)}, trace_id)

    if fmt != "CSV":
        text = file_path.read_text(encoding="utf-8")
        lines = text.splitlines()
        return ok({"columns": ["content"], "rows": [[line] for line in lines[offset : offset + limit]], "total": len(lines)}, trace_id)

    rows: List[List[str]] = []
    with file_path.open("r", encoding="utf-8") as f:
        reader = list(csv.reader(f))
    columns = reader[0] if reader else []
    data_rows = reader[1:]
    sliced = data_rows[offset : offset + limit]
    rows.extend(sliced)
    return ok({"columns": columns, "rows": rows, "total": len(data_rows)}, trace_id)


@app.get("/api/v1/files")
def list_files(
    fileFormat: Optional[str] = Query(default=None),
    nifiOnly: bool = Query(default=True),
    pageNo: int = Query(default=1),
    pageSize: int = Query(default=20),
    x_trace_id: Optional[str] = Header(default=None),
):
    trace_id = make_trace_id(x_trace_id)
    _sync_nifi_files()
    data = list(files.values())
    if nifiOnly:
        data = [f for f in data if str(f.get("storagePath", "")).startswith("/home/yhz/nifi-data/")]
    if fileFormat:
        data = [f for f in data if f.get("fileFormat") == fileFormat.upper()]
    total = len(data)
    start = max((pageNo - 1) * pageSize, 0)
    rows = data[start : start + pageSize]
    return ok({"total": total, "pageNo": pageNo, "pageSize": pageSize, "rows": rows}, trace_id)


@app.post("/api/v1/tags/manual")
def manual_tag(req: ManualTagReq, x_trace_id: Optional[str] = Header(default=None)):
    trace_id = make_trace_id(x_trace_id)
    if req.fileId not in files:
        return err(1002404, "file not found", trace_id)
    if not req.records:
        return err(1002401, "records must not be empty", trace_id)

    source_meta = files[req.fileId]
    source_path = Path(source_meta["storagePath"])
    source_fmt = source_meta.get("fileFormat", "CSV")

    try:
        columns, rows = _read_file_records(source_path, source_fmt)
    except Exception as e:
        return err(1002402, f"read source file failed: {e}", trace_id)

    tagged_rows = _build_manual_tagged_rows(rows, req.records)
    # persist tags
    updated = 0
    try:
        db = SessionLocal()
        for r in req.records:
            tag = db_models.TagModel(file_id=req.fileId, row_id=r.get("rowId"), label=r.get("label"), operator=req.operator)
            db.add(tag)
            updated += 1
        db.commit()
    except Exception:
        pass

    out_meta = _write_tagged_output(source_meta, tagged_rows, "label", columns)
    return ok({"updated": updated, "operator": req.operator, "fileId": req.fileId, "file": out_meta}, trace_id)


@app.post("/api/v1/tags/manual-table")
def manual_table_edit(req: ManualTableEditReq, request: Request, x_trace_id: Optional[str] = Header(default=None)):
    trace_id = request.state.trace_id
    if req.fileId not in files:
        request.state.observation = {
            "operation": "manual_table_edit",
            "status": "FAILED",
            "sourcePath": "",
            "targetPath": "",
            "errorCode": "1002404",
            "errorMessage": "file not found",
            "phase": "execute",
        }
        return err(1002404, "file not found", trace_id)

    source_meta = files[req.fileId]
    source_path = Path(source_meta["storagePath"])
    source_fmt = source_meta.get("fileFormat", "CSV")

    try:
        columns, rows = _read_file_records(source_path, source_fmt)
    except Exception as e:
        request.state.observation = {
            "operation": "manual_table_edit",
            "status": "FAILED",
            "sourcePath": str(source_path),
            "targetPath": "",
            "errorCode": "1002402",
            "errorMessage": f"read source file failed: {e}",
            "phase": "execute",
        }
        return err(1002402, f"read source file failed: {e}", trace_id)

    # Rename columns first so subsequent cell edits can use new column names.
    rename_count = 0
    if req.renameColumns:
        for item in req.renameColumns:
            old = str(item.get("old", "")).strip()
            new = str(item.get("new", "")).strip()
            if not old or not new or old == new:
                continue
            if old in columns:
                columns = [new if c == old else c for c in columns]
                for r in rows:
                    if old in r:
                        r[new] = r.pop(old)
                rename_count += 1

    changed_cells = 0
    db_tag_rows: List[Dict[str, str]] = []
    for change in req.changes:
        try:
            row_index = int(str(change.get("rowId", "0"))) - 1
        except ValueError:
            continue
        if row_index < 0 or row_index >= len(rows):
            continue
        column = str(change.get("column", "")).strip()
        if not column:
            continue
        value = str(change.get("value", ""))
        rows[row_index][column] = value
        if column not in columns:
            columns.append(column)
        changed_cells += 1
        if column == "label" and value:
            db_tag_rows.append({"rowId": str(row_index + 1), "label": value})

    if changed_cells == 0:
        if rename_count == 0:
            return err(1002401, "no valid table changes", trace_id)

    try:
        if db_tag_rows:
            db = SessionLocal()
            for r in db_tag_rows:
                tag = db_models.TagModel(file_id=req.fileId, row_id=r.get("rowId"), label=r.get("label"), operator=req.operator)
                db.add(tag)
            db.commit()
    except Exception:
        pass

    out_meta = _write_edited_output(source_meta, rows, columns)
    request.state.observation = {
        "operation": "manual_table_edit",
        "status": "SUCCEEDED",
        "sourcePath": str(source_path),
        "targetPath": out_meta.get("storagePath", ""),
        "errorCode": "",
        "errorMessage": "",
        "phase": "execute",
    }
    payload = {
        "updatedCells": changed_cells,
        "renamedColumns": rename_count,
        "operator": req.operator,
        "fileId": req.fileId,
        "file": out_meta,
    }
    payload = _with_observation(
        payload,
        operation="manual_table_edit",
        source_path=str(source_path),
        target_path=out_meta.get("storagePath", ""),
        status="SUCCEEDED",
        duration_ms=0,
    )
    return ok(payload, trace_id)


@app.post("/api/v1/files/upload")
async def upload_file(file: UploadFile = UploadFileField(...)):
    # Compatibility endpoint: route CSV/JSON/TSV uploads to inbox dirs, others to GENERATED_DIR.
    filename = f"uploaded_{uuid.uuid4().hex[:8]}_{file.filename}"
    content = await file.read()
    suffix = Path(file.filename or "").suffix.lower()

    if suffix == ".csv":
        dest = INBOX_CSV_DIR / filename
        dest.write_bytes(content)
        meta = register_existing_file(dest, "csv")
        try:
            text = dest.read_text(encoding="utf-8")
            rows = []
            reader = csv.DictReader(text.splitlines())
            for r in reader:
                rows.append(r)
            if rows:
                out_name = f"uploaded_user_csv_{now_ts()}.json"
                out_path = CSV_TO_JSON_DIR / out_name
                _write_ndjson(out_path, list(rows[0].keys()), rows)
                _register_and_return_meta(out_path, "json")
                out_tsv_name = f"uploaded_user_csv_{now_ts()}.tsv"
                out_tsv_path = CSV_TO_TSV_DIR / out_tsv_name
                _write_tsv(out_tsv_path, list(rows[0].keys()), rows)
                _register_and_return_meta(out_tsv_path, "tsv")
        except Exception:
            pass
        return ok(meta, make_trace_id(None))

    if suffix in {".json", ".jsonl", ".ndjson"}:
        dest = INBOX_JSON_DIR / filename
        dest.write_bytes(content)
        meta = register_existing_file(dest, "json")
        try:
            text = dest.read_text(encoding="utf-8")
            lines = text.splitlines()
            objs = []
            if lines and (lines[0].strip().startswith("{") or lines[0].strip().startswith("[")):
                try:
                    data = json.loads(text)
                    if isinstance(data, list):
                        objs = data
                    elif isinstance(data, dict):
                        objs = [data]
                except Exception:
                    for ln in lines:
                        if ln.strip():
                            objs.append(json.loads(ln))
            if objs:
                cols = sorted({k for o in objs for k in o.keys()})
                out_name = f"uploaded_user_json_{now_ts()}.csv"
                out_path = JSON_TO_CSV_DIR / out_name
                _write_csv(out_path, cols, objs)
                _register_and_return_meta(out_path, "csv")
                out_tsv_name = f"uploaded_user_json_{now_ts()}.tsv"
                out_tsv_path = JSON_TO_TSV_DIR / out_tsv_name
                _write_tsv(out_tsv_path, cols, objs)
                _register_and_return_meta(out_tsv_path, "tsv")
        except Exception:
            pass
        return ok(meta, make_trace_id(None))

    if suffix == ".tsv":
        dest = INBOX_TSV_DIR / filename
        dest.write_bytes(content)
        meta = register_existing_file(dest, "tsv")
        try:
            lines = dest.read_text(encoding="utf-8").splitlines()
            if lines:
                header = lines[0].split("\t")
                rows: List[Dict[str, Any]] = []
                for ln in lines[1:]:
                    vals = ln.split("\t")
                    rows.append({header[i]: (vals[i] if i < len(vals) else "") for i in range(len(header))})
                if rows:
                    out_json_name = f"uploaded_user_tsv_{now_ts()}.json"
                    out_json_path = TSV_TO_JSON_DIR / out_json_name
                    _write_ndjson(out_json_path, header, rows)
                    _register_and_return_meta(out_json_path, "json")
                    out_csv_name = f"uploaded_user_tsv_{now_ts()}.csv"
                    out_csv_path = TSV_TO_CSV_DIR / out_csv_name
                    _write_csv(out_csv_path, header, rows)
                    _register_and_return_meta(out_csv_path, "csv")
        except Exception:
            pass
        return ok(meta, make_trace_id(None))

    # fallback for non-CSV/JSON files
    dest = GENERATED_DIR / filename
    dest.write_bytes(content)
    meta = register_existing_file(dest, filename.split(".")[-1] if "." in filename else "txt")
    return ok(meta, make_trace_id(None))


@app.post("/api/v1/upload")
async def upload_with_mode(request: Request,
                           file: UploadFile = UploadFileField(...),
                           conversionMode: Optional[str] = Query(default="local"),
                           username: Optional[str] = Query(default="user"),
                           convertType: Optional[str] = Query(default=None),
                           columns: Optional[str] = Query(default=None),
                           x_trace_id: Optional[str] = Header(default=None)):
    """Unified upload that supports conversionMode: local | nifi | both.
    For PoC: local mode runs a demo conversion (create_demo_file), nifi mode writes file to inbox and leaves job pending.
    """
    trace_id = make_trace_id(x_trace_id or request.state.trace_id)
    job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:4]}"
    job = {
        "jobId": job_id,
        "jobType": "UPLOAD",
        "status": "PENDING",
        "progress": 0,
        "source": {"fileName": file.filename},
        "target": {"format": (Path(file.filename).suffix.lstrip('.') or '').upper()},
        "errorCode": "",
        "errorMessage": "",
        "nifiFlowId": "",
        "createdAt": now_iso(),
        "startedAt": None,
        "finishedAt": None,
        "outputs": [],
    }
    jobs[job_id] = job

    # persist job to DB
    try:
        db = SessionLocal()
        jm = db_models.JobModel(job_id=job_id, job_type=job["jobType"], status=job["status"], progress=job["progress"], payload=job)
        db.add(jm)
        db.commit()
    except Exception:
        pass

    # save uploaded file to a temp location
    filename = f"{username}_{uuid.uuid4().hex[:8]}_{file.filename}"
    content = await file.read()
    suffix = Path(file.filename or "").suffix.lower()
    saved_path = GENERATED_DIR / filename
    saved_path.write_bytes(content)
    register_existing_file(saved_path, suffix.lstrip('.') or 'file')

    import asyncio

    async def _handle_local(job_id: str, fmt: str):
        # simulate conversion / processing for PoC
        job = jobs.get(job_id)
        if not job:
            return
        job["status"] = "PROCESSING"
        job["startedAt"] = now_iso()
        try:
            out_meta = create_demo_file(job_id, file_format=(fmt or "CSV"))
            job.setdefault("outputs", []).append(out_meta["fileId"])
            job["status"] = "SUCCEEDED"
            job["progress"] = 100
            job["finishedAt"] = now_iso()
        except Exception as e:
            job["status"] = "FAILED"
            job["errorMessage"] = str(e)
            job["finishedAt"] = now_iso()

    async def _handle_nifi(job_id: str, path: Path):
        # write to inbox based on suffix so NiFi can pick it up
        job = jobs.get(job_id)
        if not job:
            return
        job["status"] = "PENDING"
        job["startedAt"] = now_iso()
        try:
            ext = path.suffix.lower()
            if ext == ".csv":
                dest = INBOX_CSV_DIR / path.name
            elif ext == ".json":
                dest = INBOX_JSON_DIR / path.name
            elif ext == ".tsv":
                dest = INBOX_TSV_DIR / path.name
            else:
                dest = INBOX_CSV_DIR / path.name
            dest.write_bytes(path.read_bytes())
            # register inbox file
            meta = register_existing_file(dest, _guess_file_format(dest))
            job.setdefault("nifiFiles", []).append(meta["fileId"])
            # keep job pending until NiFi calls back
        except Exception as e:
            job["status"] = "FAILED"
            job["errorMessage"] = str(e)
            job["finishedAt"] = now_iso()

    mode = (conversionMode or "local").lower()
    fmt = suffix.lstrip('.') or 'csv'
    if mode in {"local", "both"}:
        asyncio.create_task(_handle_local(job_id, fmt))
    if mode in {"nifi", "both"}:
        asyncio.create_task(_handle_nifi(job_id, saved_path))

    return ok({"jobId": job_id, "status": jobs[job_id]["status"]}, trace_id)


@app.post("/api/v1/nifi-callback")
def nifi_callback(payload: Dict[str, Any], x_trace_id: Optional[str] = Header(default=None)):
    """NiFi calls this after processing a file to update job status.
    Expected payload: { jobId, status: 'SUCCEEDED'|'FAILED', filePath, rowCount, traceId }
    """
    trace_id = make_trace_id(x_trace_id)
    job_id = payload.get("jobId")
    if not job_id or job_id not in jobs:
        return err(1001404, "job not found", trace_id)
    job = jobs[job_id]
    status = payload.get("status", "SUCCEEDED")
    file_path = payload.get("filePath")
    row_count = payload.get("rowCount")
    error = payload.get("errorMessage")
    if file_path:
        p = Path(file_path)
        if p.exists():
            meta = register_existing_file(p, _guess_file_format(p))
            job.setdefault("outputs", []).append(meta["fileId"])
    job["status"] = "SUCCEEDED" if status == "SUCCEEDED" else "FAILED"
    job["finishedAt"] = now_iso()
    if error:
        job["errorMessage"] = error
    return ok({"jobId": job_id, "status": job["status"]}, trace_id)


@app.post("/api/v1/upload/inbox_csv")
async def upload_inbox_csv(
    request: Request,
    file: UploadFile = UploadFileField(...),
    username: Optional[str] = Query(default="user"),
    convertType: Optional[str] = Query(default="csv_to_json"),
    columns: Optional[str] = Query(default=None),
):
    trace_id = request.state.trace_id
    filename = f"{username}_{uuid.uuid4().hex[:8]}_{file.filename}"
    dest = INBOX_CSV_DIR / filename
    content = await file.read()
    dest.write_bytes(content)
    # register in file index
    meta = register_existing_file(dest, "csv")
    # single conversion route: csv_to_json or csv_to_tsv
    target_path = ""
    try:
        text = dest.read_text(encoding="utf-8")
        lines = text.splitlines()
        rows: List[Dict[str, Any]] = []
        cols = _parse_columns_arg(columns)

        if cols:
            reader = csv.reader(lines)
            for rec in reader:
                if not rec:
                    continue
                rows.append({cols[i]: (rec[i] if i < len(rec) else "") for i in range(len(cols))})
        else:
            reader = csv.DictReader(lines)
            for r in reader:
                rows.append(r)
            cols = list(rows[0].keys()) if rows else (reader.fieldnames or [])

        if not rows:
            request.state.observation = {
                "operation": "upload_csv",
                "status": "FAILED",
                "sourcePath": str(dest),
                "targetPath": "",
                "errorCode": "1002401",
                "errorMessage": "csv has no data rows; if file has no header please provide columns",
                "phase": "execute",
            }
            return err(1002401, "csv has no data rows; if file has no header please provide columns", trace_id)

        if rows:
            convert = (convertType or "csv_to_json").lower()
            if convert == "csv_to_tsv":
                out_name = f"uploaded_{username}_csv_{now_ts()}.tsv"
                out_path = CSV_TO_TSV_DIR / out_name
                _write_tsv(out_path, cols, rows)
                _register_and_return_meta(out_path, "tsv")
                target_path = str(out_path)
            else:
                out_name = f"uploaded_{username}_csv_{now_ts()}.json"
                out_path = CSV_TO_JSON_DIR / out_name
                _write_ndjson(out_path, cols, rows)
                _register_and_return_meta(out_path, "json")
                target_path = str(out_path)
    except Exception as ex:
        request.state.observation = {
            "operation": f"upload_{(convertType or 'csv_to_json').lower()}",
            "status": "FAILED",
            "sourcePath": str(dest),
            "targetPath": target_path,
            "errorCode": "1002500",
            "errorMessage": f"csv convert failed: {str(ex)}",
            "phase": "execute",
        }
        return err(1002500, f"csv convert failed: {str(ex)}", trace_id)
    request.state.observation = {
        "operation": f"upload_{(convertType or 'csv_to_json').lower()}",
        "status": "SUCCEEDED",
        "sourcePath": str(dest),
        "targetPath": target_path,
        "errorCode": "",
        "errorMessage": "",
        "phase": "execute",
    }
    payload = _with_observation(
        meta,
        operation=f"upload_{(convertType or 'csv_to_json').lower()}",
        source_path=str(dest),
        target_path=target_path,
        status="SUCCEEDED",
        duration_ms=0,
    )
    return ok(payload, trace_id)


@app.post("/api/v1/upload/inbox_json")
async def upload_inbox_json(
    request: Request,
    file: UploadFile = UploadFileField(...),
    username: Optional[str] = Query(default="user"),
    convertType: Optional[str] = Query(default="json_to_csv"),
):
    trace_id = request.state.trace_id
    filename = f"{username}_{uuid.uuid4().hex[:8]}_{file.filename}"
    dest = INBOX_JSON_DIR / filename
    content = await file.read()
    dest.write_bytes(content)
    meta = register_existing_file(dest, "json")
    # single conversion route: json_to_csv or json_to_tsv
    target_path = ""
    try:
        text = dest.read_text(encoding="utf-8")
        lines = text.splitlines()
        objs = []
        # support NDJSON or JSON array
        if lines and (lines[0].strip().startswith("{") or lines[0].strip().startswith("[")):
            try:
                # try parse as array
                data = json.loads(text)
                if isinstance(data, list):
                    objs = data
            except Exception:
                # fallback to ndjson
                for ln in lines:
                    if ln.strip():
                        objs.append(json.loads(ln))
        if objs:
            cols = sorted({k for o in objs for k in o.keys()})
            convert = (convertType or "json_to_csv").lower()
            if convert == "json_to_tsv":
                out_name = f"uploaded_{username}_json_{now_ts()}.tsv"
                out_path = JSON_TO_TSV_DIR / out_name
                _write_tsv(out_path, cols, objs)
                _register_and_return_meta(out_path, "tsv")
                target_path = str(out_path)
            else:
                out_name = f"uploaded_{username}_json_{now_ts()}.csv"
                out_path = JSON_TO_CSV_DIR / out_name
                _write_csv(out_path, cols, objs)
                _register_and_return_meta(out_path, "csv")
                target_path = str(out_path)
    except Exception as ex:
        request.state.observation = {
            "operation": f"upload_{(convertType or 'json_to_csv').lower()}",
            "status": "FAILED",
            "sourcePath": str(dest),
            "targetPath": target_path,
            "errorCode": "1002500",
            "errorMessage": f"json convert failed: {str(ex)}",
            "phase": "execute",
        }
        return err(1002500, f"json convert failed: {str(ex)}", trace_id)
    request.state.observation = {
        "operation": f"upload_{(convertType or 'json_to_csv').lower()}",
        "status": "SUCCEEDED",
        "sourcePath": str(dest),
        "targetPath": target_path,
        "errorCode": "",
        "errorMessage": "",
        "phase": "execute",
    }
    payload = _with_observation(
        meta,
        operation=f"upload_{(convertType or 'json_to_csv').lower()}",
        source_path=str(dest),
        target_path=target_path,
        status="SUCCEEDED",
        duration_ms=0,
    )
    return ok(payload, trace_id)


@app.post("/api/v1/upload/inbox_tsv")
async def upload_inbox_tsv(
    request: Request,
    file: UploadFile = UploadFileField(...),
    username: Optional[str] = Query(default="user"),
    convertType: Optional[str] = Query(default="tsv_to_json"),
    columns: Optional[str] = Query(default=None),
):
    trace_id = request.state.trace_id
    filename = f"{username}_{uuid.uuid4().hex[:8]}_{file.filename}"
    dest = INBOX_TSV_DIR / filename
    content = await file.read()
    dest.write_bytes(content)
    meta = register_existing_file(dest, "tsv")
    # single conversion route: tsv_to_json or tsv_to_csv
    target_path = ""
    try:
        lines = dest.read_text(encoding="utf-8").splitlines()
        header = _parse_columns_arg(columns)
        data_lines = lines
        if not header and lines:
            header = lines[0].split("\t")
            data_lines = lines[1:]
        if header:
            rows: List[Dict[str, Any]] = []
            for ln in data_lines:
                if not ln.strip():
                    continue
                vals = ln.split("\t")
                rows.append({header[i]: (vals[i] if i < len(vals) else "") for i in range(len(header))})
            if not rows:
                request.state.observation = {
                    "operation": "upload_tsv",
                    "status": "FAILED",
                    "sourcePath": str(dest),
                    "targetPath": "",
                    "errorCode": "1002401",
                    "errorMessage": "tsv has no data rows; if file has no header please provide columns",
                    "phase": "execute",
                }
                return err(1002401, "tsv has no data rows; if file has no header please provide columns", trace_id)
            convert = (convertType or "tsv_to_json").lower()
            if convert == "tsv_to_csv":
                out_name = f"uploaded_{username}_tsv_{now_ts()}.csv"
                out_path = TSV_TO_CSV_DIR / out_name
                _write_csv(out_path, header, rows)
                _register_and_return_meta(out_path, "csv")
                target_path = str(out_path)
            else:
                out_name = f"uploaded_{username}_tsv_{now_ts()}.json"
                out_path = TSV_TO_JSON_DIR / out_name
                _write_ndjson(out_path, header, rows)
                _register_and_return_meta(out_path, "json")
                target_path = str(out_path)
        else:
            return err(1002401, "tsv header is empty; please provide columns", trace_id)
    except Exception as ex:
        request.state.observation = {
            "operation": f"upload_{(convertType or 'tsv_to_json').lower()}",
            "status": "FAILED",
            "sourcePath": str(dest),
            "targetPath": target_path,
            "errorCode": "1002500",
            "errorMessage": f"tsv convert failed: {str(ex)}",
            "phase": "execute",
        }
        return err(1002500, f"tsv convert failed: {str(ex)}", trace_id)
    request.state.observation = {
        "operation": f"upload_{(convertType or 'tsv_to_json').lower()}",
        "status": "SUCCEEDED",
        "sourcePath": str(dest),
        "targetPath": target_path,
        "errorCode": "",
        "errorMessage": "",
        "phase": "execute",
    }
    payload = _with_observation(
        meta,
        operation=f"upload_{(convertType or 'tsv_to_json').lower()}",
        source_path=str(dest),
        target_path=target_path,
        status="SUCCEEDED",
        duration_ms=0,
    )
    return ok(payload, trace_id)


class MySQLExportReq(BaseModel):
    host: str = Field(default="127.0.0.1")
    port: int = Field(default=3306)
    user: str = Field(default="root")
    password: str = Field(default="root")
    db: str = Field(default="test")
    table: str = Field(default="sensor")
    where: Optional[str] = Field(default="")
    format: str = Field(default="CSV")
    append_to_latest: bool = Field(default=False)


@app.post("/api/v1/export/mysql")
def export_mysql(req: MySQLExportReq, request: Request, x_trace_id: Optional[str] = Header(default=None)):
    trace_id = request.state.trace_id
    try:
        conn = _connect_mysql(req.host, req.port, req.user, req.password, req.db)
    except Exception as e:
        request.state.observation = {
            "operation": "export_mysql",
            "status": "FAILED",
            "sourcePath": f"mysql://{req.host}:{req.port}/{req.db}.{req.table}",
            "targetPath": "",
            "errorCode": "1005001",
            "errorMessage": f"mysql connect failed: {e}",
            "phase": "execute",
        }
        return err(1005001, f"mysql connect failed: {e}", trace_id)
    try:
        cols, rows = _export_table_to_rows(conn, req.table, req.where or "")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    ts = now_ts()
    fmt = req.format.lower()
    base_name = f"{req.table}_export_{ts}"
    if fmt == "csv":
        out_path = NIFI_OUTPUT_DIR / f"{base_name}.csv"
        _write_csv(out_path, cols, rows, append=False)
        # update latest
        latest_path = NIFI_OUTPUT_DIR / f"{req.table}_export_latest.csv"
        if req.append_to_latest and latest_path.exists():
            _write_csv(latest_path, cols, rows, append=True)
        else:
            # copy/overwrite latest
            out_path.replace(latest_path)
        # out_path may be moved to latest_path by replace(); always register the actual existing file
        reg_path = out_path if out_path.exists() else latest_path
        meta = _register_and_return_meta(reg_path, "csv")
    elif fmt == "tsv":
        out_path = OUTPUT_TSV_DIR / f"{base_name}.tsv"
        _write_tsv(out_path, cols, rows, append=False)
        latest_path = OUTPUT_TSV_DIR / f"{req.table}_export_latest.tsv"
        if req.append_to_latest and latest_path.exists():
            _write_tsv(latest_path, cols, rows, append=True)
        else:
            out_path.replace(latest_path)
        reg_path = out_path if out_path.exists() else latest_path
        meta = _register_and_return_meta(reg_path, "tsv")
    else:
        out_path = Path(os.getenv("NIFI_OUTPUT_JSON_DIR", "/home/yhz/nifi-data/output_json")) / f"{base_name}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        _write_ndjson(out_path, cols, rows, append=False)
        latest_path = out_path.parent / f"{req.table}_export_latest.json"
        if req.append_to_latest and latest_path.exists():
            _write_ndjson(latest_path, cols, rows, append=True)
        else:
            out_path.replace(latest_path)
        reg_path = out_path if out_path.exists() else latest_path
        meta = _register_and_return_meta(reg_path, "json")

    payload = {
        "file": meta,
    }
    payload = _with_observation(
        payload,
        operation="export_mysql",
        source_path=f"mysql://{req.host}:{req.port}/{req.db}.{req.table}",
        target_path=meta.get("storagePath", ""),
        status="SUCCEEDED",
        duration_ms=0,
    )
    request.state.observation = {
        "operation": "export_mysql",
        "status": "SUCCEEDED",
        "sourcePath": f"mysql://{req.host}:{req.port}/{req.db}.{req.table}",
        "targetPath": meta.get("storagePath", ""),
        "errorCode": "",
        "errorMessage": "",
        "phase": "execute",
    }
    return ok(payload, trace_id)


@app.post("/api/v1/tags/auto")
async def auto_tag(req: AutoTagReq, x_trace_id: Optional[str] = Header(default=None)):
    trace_id = make_trace_id(x_trace_id)
    if req.fileId not in files:
        return err(1002404, "file not found", trace_id)
    source_meta = files[req.fileId]
    source_path = Path(source_meta["storagePath"])
    source_fmt = source_meta.get("fileFormat", "CSV")

    try:
        columns, rows = _read_file_records(source_path, source_fmt)
    except Exception as e:
        return err(1002402, f"read source file failed: {e}", trace_id)

    tagged_rows = _build_auto_tagged_rows(rows)
    out_meta = _write_tagged_output(source_meta, tagged_rows, "auto_tag", columns)
    out_fmt = req.outputFormat.upper()

    job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:4]}"
    job = {
        "jobId": job_id,
        "jobType": "TAG_AUTO",
        "status": "SUCCEEDED",
        "progress": 100,
        "source": {"sourceType": "FILE_UPLOAD", "fileId": req.fileId},
        "target": {"format": out_fmt, "outputDir": str(TAGGED_OUTPUT_DIR)},
        "errorCode": "",
        "errorMessage": "",
        "nifiFlowId": "local-tagging",
        "createdAt": now_iso(),
        "startedAt": now_iso(),
        "finishedAt": now_iso(),
        "outputs": [out_meta["fileId"]],
    }
    jobs[job_id] = job
    out_meta["jobId"] = job_id

    return ok({"jobId": job_id, "status": job["status"], "file": out_meta}, trace_id)


@app.get("/api/v1/tags/rules")
def get_tag_rules(x_trace_id: Optional[str] = Header(default=None)):
    return ok(tag_rules, make_trace_id(x_trace_id))


@app.post("/api/v1/schedules")
def create_schedule(req: ScheduleReq, x_trace_id: Optional[str] = Header(default=None)):
    trace_id = make_trace_id(x_trace_id)
    schedule_id = f"sch_{uuid.uuid4().hex[:8]}"
    schedule = {
        "scheduleId": schedule_id,
        "name": req.name,
        "cron": req.cron,
        "status": "ENABLED",
        "visibility": req.visibility,
        "jobTemplate": req.jobTemplate,
        "createdAt": now_iso(),
        "updatedAt": now_iso(),
    }
    schedules[schedule_id] = schedule
    return ok(schedule, trace_id)


@app.get("/api/v1/schedules")
def list_schedules(x_trace_id: Optional[str] = Header(default=None)):
    return ok(list(schedules.values()), make_trace_id(x_trace_id))


@app.patch("/api/v1/schedules/{schedule_id}")
def patch_schedule(schedule_id: str, req: SchedulePatchReq, x_trace_id: Optional[str] = Header(default=None)):
    trace_id = make_trace_id(x_trace_id)
    schedule = schedules.get(schedule_id)
    if not schedule:
        return err(1003404, "schedule not found", trace_id)
    schedule["status"] = req.status
    schedule["updatedAt"] = now_iso()
    return ok(schedule, trace_id)


@app.delete("/api/v1/schedules/{schedule_id}")
def delete_schedule(schedule_id: str, x_trace_id: Optional[str] = Header(default=None)):
    trace_id = make_trace_id(x_trace_id)
    if schedule_id not in schedules:
        return err(1003404, "schedule not found", trace_id)
    del schedules[schedule_id]
    return ok({"deleted": True, "scheduleId": schedule_id}, trace_id)
