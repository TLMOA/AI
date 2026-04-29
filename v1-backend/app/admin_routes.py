from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
from pathlib import Path
import json
from datetime import datetime
import threading

from .silent_export_worker import process_once

router = APIRouter()


def _get_generated_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "data" / "generated"


def _get_silent_export_dir() -> Path:
    return Path("/home/yhz/iot/nifi_data") / "silent_exports"


def _config_path() -> Path:
    d = _get_generated_dir()
    d.mkdir(parents=True, exist_ok=True)
    return d / "silent_export_config.json"


def _requests_path() -> Path:
    d = _get_generated_dir()
    d.mkdir(parents=True, exist_ok=True)
    return d / "silent_export_requests.ndjson"


def _require_admin(request: Request):
    from .auth import _get_current_user_from_token

    cookie = request.cookies.get("access_token")
    user = _get_current_user_from_token(cookie)
    if not user or not user.get("is_admin"):
        raise HTTPException(status_code=403, detail="管理员权限不足")
    return user


class SilentExportConfig(BaseModel):
    enabled: Optional[bool] = False
    cron: Optional[str] = "daily"
    retention_days: Optional[int] = 7
    incremental_marker_column: Optional[str] = "updated_at"
    max_concurrent: Optional[int] = 1
    db: Optional[Dict[str, Any]] = None


@router.get("/api/internal/tenants/{tenant}/silent-export")
def get_silent_export(tenant: str):
    cfgp = _config_path()
    if not cfgp.exists():
        return {"code": 0, "message": "OK", "data": {"enabled": False, "cron": "0 2 * * *", "retention_days": 7, "incremental_marker_column": "updated_at"}, "traceId": ""}
    try:
        data = json.loads(cfgp.read_text(encoding="utf-8"))
        tenants: Dict[str, Any] = data.get("tenants", {})
        tcfg = tenants.get(tenant, {})
        if not tcfg:
            tcfg = {"enabled": False, "cron": "0 2 * * *", "retention_days": 7, "incremental_marker_column": "updated_at"}
        return {"code": 0, "message": "OK", "data": tcfg, "traceId": ""}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取配置失败: {e}")


@router.post("/api/internal/tenants/{tenant}/silent-export")
def set_silent_export(tenant: str, req: SilentExportConfig, request: Request = None):
    user = _require_admin(request)
    cfgp = _config_path()
    data = {"tenants": {}}
    if cfgp.exists():
        try:
            data = json.loads(cfgp.read_text(encoding="utf-8"))
        except Exception:
            data = {"tenants": {}}

    tenants = data.setdefault("tenants", {})
    old = tenants.get(tenant, {})
    tenants[tenant] = {
        "enabled": bool(req.enabled),
        "cron": req.cron or "daily",
        "retention_days": int(req.retention_days or 7),
        "incremental_marker_column": req.incremental_marker_column or "updated_at",
        "max_concurrent": int(req.max_concurrent or 1),
        "db": req.db or old.get("db") or {},
        "updated_at": datetime.utcnow().isoformat(),
        "updated_by": user.get("username") if isinstance(user, dict) else None,
    }
    cfgp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"code": 0, "message": "OK", "data": tenants[tenant], "traceId": ""}


@router.post("/api/internal/tenants/{tenant}/silent-export/trigger")
def trigger_silent_export(tenant: str, payload: Optional[Dict[str, Any]] = None, request: Request = None):
    user = _require_admin(request)
    req = payload or {}
    operator = req.get("operator") or (user.get("username") if isinstance(user, dict) else "web-admin")
    job = {
        "tenant": tenant,
        "operator": operator,
        "ts": datetime.utcnow().isoformat(),
        "status": "queued",
    }
    p = _requests_path()
    with p.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(job, ensure_ascii=False) + "\n")
    _get_silent_export_dir().mkdir(parents=True, exist_ok=True)
    try:
        t = threading.Thread(target=process_once, kwargs={"tenant_filter": tenant}, daemon=True)
        t.start()
    except Exception:
        pass
    return {"code": 0, "message": "enqueued", "data": job, "traceId": ""}
