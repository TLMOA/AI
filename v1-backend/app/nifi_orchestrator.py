import json
import os
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


NIFI_CONTAINER_NAME = os.getenv("NIFI_CONTAINER_NAME", "iot-nifi")
NIFI_IMAGE = os.getenv("NIFI_IMAGE", "apache/nifi:latest")
NIFI_HTTP_PORT = int(os.getenv("NIFI_HTTP_PORT", "8080"))
NIFI_API_BASE = os.getenv("NIFI_API_BASE", f"http://127.0.0.1:{NIFI_HTTP_PORT}/nifi-api")
NIFI_REAL_BASE_DIR = Path(os.getenv("NIFI_REAL_BASE_DIR", "/home/yhz/iot/real_nifi_data"))
NIFI_CONTAINER_DATA_DIR = os.getenv("NIFI_CONTAINER_DATA_DIR", "/opt/nifi/nifi-current/data/iot")
NIFI_READY_TIMEOUT_SECONDS = int(os.getenv("NIFI_READY_TIMEOUT_SECONDS", "120"))
NIFI_READY_INTERVAL_SECONDS = float(os.getenv("NIFI_READY_INTERVAL_SECONDS", "3"))
# 生产默认：只做后端对接，不在运行时自动创建容器或部署 Flow。
NIFI_AUTO_CREATE_CONTAINER = os.getenv("NIFI_AUTO_CREATE_CONTAINER", "false").lower() == "true"
NIFI_AUTO_START_CONTAINER = os.getenv("NIFI_AUTO_START_CONTAINER", "false").lower() == "true"
NIFI_AUTO_DEPLOY_FLOW = os.getenv("NIFI_AUTO_DEPLOY_FLOW", "false").lower() == "true"
NIFI_FLOW_MARKER = NIFI_REAL_BASE_DIR / "export_jobs" / ".iot_mysql_export_flow_v1.ready.json"
NIFI_WORKER_SOURCE = Path(__file__).resolve().parent.parent / "scripts" / "nifi_mysql_export_worker.py"
NIFI_WORKER_TARGET = NIFI_REAL_BASE_DIR / "bin" / "nifi_mysql_export_worker.py"
NIFI_FLOW_DOC_SOURCE = Path(__file__).resolve().parent.parent / "nifi_mysql_export_flow.md"
NIFI_FLOW_DOC_TARGET = NIFI_REAL_BASE_DIR / "export_jobs" / "nifi_mysql_export_flow.md"

_REQUIRED_DIRS = [
    "export_jobs/inbox",
    "export_jobs/done",
    "export_jobs/error",
    "output_csv",
    "output_json",
    "output_tsv",
    "inbox_csv",
    "inbox_json",
    "inbox_tsv",
    "tagged_output",
    "bin",
]


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def ensure_nifi_dirs() -> Dict[str, Any]:
    created = []
    for rel in _REQUIRED_DIRS:
        path = NIFI_REAL_BASE_DIR / rel
        existed = path.exists()
        path.mkdir(parents=True, exist_ok=True)
        if not existed:
            created.append(str(path))
    return {"baseDir": str(NIFI_REAL_BASE_DIR), "created": created}


def ensure_mysql_export_worker_assets() -> Dict[str, Any]:
    ensure_nifi_dirs()
    copied = []
    errors = []
    if NIFI_WORKER_SOURCE.exists():
        NIFI_WORKER_TARGET.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(NIFI_WORKER_SOURCE, NIFI_WORKER_TARGET)
        try:
            NIFI_WORKER_TARGET.chmod(0o755)
        except Exception:
            pass
        copied.append(str(NIFI_WORKER_TARGET))
    else:
        errors.append(f"worker source not found: {NIFI_WORKER_SOURCE}")

    if NIFI_FLOW_DOC_SOURCE.exists():
        NIFI_FLOW_DOC_TARGET.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(NIFI_FLOW_DOC_SOURCE, NIFI_FLOW_DOC_TARGET)
        copied.append(str(NIFI_FLOW_DOC_TARGET))
    return {"ok": not errors, "copied": copied, "errors": errors, "workerPath": str(NIFI_WORKER_TARGET), "flowDocPath": str(NIFI_FLOW_DOC_TARGET)}


def _run_docker(args: list[str], timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", *args],
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def docker_available() -> Dict[str, Any]:
    try:
        proc = _run_docker(["version", "--format", "{{json .Server.Version}}"], timeout=10)
        return {
            "available": proc.returncode == 0,
            "returnCode": proc.returncode,
            "stdout": proc.stdout.strip(),
            "stderr": proc.stderr.strip(),
        }
    except FileNotFoundError:
        return {"available": False, "returnCode": 127, "stdout": "", "stderr": "docker command not found"}
    except Exception as exc:
        return {"available": False, "returnCode": 1, "stdout": "", "stderr": str(exc)}


def inspect_container() -> Dict[str, Any]:
    try:
        proc = _run_docker(["inspect", NIFI_CONTAINER_NAME], timeout=10)
    except Exception as exc:
        return {"exists": False, "running": False, "error": str(exc)}
    if proc.returncode != 0:
        return {"exists": False, "running": False, "stderr": proc.stderr.strip()}
    try:
        data = json.loads(proc.stdout)[0]
        state = data.get("State") or {}
        return {
            "exists": True,
            "running": bool(state.get("Running")),
            "status": state.get("Status"),
            "image": data.get("Config", {}).get("Image"),
            "id": data.get("Id", "")[:12],
        }
    except Exception as exc:
        return {"exists": True, "running": False, "error": str(exc)}


def create_container() -> Dict[str, Any]:
    ensure_nifi_dirs()
    if not NIFI_AUTO_CREATE_CONTAINER:
        return {"created": False, "skipped": True, "reason": "NIFI_AUTO_CREATE_CONTAINER=false"}
    proc = _run_docker([
        "run",
        "-d",
        "--name",
        NIFI_CONTAINER_NAME,
        "-p",
        f"{NIFI_HTTP_PORT}:8080",
        "-e",
        "NIFI_WEB_HTTP_PORT=8080",
        "-v",
        f"{NIFI_REAL_BASE_DIR}:{NIFI_CONTAINER_DATA_DIR}",
        NIFI_IMAGE,
    ], timeout=120)
    return {
        "created": proc.returncode == 0,
        "returnCode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def start_container() -> Dict[str, Any]:
    if not NIFI_AUTO_START_CONTAINER:
        return {"started": False, "skipped": True, "reason": "NIFI_AUTO_START_CONTAINER=false"}
    proc = _run_docker(["start", NIFI_CONTAINER_NAME], timeout=60)
    return {
        "started": proc.returncode == 0,
        "returnCode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def ensure_nifi_container() -> Dict[str, Any]:
    result: Dict[str, Any] = {"dirs": ensure_nifi_dirs(), "docker": docker_available(), "actions": []}
    if not result["docker"].get("available"):
        result["ok"] = False
        result["error"] = result["docker"].get("stderr") or "docker is not available"
        return result

    state = inspect_container()
    result["containerBefore"] = state
    if not state.get("exists"):
        created = create_container()
        result["actions"].append({"action": "create_container", **created})
        if not created.get("created"):
            result["ok"] = False
            result["error"] = created.get("stderr") or created.get("reason") or "create nifi container failed"
            return result
    elif not state.get("running"):
        started = start_container()
        result["actions"].append({"action": "start_container", **started})
        if not started.get("started"):
            result["ok"] = False
            result["error"] = started.get("stderr") or started.get("reason") or "start nifi container failed"
            return result

    result["containerAfter"] = inspect_container()
    result["ok"] = bool(result["containerAfter"].get("running"))
    if not result["ok"]:
        result["error"] = "nifi container is not running"
    return result


def _decode_nifi_response(resp) -> Any:
    body = resp.read().decode("utf-8", errors="replace")
    try:
        return json.loads(body) if body else None
    except Exception:
        return body


def request_nifi_api(path: str, method: str = "GET", payload: Optional[Dict[str, Any]] = None, timeout: int = 10) -> Dict[str, Any]:
    url = f"{NIFI_API_BASE.rstrip('/')}/{path.lstrip('/')}"
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    try:
        req = Request(url, data=data, headers=headers, method=method.upper())
        with urlopen(req, timeout=timeout) as resp:
            decoded = _decode_nifi_response(resp)
            return {"ok": 200 <= resp.status < 300, "status": resp.status, "url": url, "data": decoded}
    except HTTPError as exc:
        try:
            error_body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            error_body = ""
        return {"ok": False, "status": exc.code, "url": url, "error": error_body or str(exc)}
    except URLError as exc:
        return {"ok": False, "status": 0, "url": url, "error": str(exc)}
    except Exception as exc:
        return {"ok": False, "status": 0, "url": url, "error": str(exc)}


def get_nifi_api(path: str, timeout: int = 5) -> Dict[str, Any]:
    return request_nifi_api(path, "GET", None, timeout)


def wait_nifi_ready(timeout_seconds: Optional[int] = None) -> Dict[str, Any]:
    timeout = timeout_seconds or NIFI_READY_TIMEOUT_SECONDS
    deadline = time.time() + timeout
    attempts = 0
    last: Dict[str, Any] = {}
    while time.time() < deadline:
        attempts += 1
        last = get_nifi_api("system-diagnostics")
        if last.get("ok"):
            return {"ok": True, "attempts": attempts, "api": last, "readyAt": _now_iso()}
        time.sleep(NIFI_READY_INTERVAL_SECONDS)
    return {"ok": False, "attempts": attempts, "api": last, "error": "nifi api readiness timeout"}


def get_nifi_status() -> Dict[str, Any]:
    api = get_nifi_api("system-diagnostics", timeout=3)
    flow = inspect_export_flow()
    return {
        "docker": docker_available(),
        "container": inspect_container(),
        "api": api,
        "flow": flow,
        "worker": {
            "source": str(NIFI_WORKER_SOURCE),
            "target": str(NIFI_WORKER_TARGET),
            "targetExists": NIFI_WORKER_TARGET.exists(),
        },
        "config": {
            "containerName": NIFI_CONTAINER_NAME,
            "image": NIFI_IMAGE,
            "httpPort": NIFI_HTTP_PORT,
            "apiBase": NIFI_API_BASE,
            "realBaseDir": str(NIFI_REAL_BASE_DIR),
            "containerDataDir": NIFI_CONTAINER_DATA_DIR,
        },
    }


def _root_process_group_id() -> Optional[str]:
    root = get_nifi_api("flow/process-groups/root")
    if not root.get("ok"):
        return None
    data = root.get("data") or {}
    return data.get("processGroupFlow", {}).get("id") or data.get("processGroupFlow", {}).get("breadcrumb", {}).get("id") or "root"


def _list_root_processors() -> Dict[str, Any]:
    return get_nifi_api("process-groups/root/processors")


def _list_root_connections() -> Dict[str, Any]:
    return get_nifi_api("process-groups/root/connections")


def _find_processor_by_name(name: str) -> Optional[Dict[str, Any]]:
    res = _list_root_processors()
    if not res.get("ok"):
        return None
    for proc in (res.get("data") or {}).get("processors") or []:
        if proc.get("component", {}).get("name") == name:
            return proc
    return None


def _create_processor(name: str, processor_type: str, x: float, y: float) -> Dict[str, Any]:
    payload = {
        "revision": {"version": 0},
        "component": {
            "type": processor_type,
            "name": name,
            "position": {"x": x, "y": y},
        },
    }
    return request_nifi_api("process-groups/root/processors", "POST", payload, timeout=20)


def _update_processor_config(proc: Dict[str, Any], properties: Dict[str, Any], auto_terminated_relationships: Optional[list[str]] = None) -> Dict[str, Any]:
    component = proc.get("component") or {}
    proc_id = component.get("id") or proc.get("id")
    revision = proc.get("revision") or {"version": 0}
    config = dict(component.get("config") or {})
    merged_properties = dict(config.get("properties") or {})
    merged_properties.update(properties)
    config["properties"] = merged_properties
    if auto_terminated_relationships is not None:
        config["autoTerminatedRelationships"] = auto_terminated_relationships
    payload = {
        "revision": revision,
        "component": {
            "id": proc_id,
            "name": component.get("name"),
            "config": config,
        },
    }
    return request_nifi_api(f"processors/{proc_id}", "PUT", payload, timeout=20)


def _stop_processor(proc: Dict[str, Any]) -> Dict[str, Any]:
    component = proc.get("component") or {}
    proc_id = component.get("id") or proc.get("id")
    revision = proc.get("revision") or {"version": 0}
    payload = {"revision": revision, "state": "STOPPED"}
    return request_nifi_api(f"processors/{proc_id}/run-status", "PUT", payload, timeout=20)


def _start_processor(proc: Dict[str, Any]) -> Dict[str, Any]:
    latest = request_nifi_api(f"processors/{(proc.get('component') or {}).get('id') or proc.get('id')}")
    entity = latest.get("data") if latest.get("ok") else proc
    component = entity.get("component") or {}
    proc_id = component.get("id") or entity.get("id")
    revision = entity.get("revision") or {"version": 0}
    payload = {"revision": revision, "state": "RUNNING"}
    return request_nifi_api(f"processors/{proc_id}/run-status", "PUT", payload, timeout=20)


def _ensure_getfile_processor() -> Dict[str, Any]:
    name = "iot_mysql_export_getfile_v1"
    proc = _find_processor_by_name(name)
    created = False
    if proc is None:
        res = _create_processor(name, "org.apache.nifi.processors.standard.GetFile", 320.0, 240.0)
        if not res.get("ok"):
            return {"ok": False, "error": res.get("error"), "api": res}
        proc = res.get("data")
        created = True
    _stop_processor(proc)
    updated = _update_processor_config(proc, {
        "Input Directory": f"{NIFI_CONTAINER_DATA_DIR}/export_jobs/inbox",
        "File Filter": ".*\\\\.json",
        "Keep Source File": "false",
        "Batch Size": "1",
    })
    if not updated.get("ok"):
        return {"ok": False, "created": created, "error": updated.get("error"), "api": updated}
    return {"ok": True, "created": created, "processor": updated.get("data")}


def _ensure_command_processor() -> Dict[str, Any]:
    name = "iot_mysql_export_command_v1"
    proc = _find_processor_by_name(name)
    created = False
    if proc is None:
        res = _create_processor(name, "org.apache.nifi.processors.standard.ExecuteStreamCommand", 760.0, 240.0)
        if not res.get("ok"):
            return {"ok": False, "error": res.get("error"), "api": res}
        proc = res.get("data")
        created = True
    _stop_processor(proc)
    updated = _update_processor_config(proc, {
        "Command Path": "python3",
        "Command Arguments": f"{NIFI_CONTAINER_DATA_DIR}/bin/nifi_mysql_export_worker.py",
    }, auto_terminated_relationships=["output stream", "nonzero status", "original"])
    if not updated.get("ok"):
        return {"ok": False, "created": created, "error": updated.get("error"), "api": updated}
    return {"ok": True, "created": created, "processor": updated.get("data")}


def _connection_exists(source_id: str, destination_id: str) -> bool:
    res = _list_root_connections()
    if not res.get("ok"):
        return False
    for conn in (res.get("data") or {}).get("connections") or []:
        comp = conn.get("component") or {}
        if comp.get("source", {}).get("id") == source_id and comp.get("destination", {}).get("id") == destination_id:
            return True
    return False


def _create_success_connection(source_proc: Dict[str, Any], dest_proc: Dict[str, Any]) -> Dict[str, Any]:
    source = source_proc.get("component") or {}
    dest = dest_proc.get("component") or {}
    source_id = source.get("id")
    dest_id = dest.get("id")
    if _connection_exists(source_id, dest_id):
        return {"ok": True, "created": False, "message": "connection already exists"}
    payload = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": source_id, "type": "PROCESSOR", "groupId": "root"},
            "destination": {"id": dest_id, "type": "PROCESSOR", "groupId": "root"},
            "selectedRelationships": ["success"],
            "flowFileExpiration": "0 sec",
            "backPressureDataSizeThreshold": "1 GB",
            "backPressureObjectThreshold": "10000",
        },
    }
    res = request_nifi_api("process-groups/root/connections", "POST", payload, timeout=20)
    return {"ok": res.get("ok"), "created": res.get("ok"), "api": res, "error": res.get("error")}


def _deploy_mysql_export_flow_via_api() -> Dict[str, Any]:
    getfile = _ensure_getfile_processor()
    if not getfile.get("ok"):
        return {"ok": False, "step": "getfile", **getfile}
    command = _ensure_command_processor()
    if not command.get("ok"):
        return {"ok": False, "step": "command", **command}
    connection = _create_success_connection(getfile["processor"], command["processor"])
    if not connection.get("ok"):
        return {"ok": False, "step": "connection", **connection}
    return {"ok": True, "getfile": getfile, "command": command, "connection": connection}


def inspect_export_flow() -> Dict[str, Any]:
    getfile = _find_processor_by_name("iot_mysql_export_getfile_v1")
    command = _find_processor_by_name("iot_mysql_export_command_v1")
    api_exists = bool(getfile and command)
    marker_payload = None
    if NIFI_FLOW_MARKER.exists():
        try:
            marker_payload = json.loads(NIFI_FLOW_MARKER.read_text(encoding="utf-8"))
        except Exception:
            marker_payload = None
    return {
        "exists": api_exists or NIFI_FLOW_MARKER.exists(),
        "apiExists": api_exists,
        "markerPath": str(NIFI_FLOW_MARKER),
        "markerExists": NIFI_FLOW_MARKER.exists(),
        "marker": marker_payload,
        "processors": {
            "getfile": getfile,
            "command": command,
        },
    }


def ensure_export_flow() -> Dict[str, Any]:
    assets = ensure_mysql_export_worker_assets()
    if not assets.get("ok"):
        return {"ok": False, "deployed": False, "assets": assets, "error": "; ".join(assets.get("errors") or [])}
    flow = inspect_export_flow()
    if flow.get("exists"):
        return {"ok": True, "deployed": False, "flow": flow, "assets": assets, "message": "export flow marker exists; reuse existing flow"}
    if not NIFI_AUTO_DEPLOY_FLOW:
        return {
            "ok": True,
            "deployed": False,
            "flow": flow,
            "assets": assets,
            "message": "worker assets prepared; create NiFi GetFile + ExecuteStreamCommand flow using the copied guide",
            "warning": "NIFI_AUTO_DEPLOY_FLOW=false",
        }
    deployed = _deploy_mysql_export_flow_via_api()
    if not deployed.get("ok"):
        return {"ok": False, "deployed": False, "assets": assets, "error": deployed.get("error") or f"deploy flow failed at {deployed.get('step')}", "deployment": deployed}
    NIFI_FLOW_MARKER.parent.mkdir(parents=True, exist_ok=True)
    marker = {
        "flowName": "iot_mysql_export_flow_v1",
        "processGroupName": "root",
        "status": "DEPLOYED",
        "message": "Created GetFile + ExecuteStreamCommand to call the prepared Python worker",
        "createdAt": _now_iso(),
        "inboxDir": f"{NIFI_CONTAINER_DATA_DIR}/export_jobs/inbox",
        "outputRoot": NIFI_CONTAINER_DATA_DIR,
        "workerCommand": "python3",
        "workerArguments": f"{NIFI_CONTAINER_DATA_DIR}/bin/nifi_mysql_export_worker.py",
        "flowGuide": f"{NIFI_CONTAINER_DATA_DIR}/export_jobs/nifi_mysql_export_flow.md",
        "deployment": deployed,
    }
    NIFI_FLOW_MARKER.write_text(json.dumps(marker, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return {"ok": True, "deployed": True, "flow": inspect_export_flow(), "assets": assets, "deployment": deployed, "message": "flow created via NiFi API"}


def start_export_flow() -> Dict[str, Any]:
    flow = inspect_export_flow()
    getfile = flow.get("processors", {}).get("getfile")
    command = flow.get("processors", {}).get("command")
    if not getfile or not command:
        return {"ok": False, "started": False, "message": "export flow processors not found", "flow": flow}
    results = []
    for proc in [command, getfile]:
        latest = _find_processor_by_name((proc.get("component") or {}).get("name") or "") or proc
        started = _start_processor(latest or proc)
        results.append(started)
        if not started.get("ok"):
            return {"ok": False, "started": False, "message": started.get("error") or "start processor failed", "results": results, "flow": inspect_export_flow()}
    return {"ok": True, "started": True, "message": "export flow processors started", "results": results, "flow": inspect_export_flow()}


def ensure_nifi_ready_for_export() -> Dict[str, Any]:
    result: Dict[str, Any] = {"startedAt": _now_iso(), "steps": []}
    container = ensure_nifi_container()
    result["steps"].append({"step": "ensure_nifi_container", **container})
    if not container.get("ok"):
        result["ok"] = False
        result["error"] = container.get("error") or "ensure nifi container failed"
        return result

    ready = wait_nifi_ready()
    result["steps"].append({"step": "wait_nifi_ready", **ready})
    if not ready.get("ok"):
        result["ok"] = False
        result["error"] = ready.get("error") or "nifi api is not ready"
        return result

    flow = ensure_export_flow()
    result["steps"].append({"step": "ensure_export_flow", **flow})
    if not flow.get("ok"):
        result["ok"] = False
        result["error"] = flow.get("error") or "ensure export flow failed"
        return result

    started = start_export_flow()
    result["steps"].append({"step": "start_export_flow", **started})
    if not started.get("ok"):
        result["ok"] = False
        result["error"] = started.get("error") or "start export flow failed"
        return result

    result["ok"] = True
    result["finishedAt"] = _now_iso()
    return result
