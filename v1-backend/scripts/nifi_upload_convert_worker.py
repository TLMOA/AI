#!/usr/bin/env python3
"""NiFi-invoked upload convert worker.

Reads task JSON from stdin, parses source file, converts to target formats.

Task JSON structure:
{
  "jobId": "convert_abc123",
  "sourcePath": "/opt/nifi/nifi-current/data/iot/inbox_csv/raw_admin_20260528_data.csv",
  "sourceFormat": "CSV",
  "targetFormats": ["JSON", "TSV"],
  "fileName": "raw_admin_20260528_data.csv",
  "ownerId": "admin",
  "factoryId": "factory-001"
}
"""

import csv
import json
import os
import sys
import tempfile
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


DATA_ROOT = Path("/opt/nifi/nifi-current/data/iot")

OUTPUT_DIRS = {
    "CSV_JSON":   DATA_ROOT / "csv_to_json",
    "CSV_TSV":    DATA_ROOT / "csv_to_tsv",
    "JSON_CSV":   DATA_ROOT / "json_to_csv",
    "JSON_TSV":   DATA_ROOT / "json_to_tsv",
    "TSV_CSV":    DATA_ROOT / "tsv_to_csv",
    "TSV_JSON":   DATA_ROOT / "tsv_to_json",
}

STATUS_INBOX  = DATA_ROOT / "convert_jobs" / "inbox"
STATUS_DONE   = DATA_ROOT / "convert_jobs" / "done"
STATUS_ERROR  = DATA_ROOT / "convert_jobs" / "error"


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def safe_name(name: str) -> str:
    return Path(name).stem.replace(" ", "_").replace("/", "_")


def read_csv(path: Path) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [dict(r) for r in reader]


def read_tsv(path: Path) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return [dict(r) for r in reader]


def read_json(path: Path) -> List[Dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    text = text.strip()
    if text.startswith("["):
        data = json.loads(text)
        if isinstance(data, list):
            return data
    if text.startswith("{"):
        lines = text.splitlines()
        if len(lines) > 1 and all(l.strip().startswith("{") for l in lines if l.strip()):
            return [json.loads(l) for l in lines if l.strip()]
        obj = json.loads(text)
        return [obj]
    return []


def read_file(source_path: str, source_format: str) -> List[Dict[str, Any]]:
    fmt = source_format.upper()
    p = Path(source_path)
    if not p.exists():
        raise FileNotFoundError(f"source file not found: {source_path}")
    if fmt == "CSV":
        return read_csv(p)
    elif fmt == "JSON":
        return read_json(p)
    elif fmt == "TSV":
        return read_tsv(p)
    else:
        raise ValueError(f"unsupported source format: {source_format}")


def write_csv(path: Path, rows: List[Dict[str, Any]]):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    tmp = path.parent / f".tmp_{path.name}"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    os.rename(tmp, path)


def write_tsv(path: Path, rows: List[Dict[str, Any]]):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    tmp = path.parent / f".tmp_{path.name}"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()), delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    os.rename(tmp, path)


def write_json(path: Path, rows: List[Dict[str, Any]]):
    tmp = path.parent / f".tmp_{path.name}"
    with open(tmp, "w", encoding="utf-8") as f:
        for row in rows:
            json.dump(row, f, ensure_ascii=False)
            f.write("\n")
    os.rename(tmp, path)


def write_file(output_path: Path, target_format: str, rows: List[Dict[str, Any]]):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fmt = target_format.upper()
    if fmt == "CSV":
        write_csv(output_path, rows)
    elif fmt == "JSON":
        write_json(output_path, rows)
    elif fmt == "TSV":
        write_tsv(output_path, rows)
    else:
        raise ValueError(f"unsupported target format: {target_format}")


def output_key(source_fmt: str, target_fmt: str) -> str:
    return f"{source_fmt.upper()}_{target_fmt.upper()}"


def build_output_path(source_fmt: str, target_fmt: str, file_name: str, ts: str, tagged: bool = False) -> Path:
    key = output_key(source_fmt, target_fmt)
    if tagged:
        # 有标签：输出到容器内全局顶层 tagged_real_nifi_data（/home/yhz 已挂载到容器）
        # 宿主机 /home/yhz/tagged_real_nifi_data ↔ 容器 /home/yhz/tagged_real_nifi_data
        base_dir = Path("/home/yhz/tagged_real_nifi_data")
        sub = OUTPUT_DIRS.get(key, OUTPUT_DIRS["CSV_JSON"]).name  # csv_to_json 等
        out_dir = base_dir / sub
    else:
        base_dir = DATA_ROOT
        out_dir = base_dir / OUTPUT_DIRS.get(key, OUTPUT_DIRS["CSV_JSON"]).relative_to(DATA_ROOT)
    basename = Path(file_name).stem
    ext_map = {"CSV": "csv", "JSON": "json", "TSV": "tsv"}
    out_ext = ext_map.get(target_fmt.upper(), "json")
    out_name = f"xform_{safe_name(basename)}_{ts}_{source_fmt.lower()}2{target_fmt.lower()}.{out_ext}"
    return out_dir / out_name


def write_status(
    status_dir: Path,
    job_id: str,
    state: str,
    message: str = "",
    outputs: List[str] = None,
    username: str = "",
    owner_id: str = "",
    has_tag: bool = False,
    dataset_name: str = "",
):
    status_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "jobId": job_id,
        "status": state,
        "message": message,
        "outputFiles": outputs or [],
        "username": username,
        "ownerId": owner_id,
        "hasTag": has_tag,
        "datasetName": dataset_name,
        "finishedAt": now_iso(),
    }
    (status_dir / f"{job_id}.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def main():
    try:
        raw = sys.stdin.read().strip()
        if not raw:
            raise ValueError("no task JSON on stdin")
        task = json.loads(raw)
    except Exception as e:
        msg = f"failed to parse task JSON: {e}"
        write_status(STATUS_ERROR, "unknown", "FAILED", msg)
        sys.exit(1)

    job_id     = task.get("jobId", "unknown")
    source     = task.get("sourcePath", "")
    src_fmt    = task.get("sourceFormat", "CSV")
    targets    = task.get("targetFormats", [])
    file_name  = task.get("fileName", "upload")
    username   = task.get("username", "")
    owner_id   = task.get("ownerId", "")
    has_tag    = bool(task.get("hasTag", False))
    dataset_name = str(task.get("datasetName") or "")
    ts         = datetime.now().strftime("%Y%m%d_%H%M%S")

    try:
        rows = read_file(source, src_fmt)
    except Exception as e:
        write_status(
            STATUS_ERROR,
            job_id,
            "FAILED",
            f"read source failed: {e}\n{traceback.format_exc()}",
            username=username,
            owner_id=owner_id,
            has_tag=has_tag,
            dataset_name=dataset_name,
        )
        sys.exit(1)

    outputs = []
    for target_fmt in targets:
        try:
            out_path = build_output_path(src_fmt, target_fmt, file_name, ts, tagged=has_tag)
            write_file(out_path, target_fmt, rows)
            outputs.append(str(out_path))
        except Exception as e:
            write_status(
                STATUS_ERROR,
                job_id,
                "FAILED",
                f"convert to {target_fmt} failed: {e}\n{traceback.format_exc()}",
                username=username,
                owner_id=owner_id,
                has_tag=has_tag,
            )
            sys.exit(1)

    write_status(
        STATUS_DONE,
        job_id,
        "SUCCEEDED",
        f"converted {src_fmt} to {', '.join(targets)}",
        outputs,
        username=username,
        owner_id=owner_id,
        has_tag=has_tag,
        dataset_name=dataset_name,
    )
    inbox_task = STATUS_INBOX / f"{job_id}.json"
    if inbox_task.exists():
        try:
            inbox_task.unlink()
        except Exception:
            pass


if __name__ == "__main__":
    main()
