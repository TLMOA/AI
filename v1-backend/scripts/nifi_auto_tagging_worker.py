#!/usr/bin/env python3
"""NiFi-invoked auto-tagging worker.

Reads task JSON from stdin, applies tagging rules to source file, outputs to tagged_output.

Task JSON structure:
{
  "jobId": "tag_abc123",
  "sourcePath": "/opt/nifi/nifi-current/data/iot/output_csv/export_admin_sensor_data.csv",
  "sourceFormat": "CSV",
  "tagType": "manual-table",
  "tagConfig": {
    "columns": ["status"],
    "mappings": {
      "row_rules": [
        {"column": "status", "mapping": {"default": "已打标", "0": "正常", "1": "告警"}}
      ]
    }
  },
  "targetFormat": "CSV",
  "fileName": "export_admin_sensor_data.csv",
  "ownerId": "admin",
  "factoryId": "factory-001"
}
"""

import csv
import json
import os
import re
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


DATA_ROOT = Path("/opt/nifi/nifi-current/data/iot")
TAGGED_OUTPUT_DIR = DATA_ROOT / "tagged_output"
STATUS_DONE = DATA_ROOT / "tagging_jobs" / "done"
STATUS_ERROR = DATA_ROOT / "tagging_jobs" / "error"


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


# ---------- file reading (reused pattern) ----------

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


# ---------- atomic write ----------

def write_csv_atomic(path: Path, rows: List[Dict[str, Any]]):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    tmp = path.parent / f".tmp_{path.name}"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    os.rename(tmp, path)


def write_tsv_atomic(path: Path, rows: List[Dict[str, Any]]):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    tmp = path.parent / f".tmp_{path.name}"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()), delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    os.rename(tmp, path)


def write_json_atomic(path: Path, rows: List[Dict[str, Any]]):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    tmp = path.parent / f".tmp_{path.name}"
    with open(tmp, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    os.rename(tmp, path)


def write_file_atomic(path: Path, fmt: str, rows: List[Dict[str, Any]]):
    fmt = fmt.upper()
    if fmt == "CSV":
        write_csv_atomic(path, rows)
    elif fmt == "JSON":
        write_json_atomic(path, rows)
    elif fmt == "TSV":
        write_tsv_atomic(path, rows)
    else:
        raise ValueError(f"unsupported target format: {fmt}")


# ---------- status file ----------

def write_status(status_dir: Path, job_id: str, status: str, file_path: str = "", message: str = "", owner_id: str = ""):
    _ensure_dir(status_dir)
    status_path = status_dir / f"{job_id}.json"
    payload = {
        "jobId": job_id,
        "status": status,
        "filePath": file_path,
        "message": message,
        "ownerId": owner_id,
        "factoryId": owner_id,
        "username": owner_id,
        "finishedAt": now_iso(),
    }
    status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------- tagging logic ----------

def apply_manual_table_tags(rows: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """手动打标：逐行逐列按 mapping 替换值为标签。

    config.mappings.row_rules: [{column, mapping: {val: tag, "default": fallback_tag}}]
    """
    rules = config.get("mappings", {}).get("row_rules", [])
    if not rules:
        return rows

    tagged_rows = []
    for row in rows:
        new_row = dict(row)
        for rule in rules:
            col = rule.get("column", "")
            if not col or col not in new_row:
                continue
            mapping = rule.get("mapping", {})
            default_tag = mapping.pop("default", None)
            raw_val = str(new_row.get(col, "")).strip()
            tag = mapping.get(raw_val, default_tag)
            if tag is not None:
                new_row[col] = tag
        tagged_rows.append(new_row)
    return tagged_rows


def _eval_range_condition(value: float, condition: str) -> bool:
    """Evaluate a range condition like '> 30', '<= 100', '10-50'."""
    condition = condition.strip()
    # range: "10-50"
    if "-" in condition and not condition.startswith(("-", ">")):
        parts = condition.split("-")
        if len(parts) == 2:
            try:
                lo = float(parts[0].strip())
                hi = float(parts[1].strip())
                return lo <= value <= hi
            except ValueError:
                pass
    # comparison: "> 30", "<= 100", "== 0"
    for op in (">=", "<=", "!=", "==", ">", "<"):
        if condition.startswith(op):
            try:
                threshold = float(condition[len(op):].strip())
                if op == ">=": return value >= threshold
                elif op == "<=": return value <= threshold
                elif op == "!=": return value != threshold
                elif op == "==": return value == threshold
                elif op == ">": return value > threshold
                elif op == "<": return value < threshold
            except ValueError:
                pass
    return False


def _eval_regex_condition(value: str, pattern: str) -> bool:
    try:
        return bool(re.search(pattern, value))
    except re.error:
        return False


def _eval_condition(row: Dict[str, Any], condition: Dict[str, Any]) -> bool:
    """Evaluate a single condition dict against a row."""
    cond_type = condition.get("type", "")
    column = condition.get("column", "")
    val = str(row.get(column, "")) if column else ""

    if cond_type == "regex":
        return _eval_regex_condition(val, condition.get("pattern", ""))
    elif cond_type == "range":
        try:
            return _eval_range_condition(float(val), condition.get("value", ""))
        except (ValueError, TypeError):
            return False
    elif cond_type == "equals":
        return val == str(condition.get("value", ""))
    elif cond_type == "contains":
        return str(condition.get("value", "")) in val
    elif cond_type == "not_empty":
        return bool(val.strip())
    elif cond_type == "is_empty":
        return not bool(val.strip())
    return False


def apply_auto_rules(rows: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """自动打标：按预定义规则集逐行匹配，命中即打标。

    config.rules: [{name, conditions: [{type, column, value/pattern}], then: {tagColumn, tagValue}}]
    """
    rules = config.get("rules", [])
    if not rules:
        return rows

    tagged_rows = []
    for row in rows:
        new_row = dict(row)
        for rule in rules:
            conditions = rule.get("conditions", [])
            # 所有条件都满足才命中
            if conditions and all(_eval_condition(row, c) for c in conditions):
                then = rule.get("then", {})
                tag_col = then.get("tagColumn", "tag")
                tag_val = then.get("tagValue", rule.get("name", "已打标"))
                new_row[tag_col] = tag_val
                break  # 第一个命中的规则生效
        tagged_rows.append(new_row)
    return tagged_rows


def apply_ai_suggestions(rows: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """AI 建议打标：预留接口，当前直接返回原数据。"""
    return rows


# ---------- main ----------

def main():
    task: Dict[str, Any] = {}
    try:
        task = json.load(sys.stdin)
        job_id = str(task.get("jobId") or "")
        source_path = str(task.get("sourcePath") or "")
        source_format = str(task.get("sourceFormat") or "").upper()
        tag_type = str(task.get("tagType") or "manual-table")
        tag_config = task.get("tagConfig") or {}
        target_format = str(task.get("targetFormat") or source_format)
        file_name = str(task.get("fileName") or "unknown")
        owner_id = str(task.get("ownerId") or task.get("factoryId") or "unknown")

        # 1. Read source file
        rows = read_file(source_path, source_format)

        # 2. Apply tagging rules
        if tag_type == "manual-table":
            tagged = apply_manual_table_tags(rows, tag_config)
        elif tag_type == "auto-rule":
            tagged = apply_auto_rules(rows, tag_config)
        elif tag_type == "ai-suggestion":
            tagged = apply_ai_suggestions(rows, tag_config)
        else:
            raise ValueError(f"unsupported tagType: {tag_type}")

        # 3. Write output
        _ensure_dir(TAGGED_OUTPUT_DIR)
        stem = Path(file_name).stem
        ext = target_format.lower()
        output_name = f"tag_{owner_id}_{stem}_{now_ts()}.{ext}"
        output_path = TAGGED_OUTPUT_DIR / output_name
        write_file_atomic(output_path, target_format, tagged)

        # 4. Write success status
        write_status(STATUS_DONE, job_id, "SUCCEEDED", str(output_path.resolve()),
                     f"tagged {len(tagged)} rows, tagType={tag_type}", owner_id=owner_id)
        print(json.dumps({
            "jobId": job_id,
            "status": "SUCCEEDED",
            "filePath": str(output_path.resolve()),
            "rows": len(tagged),
            "message": f"tagging completed via {tag_type}",
            "finishedAt": now_iso(),
        }, ensure_ascii=False, default=str))
        return 0

    except Exception as exc:
        job_id = task.get("jobId", "") if isinstance(task, dict) else ""
        owner_id = task.get("ownerId") or task.get("factoryId") or "" if isinstance(task, dict) else ""
        payload = {
            "jobId": job_id,
            "status": "FAILED",
            "filePath": "",
            "rows": 0,
            "message": str(exc),
            "errorTrace": traceback.format_exc(limit=5),
            "finishedAt": now_iso(),
        }
        try:
            write_status(STATUS_ERROR, job_id, "FAILED", "", str(exc), owner_id=owner_id)
        except Exception:
            pass
        print(json.dumps(payload, ensure_ascii=False, default=str), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())