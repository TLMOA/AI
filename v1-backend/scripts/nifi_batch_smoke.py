#!/usr/bin/env python3
import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from typing import Dict, List, Tuple

DEFAULT_JOB_TYPES = [
    "CONVERT",
    "TAG_AUTO",
    "COPY_MULTI_FORMAT",
    "IMPORT_EXTERNAL",
    "TAG_MANUAL",
]


def api_get(base: str, path: str, timeout: int) -> Dict:
    with urllib.request.urlopen(f"{base}{path}", timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def api_post(base: str, path: str, payload: Dict, timeout: int) -> Dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(f"{base}{path}", data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def create_job_payload(job_type: str) -> Dict:
    payload = {
        "jobType": job_type,
        "source": {
            "sourceType": "MYSQL_TABLE",
            "dbSourceId": "db-1",
            "tableName": "sensor_data",
            "where": "id <= 20",
        },
        "target": {
            "format": "CSV",
            "outputDir": "/data/jobs",
        },
        "tagConfig": {
            "mode": "AUTO",
            "ruleId": "NIFI_RULE_ID_V5",
        },
        "copyFormats": ["CSV", "JSON"] if job_type == "COPY_MULTI_FORMAT" else [],
        "cron": "",
        "runBy": "batch-smoke",
        "remark": "nifi batch smoke",
    }
    return payload


def wait_final(base: str, job_id: str, max_wait_sec: int, poll_interval_sec: float, timeout: int) -> Dict:
    deadline = time.time() + max_wait_sec
    while time.time() < deadline:
        row = api_get(base, f"/jobs/{job_id}", timeout).get("data", {})
        status = row.get("status")
        if status in {"SUCCEEDED", "FAILED", "CANCELED"}:
            return row
        time.sleep(poll_interval_sec)
    return api_get(base, f"/jobs/{job_id}", timeout).get("data", {})


def run_once(base: str, job_types: List[str], max_wait_sec: int, timeout: int) -> Tuple[List[Dict], bool]:
    rows: List[Dict] = []
    all_ok = True

    for job_type in job_types:
        created = api_post(base, "/jobs", create_job_payload(job_type), timeout)
        if created.get("code") != 0:
            all_ok = False
            rows.append(
                {
                    "jobType": job_type,
                    "jobId": "",
                    "status": "FAILED_TO_CREATE",
                    "errorCode": created.get("message", "create failed"),
                    "nifiFlowId": "",
                    "elapsedSec": 0,
                }
            )
            continue

        job_id = created["data"]["jobId"]
        t0 = time.time()
        final = wait_final(base, job_id, max_wait_sec=max_wait_sec, poll_interval_sec=1.0, timeout=timeout)
        elapsed = round(time.time() - t0, 2)
        status = final.get("status", "UNKNOWN")
        if status != "SUCCEEDED":
            all_ok = False

        rows.append(
            {
                "jobType": job_type,
                "jobId": job_id,
                "status": status,
                "errorCode": final.get("errorCode", ""),
                "nifiFlowId": final.get("nifiFlowId", ""),
                "elapsedSec": elapsed,
            }
        )

    return rows, all_ok


def main() -> int:
    parser = argparse.ArgumentParser(description="NiFi mode batch smoke test for v1-backend")
    parser.add_argument("--base-url", default="http://127.0.0.1:8082/api/v1", help="Backend API base URL")
    parser.add_argument("--rounds", type=int, default=1, help="How many rounds to run")
    parser.add_argument("--max-wait-sec", type=int, default=30, help="Max wait time per job")
    parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout in seconds")
    parser.add_argument(
        "--job-types",
        default=",".join(DEFAULT_JOB_TYPES),
        help="Comma-separated job types to test",
    )
    parser.add_argument("--json-out", default="", help="Optional output path for JSON report")
    args = parser.parse_args()

    job_types = [x.strip() for x in args.job_types.split(",") if x.strip()]
    report = {
        "baseUrl": args.base_url,
        "rounds": args.rounds,
        "jobTypes": job_types,
        "startedAt": time.strftime("%Y-%m-%d %H:%M:%S"),
        "results": [],
        "success": True,
    }

    try:
        executor_info = api_get(args.base_url, "/system/executor", args.timeout).get("data", {})
        report["executor"] = executor_info
    except Exception as e:
        print(f"[ERROR] cannot reach backend: {e}")
        return 2

    if report.get("executor", {}).get("mode") != "nifi":
        print("[ERROR] backend is not in nifi mode. abort.")
        print(json.dumps(report.get("executor", {}), ensure_ascii=False, indent=2))
        return 2

    for idx in range(1, max(args.rounds, 1) + 1):
        rows, ok = run_once(args.base_url, job_types, max_wait_sec=args.max_wait_sec, timeout=args.timeout)
        report["results"].append({"round": idx, "rows": rows, "success": ok})
        report["success"] = report["success"] and ok

        print(f"\\n=== Round {idx} ===")
        for row in rows:
            print(
                f"{row['jobType']:>18} | {row['status']:<10} | {row['errorCode']:<20} | "
                f"{row['elapsedSec']:>5}s | {row['jobId']}"
            )

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

    print("\\n=== Summary ===")
    print(f"overall success: {report['success']}")
    if args.json_out:
        print(f"json report: {args.json_out}")

    return 0 if report["success"] else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except urllib.error.URLError as e:
        print(f"[ERROR] network failure: {e}")
        sys.exit(2)
    except Exception as e:
        print(f"[ERROR] unexpected failure: {e}")
        sys.exit(2)
