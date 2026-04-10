#!/usr/bin/env python3
import sys
from pathlib import Path
import json

# ensure app package importable by running from v1-backend root
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from app.engine_factory import engine_from_config
from app.export_worker import export_table_to_csv, run_export_job
from app.main import NIFI_BASE_DIR


def main():
    db_conf = {
        "db_type": "mysql",
        "driver": "pymysql",
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "password": "root",
        "database": "nifi",
    }
    out = {"attempts": []}
    try:
        engine = engine_from_config(db_conf)
        with engine.connect() as conn:
            try:
                res = conn.execute("SHOW TABLES")
                rows = res.fetchall()
                tables = [r[0] for r in rows]
            except Exception:
                tables = []

        if tables:
            table = tables[0]
            dest = Path(NIFI_BASE_DIR) / "exports" / "manual_test"
            dest.mkdir(parents=True, exist_ok=True)
            out_path = dest / f"export_{table}_{Path().resolve().stem}.csv"
            try:
                r = export_table_to_csv(db_conf, table, out_path, limit=1000)
                out["attempts"].append({"mode": "table_export", "table": table, "result": r})
            except Exception as e:
                out["attempts"].append({"mode": "table_export", "table": table, "error": str(e)})
        else:
            # fallback to demo run
            job = {"id": "testdemo", "owner_id": "localtester", "payload": {}}
            r = run_export_job(job)
            out["attempts"].append({"mode": "demo", "result": r})
    except Exception as e:
        out["error"] = str(e)

    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
