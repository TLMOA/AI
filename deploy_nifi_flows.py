#!/usr/bin/env python3
"""一次性部署并启动 iot-nifi 容器内的真实 NiFi flow。"""
import json
import os
import sys

# 让后端包可被导入
sys.path.insert(0, "/home/yhz/iot/v1-backend")

os.environ.setdefault("NIFI_AUTO_CREATE_CONTAINER", "false")
os.environ.setdefault("NIFI_AUTO_START_CONTAINER", "false")
os.environ.setdefault("NIFI_AUTO_DEPLOY_FLOW", "true")
os.environ.setdefault("NIFI_CONTAINER_NAME", "iot-nifi")
os.environ.setdefault("NIFI_HTTP_PORT", "8080")
os.environ.setdefault("NIFI_API_BASE", "https://localhost:8080/nifi-api")
os.environ.setdefault("NIFI_ADMIN_USER", "admin")
os.environ.setdefault("NIFI_ADMIN_PASSWORD", "admin@nifi123")

from app.nifi_orchestrator import ensure_nifi_ready_for_all_flows

if __name__ == "__main__":
    result = ensure_nifi_ready_for_all_flows()
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    sys.exit(0 if result.get("ok") else 1)
