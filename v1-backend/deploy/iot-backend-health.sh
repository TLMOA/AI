#!/bin/bash
# simple health check: try to fetch minimal API response; restart service on failure
URL="http://127.0.0.1:8081/api/v1/files?nifiOnly=true&pageNo=1&pageSize=1"
PYTHON=$(which python3 || echo /usr/bin/python3)
${PYTHON} - <<PY > /dev/null 2>&1
import sys,urllib.request
try:
    with urllib.request.urlopen("""$URL""", timeout=5) as r:
        r.read(1)
        sys.exit(0)
except Exception:
    sys.exit(1)
PY
if [ $? -ne 0 ]; then
    echo "[iot-backend-health] health check failed, restarting iot-backend.service"
    systemctl restart iot-backend.service || true
fi
