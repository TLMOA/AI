#!/bin/bash
# Health check script:
# 1) Query new endpoint /api/v1/health/databases and consider OK when all reported databases are ready
# 2) If the new endpoint cannot be reached, fall back to the old files-list check

URL_NEW="http://127.0.0.1:8081/api/v1/health/databases"
URL_OLD="http://127.0.0.1:8081/api/v1/files?nifiOnly=true&pageNo=1&pageSize=1"
PYTHON=$(which python3 || echo /usr/bin/python3)
# failure threshold behaviour: only restart after N consecutive failures
# can be tuned via env: FAILURE_THRESHOLD (default 3)
FAILURE_THRESHOLD=${FAILURE_THRESHOLD:-3}
# file to persist consecutive failure count; if not writable, fallback to /tmp
COUNTER_FILE=${COUNTER_FILE:-/var/run/iot-backend-health.failcount}
if [ ! -w "$(dirname "$COUNTER_FILE")" ] && [ ! -w "$COUNTER_FILE" ]; then
    COUNTER_FILE=/tmp/iot-backend-health.failcount
fi

read_count() {
    if [ -f "$COUNTER_FILE" ]; then
        cat "$COUNTER_FILE" 2>/dev/null || echo 0
    else
        echo 0
    fi
}

write_count() {
    mkdir -p "$(dirname "$COUNTER_FILE")" 2>/dev/null || true
    echo "$1" > "$COUNTER_FILE" 2>/dev/null || echo "$1" > /tmp/iot-backend-health.failcount 2>/dev/null
}

# Try new health endpoint first. Exit codes:
# 0 -> all DBs ready
# 1 -> endpoint returned but not all ready
# 2 -> network/parse error (will trigger fallback)
${PYTHON} - <<PY > /dev/null 2>&1
import sys, urllib.request, json
try:
    with urllib.request.urlopen("""$URL_NEW""", timeout=5) as r:
        data = json.load(r)
        # support wrapped response {code:0, message:'OK', data: {...}}
        body = data.get('data') if isinstance(data, dict) and 'data' in data else data
        if not isinstance(body, dict):
            sys.exit(2)
        dbs = body.get('databases', [])
        if dbs:
            all_ready = all(bool(d.get('ready')) for d in dbs)
        else:
            # no explicit list: fall back to simulated flag if present
            all_ready = bool(body.get('simulated'))
        sys.exit(0 if all_ready else 1)
except Exception:
    sys.exit(2)
PY

rc=$?
if [ $rc -eq 0 ]; then
    # success -> reset counter
    write_count 0
    exit 0
fi

if [ $rc -eq 1 ]; then
    # reported but not all ready -> increment counter and maybe restart
    cnt=$(read_count)
    cnt=$((cnt + 1))
    write_count $cnt
    if [ $cnt -ge $FAILURE_THRESHOLD ]; then
        echo "[iot-backend-health] databases not ready for $cnt consecutive checks, restarting iot-backend.service"
        systemctl restart iot-backend.service || true
        write_count 0
    else
        echo "[iot-backend-health] databases not ready (consecutive=$cnt/$FAILURE_THRESHOLD)"
    fi
    exit 0
fi

# rc == 2 -> fallback to old check
${PYTHON} - <<PY > /dev/null 2>&1
import sys,urllib.request
try:
    with urllib.request.urlopen("""$URL_OLD""", timeout=5) as r:
        r.read(1)
        sys.exit(0)
except Exception:
    sys.exit(1)
PY
fb_rc=$?
if [ $fb_rc -eq 0 ]; then
    # fallback succeeded -> reset counter
    write_count 0
    exit 0
else
    # fallback failed -> increment counter and maybe restart
    cnt=$(read_count)
    cnt=$((cnt + 1))
    write_count $cnt
    if [ $cnt -ge $FAILURE_THRESHOLD ]; then
        echo "[iot-backend-health] fallback health check failed for $cnt consecutive checks, restarting iot-backend.service"
        systemctl restart iot-backend.service || true
        write_count 0
    else
        echo "[iot-backend-health] fallback health check failed (consecutive=$cnt/$FAILURE_THRESHOLD)"
    fi
fi
