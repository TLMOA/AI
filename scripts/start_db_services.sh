#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  exec sudo -E bash "$0" "$@"
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKEND_URL="http://127.0.0.1:8081"
SQLITE_PATH="$ROOT_DIR/backup_sqlite/app.db.1778064917"

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

compose_up() {
  local stack_dir="$1"
  local compose_file="$stack_dir/docker-compose.yml"
  if [[ ! -f "$compose_file" ]]; then
    log "skip compose stack: $stack_dir"
    return 0
  fi

  log "docker compose up -d in $(basename "$stack_dir")"
  if command -v docker-compose >/dev/null 2>&1; then
    (cd "$stack_dir" && docker-compose up -d)
  else
    (cd "$stack_dir" && docker compose up -d)
  fi
}

start_systemd_unit() {
  local unit="$1"
  local action="${2:-start}"
  local load_state
  load_state="$(systemctl show -p LoadState --value "$unit" 2>/dev/null || true)"
  if [[ "$load_state" != "loaded" ]]; then
    log "skip missing unit: $unit"
    return 0
  fi
  log "systemctl $action $unit"
  systemctl "$action" "$unit"
}

start_container_if_exists() {
  local name="$1"
  if ! docker inspect "$name" >/dev/null 2>&1; then
    log "skip missing container: $name"
    return 0
  fi

  local state
  state="$(docker inspect -f '{{.State.Status}}' "$name")"
  if [[ "$state" == "running" ]]; then
    log "container already running: $name"
    return 0
  fi

  log "docker start $name"
  docker start "$name" >/dev/null
}

wait_for_tcp() {
  local host="$1"
  local port="$2"
  local label="$3"
  local timeout_seconds="${4:-120}"
  local deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    if bash -lc "echo >/dev/tcp/${host}/${port}" >/dev/null 2>&1; then
      log "ready: ${label} (${host}:${port})"
      return 0
    fi
    sleep 2
  done

  log "timeout waiting for ${label} (${host}:${port})"
  return 1
}

wait_for_http() {
  local url="$1"
  local label="$2"
  local timeout_seconds="${3:-120}"
  local deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      log "ready: $label"
      return 0
    fi
    sleep 2
  done

  log "timeout waiting for $label"
  return 1
}

ensure_sqlite_access() {
  if [[ -f "$SQLITE_PATH" ]]; then
    log "fix sqlite permissions: $SQLITE_PATH"
    chown -R yhz:yhz "$ROOT_DIR/backup_sqlite"
    chmod 755 "$ROOT_DIR/backup_sqlite"
    chmod 644 "$SQLITE_PATH"
  else
    log "sqlite file not found, skip: $SQLITE_PATH"
  fi
}

test_db() {
  local name="$1"
  local payload="$2"
  local resp_file
  resp_file="$(mktemp)"

  if ! curl -fsS -X POST "$BACKEND_URL/api/v1/db/test-connection" \
    -H 'Content-Type: application/json' \
    -d "$payload" >"$resp_file"; then
    log "[FAIL] $name: backend request failed"
    cat "$resp_file" || true
    rm -f "$resp_file"
    return 1
  fi

  if python3 - "$name" "$resp_file" <<'PY'
import json
import sys

name = sys.argv[1]
path = sys.argv[2]

with open(path, 'r', encoding='utf-8') as fh:
    data = json.load(fh)

if data.get('code') == 0:
    print(f'[OK] {name}: {data.get("message", "OK")}')
    sys.exit(0)

detail = data.get('detail') or ''
print(f'[FAIL] {name}: {data.get("message", "unknown error")} {detail}'.rstrip())
sys.exit(1)
PY
  then
    rm -f "$resp_file"
    return 0
  fi

  rm -f "$resp_file"
  return 1
}

log "ensure docker daemon is running"
systemctl start docker >/dev/null 2>&1 || true

ensure_sqlite_access

compose_up "$ROOT_DIR/docker/hadoop"

for container in docker-db-1 mssql-local iot-oracle-free; do
  start_container_if_exists "$container"
done

wait_for_tcp 127.0.0.1 5432 "postgres"
wait_for_tcp 127.0.0.1 1433 "sqlserver"
wait_for_tcp 127.0.0.1 1521 "oracle"
wait_for_tcp 127.0.0.1 2181 "zookeeper"
wait_for_tcp 127.0.0.1 9870 "hdfs namenode"
wait_for_tcp 127.0.0.1 10000 "hive server2"
wait_for_tcp 127.0.0.1 19090 "hbase thrift"

start_systemd_unit iot-backend.service restart
start_systemd_unit iot-frontend.service restart
start_systemd_unit iot-backend-health.timer start

wait_for_http "$BACKEND_URL/health" "backend health"

log "run backend database connectivity checks"
test_db sqlite '{"db_type":"sqlite","host":"127.0.0.1","port":0,"username":"root","password":"","database":"/home/yhz/iot/backup_sqlite/app.db.1778064917"}'
test_db postgres '{"db_type":"postgresql","host":"127.0.0.1","port":5432,"username":"postgres","password":"difyai123456","database":"postgres"}'
test_db sqlserver '{"db_type":"sqlserver","host":"127.0.0.1","port":1433,"username":"sa","password":"Your_password123","database":"master"}'
test_db oracle '{"db_type":"oracle","host":"127.0.0.1","port":1521,"username":"system","password":"Oracle123456","database":"FREEPDB1"}'
test_db hive '{"db_type":"hive","host":"localhost","port":10000,"username":"hive","password":"","database":"default"}'
test_db hbase '{"db_type":"hbase","host":"127.0.0.1","port":19090,"username":"root","password":"","database":""}'
test_db hdfs '{"db_type":"hdfs","host":"localhost","port":9870,"username":"hadoop","password":"","database":"/"}'

log "all database startup and connectivity checks completed"