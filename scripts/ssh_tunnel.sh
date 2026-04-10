#!/usr/bin/env bash
set -euo pipefail

# Simple SSH tunnel helper
# Usage:
#  ./ssh_tunnel.sh --user youruser --host 202.113.76.55 [--ssh-port 22] [--key /path/to/key] [--local-port 5174] [--remote-host 127.0.0.1] [--remote-port 5174] [--background]

LOCAL_PORT=5174
REMOTE_HOST=127.0.0.1
REMOTE_PORT=5174
SSH_PORT=22
KEY_PATH=""
BACKGROUND=0

print_usage(){
  cat <<EOF
Usage: $0 --user USER --host HOST [options]
Options:
  --user USER         SSH username (required)
  --host HOST         SSH host or IP (required)
  --ssh-port PORT     SSH port (default 22)
  --key PATH          SSH private key path (optional)
  --local-port PORT   Local port to bind (default 5174)
  --remote-host HOST  Remote bind host (default 127.0.0.1)
  --remote-port PORT  Remote port (default 5174)
  --background        Run tunnel in background (ssh -fN)
  --stop              Stop matching tunnel(s)
  --help              Show this message
EOF
}

ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --user) USER="$2"; shift 2;;
    --host) HOST="$2"; shift 2;;
    --ssh-port) SSH_PORT="$2"; shift 2;;
    --key) KEY_PATH="$2"; shift 2;;
    --local-port) LOCAL_PORT="$2"; shift 2;;
    --remote-host) REMOTE_HOST="$2"; shift 2;;
    --remote-port) REMOTE_PORT="$2"; shift 2;;
    --background) BACKGROUND=1; shift;;
    --stop) STOP=1; shift;;
    --help) print_usage; exit 0;;
    *) ARGS+=("$1"); shift;;
  esac
done

if [[ -z "${USER:-}" || -z "${HOST:-}" ]]; then
  echo "Error: --user and --host are required"
  print_usage
  exit 2
fi

SSH_CMD=(ssh -p "$SSH_PORT")
if [[ -n "$KEY_PATH" ]]; then
  SSH_CMD+=( -i "$KEY_PATH" )
fi

TUNNEL_DESC="-L ${LOCAL_PORT}:${REMOTE_HOST}:${REMOTE_PORT}"

if [[ ${STOP:-0} -eq 1 ]]; then
  echo "Stopping tunnel(s) matching ${TUNNEL_DESC} to ${HOST} for user ${USER}..."
  pkill -f "ssh .*${LOCAL_PORT}:${REMOTE_HOST}:${REMOTE_PORT} .*${USER}@${HOST}" || true
  echo "Done."
  exit 0
fi

# check local port
if ss -ltnp 2>/dev/null | grep -q ":${LOCAL_PORT} "; then
  echo "Warning: local port ${LOCAL_PORT} appears in use. Choose a different --local-port or stop the occupying process." >&2
fi

FULL_CMD=("${SSH_CMD[@]}" -L "${LOCAL_PORT}:${REMOTE_HOST}:${REMOTE_PORT}" "${USER}@${HOST}")

if [[ $BACKGROUND -eq 1 ]]; then
  echo "Starting tunnel in background: ${FULL_CMD[*]} (ssh -fN)"
  if [[ -n "$KEY_PATH" ]]; then
    nohup ssh -i "$KEY_PATH" -p "$SSH_PORT" -fN -L "${LOCAL_PORT}:${REMOTE_HOST}:${REMOTE_PORT}" "${USER}@${HOST}" >/dev/null 2>&1 &
  else
    nohup ssh -p "$SSH_PORT" -fN -L "${LOCAL_PORT}:${REMOTE_HOST}:${REMOTE_PORT}" "${USER}@${HOST}" >/dev/null 2>&1 &
  fi
  sleep 0.3
  echo "Tunnel started. Access http://localhost:${LOCAL_PORT}"
  ps aux | grep "ssh -fN" | grep "${LOCAL_PORT}:${REMOTE_HOST}:${REMOTE_PORT}" || true
else
  echo "Starting tunnel (foreground). Press Ctrl-C to close."
  exec "${SSH_CMD[@]}" -L "${LOCAL_PORT}:${REMOTE_HOST}:${REMOTE_PORT}" "${USER}@${HOST}"
fi
