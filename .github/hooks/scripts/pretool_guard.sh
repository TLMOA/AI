#!/usr/bin/env bash
set -euo pipefail

payload="$(cat || true)"

# Lightweight guard: request confirmation for risky git/shell patterns.
if echo "$payload" | grep -Eqi 'git reset --hard|git checkout --|rm -rf|dd if=|mkfs|shutdown|reboot'; then
  cat <<'JSON'
{
  "hookSpecificOutput": {
    "hookEventName": "PreToolUse",
    "permissionDecision": "ask",
    "permissionDecisionReason": "Potentially destructive command detected. Please confirm before execution."
  }
}
JSON
  exit 0
fi

# Allow by default.
cat <<'JSON'
{
  "hookSpecificOutput": {
    "hookEventName": "PreToolUse",
    "permissionDecision": "allow",
    "permissionDecisionReason": "No high-risk pattern detected."
  }
}
JSON
