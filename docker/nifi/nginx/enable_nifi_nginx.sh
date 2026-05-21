#!/usr/bin/env bash
set -euo pipefail

# enable_nifi_nginx.sh
# Usage: sudo ./enable_nifi_nginx.sh <HOST_IP>
# This script writes the templated nginx conf to /etc/nginx/conf.d,
# reloads nginx and opens firewall for port 9443.

if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  echo "Please run as root: sudo $0 <HOST_IP>"
  exit 1
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <HOST_IP>"
  exit 1
fi

HOST_IP="$1"
TEMPLATE_DIR="$(cd "$(dirname "$0")" && pwd)"
TEMPLATE="$TEMPLATE_DIR/nifi-lan.conf.template"
DEST_CONF="/etc/nginx/conf.d/nifi-lan.conf"

if [[ ! -f "$TEMPLATE" ]]; then
  echo "Template not found: $TEMPLATE"
  exit 1
fi

echo "Writing nginx conf to $DEST_CONF (listening 9443)"
sed "s/HOST_IP/${HOST_IP}/g" "$TEMPLATE" > "$DEST_CONF"
chmod 644 "$DEST_CONF"

echo "Removing legacy default site files (if exist)"
rm -f /etc/nginx/sites-enabled/default /etc/nginx/sites-available/default /etc/nginx/conf.d/default.conf || true

echo "Testing nginx configuration"
nginx -t

echo "Reloading nginx"
systemctl restart nginx

echo "Opening firewall port 9443 (ufw if present and active)"
if command -v ufw >/dev/null 2>&1; then
  if ufw status | grep -qi "Status: active"; then
    ufw allow 9443/tcp || true
  else
    echo "ufw is installed but inactive; skipping firewall change."
  fi
else
  echo "ufw not installed; skipping firewall change."
fi

echo "Done. NiFi should be reachable at: https://${HOST_IP}:9443/nifi/"
echo "If browser warns about certificate, import the generated /etc/nginx/ssl/nifi-lan.crt into trusted store, or use -k for curl."
