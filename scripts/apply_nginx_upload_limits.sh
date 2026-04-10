#!/bin/bash
set -euo pipefail
TIMESTAMP=$(date +%s)
BACKUP=/etc/nginx/nginx.conf.bak.${TIMESTAMP}
# backup
sudo cp /etc/nginx/nginx.conf "${BACKUP}" || true
# write conf (safe, can be removed to rollback)
sudo tee /etc/nginx/conf.d/upload_limits.conf > /dev/null <<'CONF'
# Added by assistant: increase upload size and timeouts for file uploads
client_max_body_size 50M;
proxy_read_timeout 300s;
proxy_send_timeout 300s;
proxy_buffering off;
CONF
# test nginx config
sudo nginx -t
# reload nginx (graceful)
sudo systemctl reload nginx
echo "OK: upload_limits applied; backup: ${BACKUP}"
