#!/usr/bin/env bash
set -euo pipefail

# Generate a self-signed TLS cert for NiFi LAN access via nginx.
# Usage:
#   sudo ./generate_nifi_self_signed_cert.sh <HOST_IP> [CERT_DIR]
# Example:
#   sudo ./generate_nifi_self_signed_cert.sh 192.168.1.10 /etc/nginx/ssl

if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  echo "Please run as root (sudo)."
  exit 1
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <HOST_IP> [CERT_DIR]"
  exit 1
fi

HOST_IP="$1"
CERT_DIR="${2:-/etc/nginx/ssl}"
CERT_CRT="${CERT_DIR}/nifi-lan.crt"
CERT_KEY="${CERT_DIR}/nifi-lan.key"
OPENSSL_CNF="$(mktemp)"

trap 'rm -f "$OPENSSL_CNF"' EXIT

mkdir -p "$CERT_DIR"

cat >"$OPENSSL_CNF" <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
x509_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = nifi.lan

[v3_req]
subjectAltName = @alt_names

[alt_names]
IP.1 = ${HOST_IP}
DNS.1 = localhost
DNS.2 = nifi.lan
EOF

openssl req -x509 -nodes -days 825 -newkey rsa:2048 \
  -keyout "$CERT_KEY" \
  -out "$CERT_CRT" \
  -config "$OPENSSL_CNF" \
  -extensions v3_req

chmod 600 "$CERT_KEY"
chmod 644 "$CERT_CRT"

echo "Generated certificate: $CERT_CRT"
echo "Generated private key: $CERT_KEY"
echo "Next: install nginx site config and reload nginx."
