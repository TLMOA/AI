#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "[1/5] install backend service"
sudo cp "$ROOT_DIR/v1-backend/deploy/iot-backend.service" /etc/systemd/system/iot-backend.service

echo "[2/5] install backend health service/timer"
sudo cp "$ROOT_DIR/v1-backend/deploy/iot-backend-health.service" /etc/systemd/system/iot-backend-health.service
sudo cp "$ROOT_DIR/v1-backend/deploy/iot-backend-health.timer" /etc/systemd/system/iot-backend-health.timer
sudo cp "$ROOT_DIR/v1-backend/deploy/iot-backend-health.sh" /usr/local/bin/iot-backend-health.sh
sudo chmod +x /usr/local/bin/iot-backend-health.sh

echo "[3/5] install frontend service"
sudo cp "$ROOT_DIR/v1-frontend/deploy/iot-frontend.service" /etc/systemd/system/iot-frontend.service

echo "[4/5] reload systemd"
sudo systemctl daemon-reload

echo "[5/5] enable and start services"
sudo systemctl enable --now iot-backend.service
sudo systemctl enable --now iot-backend-health.timer
sudo systemctl enable --now iot-frontend.service

echo "done"
systemctl --no-pager --full status iot-backend.service iot-frontend.service iot-backend-health.timer | cat
