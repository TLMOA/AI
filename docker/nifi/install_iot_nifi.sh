#!/usr/bin/env bash
set -euo pipefail

# Usage: sudo ./install_iot_nifi.sh
# This script creates required host dirs, sets permissions, builds the image,
# starts docker-compose, and installs the example systemd unit to /etc/systemd/system.

REPO_DIR=$(cd "$(dirname "$0")" && pwd)
CONF_DIR=/home/yhz/iot/real_nifi_conf
DATA_DIR=/home/yhz/iot/real_nifi_data
LIB_DIR=${DATA_DIR}/lib
UNIT_SRC=${REPO_DIR}/iot-nifi.service
UNIT_DST=/etc/systemd/system/iot-nifi.service

echo "1) 创建目录并设置权限"
mkdir -p "$DATA_DIR" "$CONF_DIR" "$LIB_DIR"
chown -R 1000:1000 "$DATA_DIR" "$CONF_DIR" || true

echo "2) 构建镜像"
cd "$REPO_DIR"
chmod +x build.sh || true
./build.sh

echo "3) 使用 docker-compose 启动"
if command -v docker-compose >/dev/null 2>&1; then
  docker-compose up -d --build
else
  docker compose up -d --build
fi

if [ -f "$UNIT_SRC" ]; then
  echo "4) 安装 systemd unit 到 $UNIT_DST (需要 sudo)"
  cp "$UNIT_SRC" "$UNIT_DST"
  chmod 644 "$UNIT_DST"
  systemctl daemon-reload
  systemctl enable iot-nifi.service
  systemctl start iot-nifi.service || true
  echo "systemd unit 安装并尝试启动完毕"
else
  echo "systemd unit 示例未找到：$UNIT_SRC"
fi

echo "完成。请检查： docker ps | grep iot-nifi  和 systemctl status iot-nifi.service"
