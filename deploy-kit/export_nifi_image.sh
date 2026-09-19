#!/usr/bin/env bash
# ============================================================
# 导出 NiFi 镜像 —— 供【离线】工厂机器使用
#
# 为什么要这个：
#   deploy.sh 默认用 docker build 构建 NiFi 镜像，需要从外网拉取
#   apache/nifi 基础镜像（约 1.3GB）。工厂机器若无外网，build 必然失败。
#   解决办法：在能联网的机器上先把镜像导出成文件，随交付介质带过去，
#   工厂机器上 docker load 即可，全程不需要网络。
#
# 用法:
#   ./export_nifi_image.sh [输出目录]      # 默认 /home/yhz
#
# 产物: iot-nifi-python-<日期>.tar（约 1.7GB）
# 工厂机器上: docker load -i iot-nifi-python-<日期>.tar
#             （deploy.sh 检测到该文件会自动加载，跳过 build）
# ============================================================
set -euo pipefail

OUT_DIR="${1:-/home/yhz}"
IMG="iot-nifi-python:latest"
STAMP="$(date +%Y%m%d)"
OUT="${OUT_DIR}/iot-nifi-python-${STAMP}.tar"

C_G='\033[0;32m'; C_Y='\033[0;33m'; C_R='\033[0;31m'; C_OFF='\033[0m'

command -v docker >/dev/null 2>&1 || { echo -e "${C_R}[err] 未找到 docker${C_OFF}"; exit 1; }

# 若本地还没构建过，先构建（需要联网）
if ! docker image inspect "$IMG" >/dev/null 2>&1; then
  echo -e "${C_Y}[info] 本地没有 $IMG，先构建（需要联网）...${C_OFF}"
  KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  PROJECT_DIR="$(cd "$KIT_DIR/.." && pwd)"
  (cd "$PROJECT_DIR/docker/nifi" && docker build -t "$IMG" .) \
    || { echo -e "${C_R}[err] 构建失败，请确认能访问外网${C_OFF}"; exit 1; }
fi

echo -e "${C_G}[export]${C_OFF} 导出 $IMG -> $OUT"
docker save -o "$OUT" "$IMG"

SIZE=$(du -h "$OUT" | cut -f1)
echo
echo "======================================"
echo -e "${C_G}导出完成${C_OFF}"
echo "  $OUT  ($SIZE)"
echo
echo "交付方式（三选一）："
echo "  1) 随离线包一起拷给工厂（包会变大，适合 U 盘/移动硬盘）"
echo "  2) 单独拷，工厂机器上先执行："
echo "       sudo docker load -i $(basename "$OUT")"
echo "     再跑 deploy.sh（会自动检测到已加载的镜像，跳过 build）"
echo "  3) 放到 deploy-kit/ 目录下命名为 nifi-image.tar，deploy.sh 会自动加载"
echo
echo "验证（工厂机器上）：docker images | grep iot-nifi"
echo "======================================"
