#!/usr/bin/env bash
set -euo pipefail

# push_image.sh: 将本地镜像推送到私有 Registry
# 用法: ./push_image.sh <registry> [image:tag]
# 例如: ./push_image.sh registry.example.com:5000 iot-nifi-python:latest

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <registry> [image:tag]"
  exit 2
fi

REGISTRY="$1"
IMAGE="${2:-iot-nifi-python:latest}"

FULL_IMAGE="${REGISTRY%/}/${IMAGE}"

echo "Tagging ${IMAGE} -> ${FULL_IMAGE}"
docker tag "${IMAGE}" "${FULL_IMAGE}"

echo "Pushing ${FULL_IMAGE} (ensure you ran: docker login ${REGISTRY})"
docker push "${FULL_IMAGE}"

echo "Done."
