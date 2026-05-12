#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="iot-nifi-python"
TAG="latest"
BUILD_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Building NiFi image: ${IMAGE_NAME}:${TAG}"
docker build -t "${IMAGE_NAME}:${TAG}" -f "${BUILD_DIR}/Dockerfile" "${BUILD_DIR}/.."

echo "Built ${IMAGE_NAME}:${TAG}. To run:"
echo "  docker run -d --name iot-nifi -p 8080:8080 -v /home/yhz/iot/real_nifi_data:/opt/nifi/nifi-current/data/iot iot-nifi-python:latest"
