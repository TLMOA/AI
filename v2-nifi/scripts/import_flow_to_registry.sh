#!/usr/bin/env bash
set -euo pipefail

# import_flow_to_registry.sh
# 用法1: 创建 bucket (若不存在): ./import_flow_to_registry.sh create-bucket http://localhost:18080
# 用法2: 上传 bundle: ./import_flow_to_registry.sh upload http://localhost:18080 <bucketId> /path/to/flow-bundle.zip

ACTION="$1"
REGISTRY_URL="$2"

if [ "$ACTION" = "create-bucket" ]; then
  echo "Create bucket 'iot-flow' on ${REGISTRY_URL}"
  curl -s -X POST -H "Content-Type: application/json" \
    -d '{"name":"iot-flow","description":"iot export flows"}' \
    "${REGISTRY_URL%/}/nifi-registry-api/buckets" | jq || true
  exit 0
fi

if [ "$ACTION" = "upload" ]; then
  BUCKET_ID="$3"
  BUNDLE_PATH="$4"
  if [ -z "${BUCKET_ID}" ] || [ -z "${BUNDLE_PATH}" ]; then
    echo "Usage: $0 upload <registry-url> <bucketId> <bundle-file>"
    exit 2
  fi
  echo "Uploading ${BUNDLE_PATH} to bucket ${BUCKET_ID}"
  # NiFi Registry expects multipart/form-data upload for a flow archive
  curl -s -X POST \
    -F "bucketIdentifier=${BUCKET_ID}" \
    -F "flow=@${BUNDLE_PATH}" \
    "${REGISTRY_URL%/}/nifi-registry-api/buckets/${BUCKET_ID}/flows" -w '\nHTTP_CODE:%{http_code}\n' || true
  exit 0
fi

echo "Unknown action. Supported: create-bucket, upload"
exit 2
