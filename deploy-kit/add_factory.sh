#!/usr/bin/env bash
# ============================================================
# 开户：给一个工厂（或管理员）创建账号 + 数据目录
#
# 用法:
#   sudo ./add_factory.sh <工厂名> [密码] [--private <本地路径>] [--admin]
#
# 例:
#   sudo ./add_factory.sh factory_a                       # 公有化，数据落 <APP_HOME>/factory_a
#   sudo ./add_factory.sh factory_b Secret123             # 指定密码
#   sudo ./add_factory.sh factory_c --private /data/fc    # 私有化，数据落 /data/fc
#   sudo ./add_factory.sh humi2 --admin                   # 再建一个管理员
#
# 说明:
#   - 私有化需要 deploy.sh 时已开启 APPLY_PRIVATE_STORAGE_PATCH=true，
#     否则 ceph_endpoint 只入库不生效，数据仍会落在平台本地盘。
# ============================================================
set -euo pipefail

KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -f "$KIT_DIR/config.env" ]] || { echo "[err] 缺少 config.env"; exit 1; }
set -a; source "$KIT_DIR/config.env"; source "$KIT_DIR/.deploy-secrets" 2>/dev/null || true; set +a

APP_USER="${APP_USER:-yhz}"
APP_HOME="${APP_HOME:-/home/$APP_USER}"
CODE_DIR="${CODE_DIR:-$APP_HOME/iot}"
DB_NAME="${DB_NAME:-nifi}"; DB_USER="${DB_USER:-iot}"
VENV="$CODE_DIR/v1-backend/.venv"
[[ -x "$VENV/bin/python" ]] || { echo "[err] 找不到虚拟环境 $VENV，请先执行 deploy.sh"; exit 1; }

NAME="${1:-}"; [[ -n "$NAME" ]] || { echo "[err] 请提供工厂名"; exit 1; }
shift
PASS=""; MODE="public"; CEPH=""; IS_ADMIN=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --private) MODE="private"; CEPH="${2:-}"; shift 2 ;;
    --admin)   IS_ADMIN="--admin"; shift ;;
    *)         PASS="$1"; shift ;;
  esac
done
[[ -n "$PASS" ]] || PASS="$(head -c 12 /dev/urandom | base64 | tr -d '\n')"
if [[ "$MODE" == "private" && -z "$CEPH" ]]; then
  echo "[err] --private 必须跟一个本地路径"; exit 1
fi

echo "[add_factory] 创建 $NAME (mode=$MODE${CEPH:+, path=$CEPH}${IS_ADMIN:+, admin})"

# 切用户统一走 as_user（runuser → sudo → su）：以前写死 sudo -u，没装 sudo 的机器会失败
source "$KIT_DIR/tools/lib.sh"
as_user env \
  CODE_DIR="$CODE_DIR" DB_HOST=127.0.0.1 DB_PORT=3306 \
  DB_USER="$DB_USER" DB_PASS="$DB_PASS" DB_NAME="$DB_NAME" \
  IN_DATA_BASE_DIR="$APP_HOME" \
  "$VENV/bin/python" "$KIT_DIR/tools/userctl.py" \
  create "$NAME" "$PASS" --mode "$MODE" ${CEPH:+--ceph "$CEPH"} $IS_ADMIN

cat <<EOF

  账号    ${NAME}
  密码    ${PASS}
  模式    ${MODE}${CEPH:+  (数据目录: $CEPH)}
  请把账号密码交付给该工厂，并提醒首次登录后修改密码。
EOF
