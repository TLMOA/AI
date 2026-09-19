#!/usr/bin/env bash
# ============================================================
# 修改本站账号的登录密码
#
#   sudo ./set_password.sh                 # 改当前站点账号，交互输入新密码
#   sudo ./set_password.sh <新密码>         # 直接给新密码
#   sudo ./set_password.sh <账号> <新密码>   # 指定账号（多账号的情况）
#
# 为什么需要这个脚本：
#   部署脚本（deploy.sh）现在是【重跑安全】的 —— 账号已存在时不会去动密码，
#   免得每次重跑都把现场人员记着的密码改掉。
#   所以"改密码"这件事单独用一个明确的操作来做，就是本脚本。
#
# 只改密码，不动数据；改完立即生效（下次登录用新密码）。
# ============================================================
set -uo pipefail

KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="$KIT_DIR/config.env"
SECRETS="$KIT_DIR/.deploy-secrets"

C_G='\033[0;32m'; C_Y='\033[0;33m'; C_R='\033[0;31m'; C_OFF='\033[0m'
log()  { echo -e "${C_G}[pwd]${C_OFF} $*"; }
warn() { echo -e "${C_Y}[warn]${C_OFF} $*"; }
err()  { echo -e "${C_R}[err]${C_OFF} $*" >&2; }

[[ $EUID -eq 0 ]] || { err "请用 root 运行：sudo bash set_password.sh"; exit 1; }
[[ -f "$CONFIG" ]] || { err "找不到 $CONFIG —— 这台机器还没部署过？"; exit 1; }

# shellcheck disable=SC1090
set -a; source "$CONFIG"; set +a
if [[ -f "$SECRETS" ]]; then
  # shellcheck disable=SC1090
  set -a; source "$SECRETS"; set +a
fi

APP_USER="${APP_USER:-yhz}"
APP_HOME="${APP_HOME:-/home/$APP_USER}"
CODE_DIR="${CODE_DIR:-$APP_HOME/iot}"
DB_NAME="${DB_NAME:-nifi}"
DB_USER="${DB_USER:-iot}"
SITE_USER="${SITE_USER:-${ADMIN_USER:-humi}}"

# 参数：1 个 = 新密码；2 个 = 账号 新密码
if [[ $# -ge 2 ]]; then
  TARGET_USER="$1"; NEW_PASS="$2"
elif [[ $# -eq 1 ]]; then
  TARGET_USER="$SITE_USER"; NEW_PASS="$1"
else
  TARGET_USER="$SITE_USER"
  echo "给账号「$TARGET_USER」设置新密码（输入时不回显）"
  read -rsp "  新密码: " NEW_PASS; echo
  read -rsp "  再输一次: " _AGAIN; echo
  [[ -n "$NEW_PASS" ]] || { err "密码不能为空"; exit 1; }
  [[ "$NEW_PASS" == "$_AGAIN" ]] || { err "两次输入不一致，已取消"; exit 1; }
fi

[[ -n "${NEW_PASS:-}" ]] || { err "密码不能为空"; exit 1; }
[[ -n "${DB_PASS:-}" ]] || { err ".deploy-secrets 里没有 DB_PASS，无法连数据库"; exit 1; }

VENV="$CODE_DIR/v1-backend/.venv"
[[ -x "$VENV/bin/python" ]] || { err "找不到 $VENV —— 请先跑部署（或先重跑一次部署包）"; exit 1; }

# 是否管理员：沿用 config.env 的 SITE_IS_ADMIN
ADMIN_FLAG=""
[[ "${SITE_IS_ADMIN:-true}" == "true" ]] && ADMIN_FLAG="--admin"

# 注意：这里【不带】--keep-password，就是要显式重置密码
# 切用户统一走 as_user（runuser → sudo → su）：以前写死 sudo -u，没装 sudo 的机器会失败
source "$KIT_DIR/tools/lib.sh"
OUT="$(as_user env CODE_DIR="$CODE_DIR" DB_HOST=127.0.0.1 DB_PORT=3306 \
  DB_USER="$DB_USER" DB_PASS="$DB_PASS" DB_NAME="$DB_NAME" IN_DATA_BASE_DIR="$APP_HOME" \
  "$VENV/bin/python" "$KIT_DIR/tools/userctl.py" create "$TARGET_USER" "$NEW_PASS" \
  $ADMIN_FLAG 2>&1)"
RC=$?
echo "$OUT"
[[ "$RC" -eq 0 ]] || { err "修改失败（见上方输出）"; exit 1; }

log "账号「$TARGET_USER」的密码已更新，立即生效"

# 【必须同步】verify.sh / smoke_test.sh 是拿 config.env 的 SITE_PASS（缺省回退
# .deploy-secrets 的 ADMIN_PASS）去登录做自检的。只改数据库不同步这两个文件，
# 改完密码后自检立刻报「[FAIL] 登录失败」，看着像部署坏了（2026-09-17 目标机实测踩到）。
if [[ "$TARGET_USER" == "$SITE_USER" ]]; then
  # 顺带刷新已渲染的 systemd override.conf 里那行 IOT_ADMIN_PASSWORD（免得旧口令长期明文留在 /etc）
  OVR_CONF="/etc/systemd/system/iot-backend.service.d/override.conf"
  OVR_ARG=()
  [[ -f "$OVR_CONF" ]] && OVR_ARG=(--override-conf "$OVR_CONF")
  python3 "$KIT_DIR/tools/sync_site_password.py" "$KIT_DIR" "$NEW_PASS" --site-user "$SITE_USER" \
    ${OVR_ARG[@]+"${OVR_ARG[@]}"} \
    || warn "密码已改，但没能同步到 config.env/.deploy-secrets → verify.sh 会报登录失败，请手工同步"
fi

echo
echo "  登录地址  http://<服务器IP>:${FRONTEND_PORT:-5174}"
echo "  账号      $TARGET_USER"
echo
warn "请把新密码记录下来：本脚本不在屏幕上回显明文（只写进 config.env/.deploy-secrets 供自检用）。"
