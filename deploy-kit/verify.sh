#!/usr/bin/env bash
# ============================================================
# 部署自检：服务 / 端口 / 登录 / 内部管理页 / 租户隔离
# 用法: sudo ./verify.sh
# ============================================================
set -uo pipefail

KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -f "$KIT_DIR/config.env" ]] || { echo "[err] 缺少 config.env"; exit 1; }
set -a; source "$KIT_DIR/config.env"; source "$KIT_DIR/.deploy-secrets" 2>/dev/null || true; set +a

APP_USER="${APP_USER:-yhz}"
APP_HOME="${APP_HOME:-/home/$APP_USER}"
CODE_DIR="${CODE_DIR:-$APP_HOME/iot}"
FRONTEND_PORT="${FRONTEND_PORT:-5174}"
BACKEND_PORT="${BACKEND_PORT:-8081}"
ADMIN_USER="${ADMIN_USER:-humi}"
# 单账号模式：优先用本站账号登录校验
SITE_USER="${SITE_USER:-$ADMIN_USER}"
SITE_PASS="${SITE_PASS:-$ADMIN_PASS}"
VENV="$CODE_DIR/v1-backend/.venv"

PASS=0; FAIL=0
ok()   { echo -e "  \033[0;32m[PASS]\033[0m $*"; PASS=$((PASS+1)); }
bad()  { echo -e "  \033[0;31m[FAIL]\033[0m $*"; FAIL=$((FAIL+1)); }
info() { echo -e "\033[0;33m== $* ==\033[0m"; }

info "1. systemd 服务"
for s in iot-backend iot-frontend; do
  systemctl is-active --quiet "$s" && ok "$s 运行中" || bad "$s 未运行"
done

info "2. 端口"
SS_OUT="$(ss -lntp 2>/dev/null || true)"
for p in "$BACKEND_PORT" "$FRONTEND_PORT"; do
  # 不能写 `ss -lntp | grep -q`：pipefail 下 grep -q 命中即退出 → ss 还在写就吃 SIGPIPE（rc=141）→ 假报「未监听」
  grep -q ":$p " <<<"$SS_OUT" && ok "端口 $p 监听中" || bad "端口 $p 未监听"
done

info "3. HTTP 可达性"
curl -sf -o /dev/null "http://127.0.0.1:${BACKEND_PORT}/docs" && ok "后端 /docs" || bad "后端不可达"
curl -sf -o /dev/null "http://127.0.0.1:${FRONTEND_PORT}/" && ok "前端首页" || bad "前端不可达"

info "4. 本站账号登录（$SITE_USER）"
CJ="$(mktemp)"
if curl -sf -c "$CJ" -X POST "http://127.0.0.1:${BACKEND_PORT}/api/v1/auth/login" \
     -H 'Content-Type: application/json' \
     -d "{\"username\":\"${SITE_USER}\",\"password\":\"${SITE_PASS}\"}" >/dev/null 2>&1; then
  ok "账号 ${SITE_USER} 登录成功"
  code=$(curl -s -b "$CJ" -o /dev/null -w '%{http_code}' "http://127.0.0.1:${BACKEND_PORT}/api/v1/internal/users")
  if [[ "${SITE_IS_ADMIN:-true}" == "true" ]]; then
    [[ "$code" == "200" ]] && ok "内部管理页可访问 (200)" || bad "内部管理页返回 $code（管理员应能访问）"
    n=$(curl -s -b "$CJ" "http://127.0.0.1:${BACKEND_PORT}/api/v1/internal/users" \
        | python3 -c 'import sys,json;print(json.load(sys.stdin).get("data",{}).get("total","?"))' 2>/dev/null)
    echo "       用户总数: $n（单账号模式应为 1）"
  else
    [[ "$code" == "403" ]] && ok "普通用户被正确拦截 (403)" || bad "普通用户竟能访问内部管理页 ($code)"
  fi
else
  bad "登录失败（检查 .deploy-secrets 里的密码是否与实际一致）"
fi
rm -f "$CJ"

info "5. 自助注册开关"
if [[ "${ALLOW_SELF_REGISTER:-false}" == "false" ]]; then
  echo "       已关闭（正确：只能管理员开户）"
else
  bad "自助注册仍处于开启状态，任何人都能开户"
fi

info "6. NiFi 容器"
PS_OUT="$(command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' 2>/dev/null || true)"
if grep -q '^iot-nifi$' <<<"$PS_OUT"; then
  ok "iot-nifi 运行中"
  LS_OUT="$(docker exec iot-nifi ls /opt/nifi/nifi-current/data/iot/bin/ 2>/dev/null || true)"
  grep -q 'worker.py' <<<"$LS_OUT" \
    && ok "worker 脚本已部署" || bad "容器内缺少 worker 脚本"
else
  echo "       NiFi 未运行（若未启用可忽略）"
fi

info "7. 用户数据目录"
if [[ -x "$VENV/bin/python" ]]; then
  # 切用户统一走 as_user（runuser → sudo → su）：以前写死 sudo -u，没装 sudo 的机器会假失败
  source "$KIT_DIR/tools/lib.sh"
  as_user env CODE_DIR="$CODE_DIR" DB_HOST=127.0.0.1 DB_PORT=3306 \
    DB_USER="${DB_USER:-iot}" DB_PASS="$DB_PASS" DB_NAME="${DB_NAME:-nifi}" \
    IN_DATA_BASE_DIR="$APP_HOME" \
    "$VENV/bin/python" "$KIT_DIR/tools/userctl.py" list 2>/dev/null || bad "读取用户列表失败"
fi

echo
echo "==================== 结果: PASS=$PASS FAIL=$FAIL ===================="
[[ $FAIL -eq 0 ]] || exit 1
