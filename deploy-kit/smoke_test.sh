#!/usr/bin/env bash
# ============================================================
# 功能冒烟测试 —— 确认部署出来的系统「各个功能都能用」
#
# 与 verify.sh 的区别：
#   verify.sh    = 部署是否成功（服务/端口/登录）
#   smoke_test.sh= 业务功能是否可用（上传/转换/打标/权限）
#
# 重要：所有请求都走【前端端口 5174】（serve.py 代理 /api/v1），
#       不直接打后端 8081 —— 这样同时验证了前端代理链路。
#
# 用法: sudo ./smoke_test.sh
# ============================================================
set -uo pipefail

KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -f "$KIT_DIR/config.env" ]] || { echo "[err] 缺少 config.env"; exit 1; }
set -a; source "$KIT_DIR/config.env"; source "$KIT_DIR/.deploy-secrets" 2>/dev/null || true; set +a

APP_USER="${APP_USER:-yhz}"
APP_HOME="${APP_HOME:-/home/$APP_USER}"
CODE_DIR="${CODE_DIR:-$APP_HOME/iot}"
FRONTEND_PORT="${FRONTEND_PORT:-5174}"
ADMIN_USER="${ADMIN_USER:-humi}"
SITE_USER="${SITE_USER:-$ADMIN_USER}"
SITE_PASS="${SITE_PASS:-$ADMIN_PASS}"
SITE_IS_ADMIN="${SITE_IS_ADMIN:-true}"

BASE="http://127.0.0.1:${FRONTEND_PORT}"   # 走前端，不走后端
SAMPLE="$KIT_DIR/testdata/sample.csv"

PASS=0; FAIL=0; WARN=0
ok()   { echo -e "  \033[0;32m[PASS]\033[0m $*"; PASS=$((PASS+1)); }
bad()  { echo -e "  \033[0;31m[FAIL]\033[0m $*"; FAIL=$((FAIL+1)); }
warn() { echo -e "  \033[0;33m[WARN]\033[0m $*"; WARN=$((WARN+1)); }
info() { echo -e "\033[0;33m== $* ==\033[0m"; }

CJ="$(mktemp)"; trap 'rm -f "$CJ"' EXIT
[[ -f "$SAMPLE" ]] || { echo "[err] 缺少测试数据 $SAMPLE"; exit 1; }

info "1. 前端与代理"
curl -sf -o /dev/null "$BASE/" && ok "前端首页可访问" || bad "前端首页不可达"
# 未登录访问受保护接口应被拒（证明代理确实把请求送到了后端）
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/v1/internal/users")
[[ "$code" == "401" || "$code" == "403" ]] \
  && ok "代理链路正常（未登录访问内部接口返回 $code）" \
  || warn "未登录访问内部接口返回 $code（预期 401/403）"

info "2. 登录（走前端）"
if curl -sf -c "$CJ" -X POST "$BASE/api/v1/auth/login" \
     -H 'Content-Type: application/json' \
     -d "{\"username\":\"${SITE_USER}\",\"password\":\"${SITE_PASS}\"}" >/dev/null 2>&1; then
  ok "账号 ${SITE_USER} 登录成功"
else
  bad "登录失败，后续测试无法进行"; exit 1
fi

info "3. 上传文件（走前端）"
UP=$(curl -s -b "$CJ" -X POST \
      "$BASE/api/v1/upload/inbox_csv?username=${SITE_USER}&convertType=csv_to_json" \
      -F "file=@${SAMPLE}" 2>&1)
if grep -qiE '"success"\s*:\s*true|"code"\s*:\s*0|filename|file_id' <<<"$UP"; then
  ok "CSV 上传成功"
else
  bad "CSV 上传失败: $(echo "$UP" | head -c 200)"
fi

info "4. 文件列表接口"
code=$(curl -s -b "$CJ" -o /dev/null -w '%{http_code}' "$BASE/api/v1/files?pageNo=1&pageSize=10")
[[ "$code" == "200" ]] && ok "文件列表接口返回 200" || warn "文件列表接口返回 $code"

info "5. 用户目录结构"
UD="$APP_HOME/$SITE_USER"
if [[ -d "$UD" ]]; then
  ok "用户目录存在: $UD"
  miss=0
  for d in nifi-data real_nifi_data tagged_nifi_data tagged_real_nifi_data; do
    [[ -d "$UD/$d" ]] || { warn "缺少数据根 $d"; miss=$((miss+1)); }
  done
  [[ $miss -eq 0 ]] && ok "4 个数据根齐全"
else
  bad "用户目录不存在: $UD"
fi

info "6. 上传文件是否落盘"
found=$(find "$UD" -name 'raw_*' -o -name '*sample*' 2>/dev/null | head -3)
if [[ -n "$found" ]]; then
  ok "上传文件已落盘:"
  echo "$found" | sed 's/^/       /'
else
  warn "未在用户目录下找到上传文件（可能落在全局 NiFi 目录，需人工确认）"
fi

info "7. 内部管理页权限（角色=$SITE_IS_ADMIN）"
code=$(curl -s -b "$CJ" -o /dev/null -w '%{http_code}' "$BASE/api/v1/internal/users")
if [[ "$SITE_IS_ADMIN" == "true" ]]; then
  [[ "$code" == "200" ]] && ok "管理员可访问内部管理接口 (200)" || bad "管理员访问内部接口返回 $code"
  n=$(curl -s -b "$CJ" "$BASE/api/v1/internal/users" \
      | python3 -c 'import sys,json;print(json.load(sys.stdin).get("data",{}).get("total","?"))' 2>/dev/null)
  if [[ "$n" == "1" ]]; then
    ok "用户总数为 1（单账号模式正确）"
  else
    warn "用户总数为 $n（单账号模式应为 1）→ 多半是「禁用默认 admin」补丁没打上"
    warn "  自查：grep -c 'no-default-admin' ${CODE_DIR}/v1-backend/app/db_models.py  # 应为 1"
    warn "  处理：python3 ${KIT_DIR}/tools/patch_no_default_admin.py ${CODE_DIR} 后重启后端"
  fi
else
  [[ "$code" == "403" ]] && ok "普通用户被正确拦截 (403)" || bad "普通用户竟能访问内部接口 ($code)"
fi

info "8. 自动打标接口（可选，依赖 NiFi/规则配置）"
code=$(curl -s -b "$CJ" -o /dev/null -w '%{http_code}' \
        -X POST "$BASE/api/v1/tags/auto" -H 'Content-Type: application/json' -d '{}')
case "$code" in
  200) ok "自动打标接口响应 200" ;;
  400) warn "自动打标返回 400（多半是缺少参数/规则，非部署问题）" ;;
  *)   warn "自动打标返回 $code（该功能依赖 NiFi 与打标规则配置）" ;;
esac

echo
echo "================= 冒烟测试结果: PASS=$PASS FAIL=$FAIL WARN=$WARN ================="
echo "说明: WARN 多为依赖 NiFi/业务配置的可选项，不一定是部署问题。"
[[ $FAIL -eq 0 ]]
