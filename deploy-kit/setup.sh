#!/usr/bin/env bash
# ============================================================
# 部署向导 —— 不懂技术的人也能用
#
#   sudo ./setup.sh
#
# 只回答 4 个问题；基础软件是否缺失由脚本自动检测，
# 缺了就在部署阶段自动安装，不缺就跳过。
# ============================================================
set -euo pipefail

KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="$KIT_DIR/config.env"

C_G='\033[0;32m'; C_Y='\033[0;33m'; C_B='\033[0;36m'; C_R='\033[0;31m'; C_OFF='\033[0m'
ask() { echo -e "${C_B}$1${C_OFF}"; }

echo "======================================"
echo "  IoT 智慧平台 · 部署向导"
echo "======================================"
echo "（直接回车 = 使用方括号里的默认值）"
echo

# ---------- 0. 先自动判断这台机器能不能装（不用人去看体检报告） ----------
# 体检脚本会检查内存/磁盘/CPU架构/Python版本/端口冲突，并给出机器可读的结论。
# 通不过就【先不问问题】，直接告诉人家差什么 —— 免得答完 4 个问题才失败。
echo -e "${C_Y}[体检] 正在自动检查这台机器能不能部署...${C_OFF}"
if ! bash "$KIT_DIR/check_env.sh" --quiet; then
  echo
  echo -e "${C_R}这台机器目前不能直接部署（原因见上面的 ✗ 项）。${C_OFF}"
  echo "  解决后重新运行：sudo ./setup.sh"
  echo "  想看完整体检报告：sudo ./check_env.sh"
  # 注意 ${_GO:-}：无人值守（stdin 是 /dev/null）时 read 失败、_GO 可能未赋值，
  # 在 set -u 下直接写 ${_GO,,} 会以 "unbound variable" 崩掉，连上面那句提示都白打。
  read -rp "  仍要继续吗？（不推荐）[y/N]: " _GO || true
  [[ "${_GO:-}" =~ ^[yY]$ ]] || exit 1
fi
echo

# ---------- 1~4. 交互提问（只在【没有现成配置】或显式 --reconfig 时才问） ----------
# 为什么：重跑是为了「把没做完的补完」，不是重新配置一遍。已经答过的问题不再问。
#   沿用判定：$CONFIG 存在且能读到 SITE_USER（或旧格式 ADMIN_USER）
#   想改配置：sudo ./setup.sh --reconfig     （等价于设 IOT_RECONFIG=1）
RECONFIG=0
for a in "$@"; do
  case "$a" in --reconfig|-r) RECONFIG=1 ;; esac
done
[[ "${IOT_RECONFIG:-0}" == "1" ]] && RECONFIG=1

# ⚠️ 末尾必须 || true：如果这台机器只有回环地址（网卡没配好、或在 --network none 的容器里），
# hostname -I 的输出会被 grep -v 全部过滤掉 → grep 退出码 1 → 脚本是 set -euo pipefail
# → 会【静默退出】在体检之后、任何提示之前（真机有真实 IP 所以一直没暴露）。
DETECTED_IP="$(hostname -I 2>/dev/null | tr ' ' '\n' | grep -vE '^$|^127\.' | head -1 || true)"
[[ -n "$DETECTED_IP" ]] || DETECTED_IP="127.0.0.1"

OLD_SITE_USER=""; OLD_SITE_IS_ADMIN=""; OLD_DOMAIN=""
if [[ -f "$CONFIG" ]]; then
  OLD_SITE_USER="$(grep -E '^SITE_USER=' "$CONFIG" | head -1 | cut -d= -f2- || true)"
  [[ -n "$OLD_SITE_USER" ]] || OLD_SITE_USER="$(grep -E '^ADMIN_USER=' "$CONFIG" | head -1 | cut -d= -f2- || true)"
  OLD_SITE_IS_ADMIN="$(grep -E '^SITE_IS_ADMIN=' "$CONFIG" | head -1 | cut -d= -f2- || true)"
  OLD_DOMAIN="$(grep -E '^DOMAIN=' "$CONFIG" | head -1 | cut -d= -f2- || true)"
fi

if [[ -n "$OLD_SITE_USER" && "$RECONFIG" != "1" ]]; then
  # ---- 重跑：沿用上次答案，一个问题都不问 ----
  SITE_USER="$OLD_SITE_USER"
  SITE_IS_ADMIN="${OLD_SITE_IS_ADMIN:-true}"
  HOST_ADDR="${OLD_DOMAIN:-$DETECTED_IP}"
  DEF_ADMIN="$SITE_IS_ADMIN"
  REUSE_CONFIG=1
  echo -e "${C_G}[复用]${C_OFF} 检测到已有配置 —— 不再询问，直接沿用上次的答案："
  echo "       账号名       : $SITE_USER"
  echo "       内部管理页   : $SITE_IS_ADMIN"
  echo "       访问地址     : $HOST_ADDR"
  echo "       登录密码     : 不变（沿用已生成的密码，重跑不会重置）"
  echo "       要改这些，用 : sudo ./setup.sh --reconfig"
else
  REUSE_CONFIG=0
  # ---------- 1. 角色 ----------
  ask "【1/4】这台机器是给谁用的？"
  echo "   1) 忽米（平台方，管理员）"
  echo "   2) 工厂（客户，普通用户）"
  read -rp "选择 [1/2，默认 1]: " ROLE
  ROLE="${ROLE:-1}"

  case "$ROLE" in
    1) DEF_USER="humi";      DEF_ADMIN="true"  ;;
    2) DEF_USER="factory01"; DEF_ADMIN="true"  ;;
    *) echo "无效选择，按忽米处理"; DEF_USER="humi"; DEF_ADMIN="true" ;;
  esac

  # ---------- 2. 账号名 ----------
  ask "【2/4】这台机器上的登录账号名叫什么？"
  echo "   （忽米一般用 humi；工厂用工厂代号，如 factory_a）"
  read -rp "账号名 [默认 $DEF_USER]: " SITE_USER
  SITE_USER="${SITE_USER:-$DEF_USER}"
  SITE_USER="$(echo "$SITE_USER" | tr -d ' /')"   # 去掉空格和斜杠，防路径穿越

  # ---------- 3. 能否进内部管理页 ----------
  ask "【3/4】这个账号可以进「内部管理页」吗？"
  echo "   - 忽米：必须选 是（要管理用户、看数据）"
  echo "   - 工厂：选 是 则该厂自己能看自己的数据，且忽米用同一账号也能远程查看"
  echo "           选 否 则纯普通用户，忽米无法远程查看这台机器"
  read -rp "可以进？[y/n，默认 $DEF_ADMIN]: " CAN_ADMIN
  CAN_ADMIN="${CAN_ADMIN:-$DEF_ADMIN}"
  case "$CAN_ADMIN" in
    y|Y|yes|true|1) SITE_IS_ADMIN="true" ;;
    *)              SITE_IS_ADMIN="false" ;;
  esac

  # ---------- 4. 访问地址 ----------
  # 自动探测本机地址，不让用户手填（回车即用）
  echo -e "${C_B}【4/4】访问地址已自动检测：${C_G}$DETECTED_IP${C_OFF}"
  echo "   （如需用域名或别的地址才需修改，直接回车就用上面这个）"
  read -rp "地址 [回车 = $DETECTED_IP]: " HOST_ADDR
  HOST_ADDR="${HOST_ADDR:-$DETECTED_IP}"
fi

# ---------- 基础软件：自动检测（不询问） ----------
echo
ask "【自动检测】基础软件"
MISSING_SW=""
for c in python3 mysql docker nginx; do
  command -v "$c" >/dev/null 2>&1 || MISSING_SW="${MISSING_SW} ${c}"
done
# python3 在但 venv 模块不在（python3-venv 是单独的包），建不了虚拟环境，也算缺
if command -v python3 >/dev/null 2>&1 \
   && ! python3 -c 'import venv, ensurepip' >/dev/null 2>&1; then
  MISSING_SW="${MISSING_SW} python3-venv"
fi
# docker 命令存在但守护进程没跑，也算不可用
if command -v docker >/dev/null 2>&1 && ! docker info >/dev/null 2>&1; then
  MISSING_SW="${MISSING_SW} docker(未运行)"
fi
# docker compose 缺了只影响 NiFi 容器，一并补上
if command -v docker >/dev/null 2>&1 \
   && ! docker compose version >/dev/null 2>&1 \
   && ! command -v docker-compose >/dev/null 2>&1; then
  MISSING_SW="${MISSING_SW} docker-compose"
fi

if [[ -n "${MISSING_SW// /}" ]]; then
  echo -e "  检测到缺少：${C_Y}${MISSING_SW}${C_OFF}"
  echo "  → 部署阶段自动安装：优先用包内离线 deb（零下载）；"
  echo "    没有对应发行版的 deb 时，自动把软件源 http:// 换成 https:// 再在线装"
  INSTALL_DEPS="true"
else
  echo -e "  基础软件齐全 ${C_G}✓${C_OFF}（无需安装，部署更快）"
  INSTALL_DEPS="false"
fi

# ---------- 生成配置 ----------
# 先备份；并沿用旧配置里「需要人工填」的两项，避免每次跑向导都把它们清掉
OLD_SITE_PASS=""; OLD_ROOT_PASS=""
if [[ -f "$CONFIG" ]]; then
  OLD_SITE_PASS="$(grep -E '^SITE_PASS=' "$CONFIG" | head -1 | cut -d= -f2- || true)"
  OLD_ROOT_PASS="$(grep -E '^MYSQL_ROOT_PASS=' "$CONFIG" | head -1 | cut -d= -f2- || true)"
  cp "$CONFIG" "${CONFIG}.bak.$(date +%s)"
  echo -e "${C_Y}[info] 已备份旧配置${C_OFF}"
  [[ -n "$OLD_SITE_PASS" ]] && echo "       沿用已填的站点密码"
  [[ -n "$OLD_ROOT_PASS" ]] && echo "       沿用已填的 MySQL root 密码"
fi

# ---------- MySQL root 登录方式：能免密就免密，不能就现场问一次 ----------
# 第 7 步要用 root 建库建用户。Ubuntu 上 apt 装的 MySQL root 走 auth_socket（免密）；
# 但若目标机的 MySQL 是别人装的、root 设了密码，这里不问就只能在部署时报错退出。
MYSQL_ROOT_PASS="${OLD_ROOT_PASS}"
if command -v mysql >/dev/null 2>&1; then
  if [[ -n "$MYSQL_ROOT_PASS" ]] \
     && mysql -u root -p"$MYSQL_ROOT_PASS" -e "SELECT 1" >/dev/null 2>&1; then
    echo -e "  MySQL root：${C_G}沿用已填密码，验证通过${C_OFF}"
  elif mysql -u root -e "SELECT 1" >/dev/null 2>&1; then
    MYSQL_ROOT_PASS=""
    echo -e "  MySQL root：${C_G}可免密登录（socket 认证）${C_OFF}"
  else
    echo
    echo -e "  ${C_Y}MySQL root 不能免密登录${C_OFF}（这台机器的 root 设了密码）"
    echo "  不填也能继续，但部署到「初始化数据库」那步会失败。"
    read -rsp "  请输入 MySQL root 密码 [直接回车 = 先跳过]: " _MP || true
    echo
    if [[ -n "$_MP" ]] && mysql -u root -p"$_MP" -e "SELECT 1" >/dev/null 2>&1; then
      MYSQL_ROOT_PASS="$_MP"
      echo -e "  ${C_G}密码验证通过${C_OFF}"
    elif [[ -n "$_MP" ]]; then
      MYSQL_ROOT_PASS="$_MP"
      echo -e "  ${C_Y}密码验证未通过，仍写入配置（可稍后手改 config.env）${C_OFF}"
    else
      MYSQL_ROOT_PASS=""
      echo -e "  ${C_Y}已跳过（稍后可手改 config.env 的 MYSQL_ROOT_PASS）${C_OFF}"
    fi
  fi
else
  echo "  MySQL 未安装（本次会自动安装，root 默认免密）"
fi

cat > "$CONFIG" <<EOF
# 由 setup.sh 于 $(date '+%F %T') 生成
APP_USER=yhz
APP_HOME=/home/yhz
CODE_DIR=/home/yhz/iot

GIT_REPO=
GIT_BRANCH=main

FRONTEND_PORT=5174
BACKEND_PORT=8081
NIFI_PORT=8080
FRONTEND_BIND=0.0.0.0
BACKEND_BIND=127.0.0.1

DB_NAME=nifi
DB_USER=iot
DB_PASS=
MYSQL_ROOT_PASS=${MYSQL_ROOT_PASS}

# ---- 本站唯一账号 ----
SITE_USER=${SITE_USER}
SITE_IS_ADMIN=${SITE_IS_ADMIN}
SITE_PASS=${OLD_SITE_PASS}
ADMIN_USER=${SITE_USER}
ADMIN_PASS=

SESSION_TTL=86400

DOMAIN=${HOST_ADDR}
NIFI_PROXY_HOST=${HOST_ADDR}:8080,127.0.0.1:8080,localhost:8080
INSTALL_NGINX=true

APPLY_NO_DEFAULT_ADMIN_PATCH=true
APPLY_SELF_REGISTER_PATCH=true
DISABLE_REGISTER_PAGE=true
ALLOW_SELF_REGISTER=false

INSTALL_SYSTEM_DEPS=${INSTALL_DEPS}
INSTALL_ODBC=true
APPLY_PRIVATE_STORAGE_PATCH=false
PRIVATE_DATA_ROOT=
INSTALL_NIFI=true
EOF

echo
echo "======================================"
echo -e "${C_G}配置已就绪${C_OFF}"
echo "  账号名      : $SITE_USER"
echo "  内部管理页  : $SITE_IS_ADMIN"
echo "  访问地址    : $HOST_ADDR"
echo "  基础软件    : $([[ "$INSTALL_DEPS" == "true" ]] && echo "缺，部署时自动安装" || echo "已齐全")"
echo "  MySQL root  : $([[ -n "$MYSQL_ROOT_PASS" ]] && echo "用密码登录（已填）" || echo "免密登录")"
echo "  登录密码    : 不变（已生成的密码不会被重跑重置）"
echo "======================================"
echo

# 供自动化测试用：只写配置、不真部署
if [[ "${IOT_SETUP_DRYRUN:-0}" == "1" ]]; then
  echo "[dryrun] 已写出配置，按 IOT_SETUP_DRYRUN=1 要求不执行部署"
  exit 0
fi

if [[ "$REUSE_CONFIG" == "1" ]]; then
  # 重跑场景：不再多问一句，直接补齐没做完的部分
  echo "同上次配置，直接开始补齐/部署（不询问）..."
  exec sudo "$KIT_DIR/deploy.sh"
fi

read -rp "现在开始部署吗？[y/n，默认 y]: " GO
GO="${GO:-y}"
case "$GO" in
  y|Y|yes)
    exec sudo "$KIT_DIR/deploy.sh"
    ;;
  *)
    echo "已取消。稍后手工执行：sudo ./deploy.sh"
    ;;
esac
