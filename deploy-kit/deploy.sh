#!/usr/bin/env bash
# ============================================================
# IoT 智慧平台 一键部署脚本（可复用）
#
#   - 部署「忽米平台」（管理员 + 多工厂租户）
#   - 部署「工厂独立站点」（单套实例）
#   两者流程完全一致，只是 config.env 参数不同、执行次数不同。
#
# 用法:
#   cp config.env.example config.env && vi config.env
#   sudo ./deploy.sh
#
# 幂等：可重复执行，会覆盖配置并重启服务。
# ============================================================
set -euo pipefail

KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${KIT_DIR}/config.env"
SECRETS_FILE="${KIT_DIR}/.deploy-secrets"

# 注意：这里必须用 $'...' 定义（即真正的 ESC 字符），不能用 '\033...' 字面量。
# 原因：末尾的汇总块是 `cat <<EOF`（不是 echo -e），字面量 \033 会被原样打印出来，
#       用户看到的就是一堆 "\033[0;32m" 乱码。
C_GREEN=$'\033[0;32m'; C_YELLOW=$'\033[0;33m'; C_RED=$'\033[0;31m'; C_OFF=$'\033[0m'
log()  { echo -e "${C_GREEN}[deploy]${C_OFF} $*"; }
warn() { echo -e "${C_YELLOW}[warn]${C_OFF} $*"; }
err()  { echo -e "${C_RED}[err]${C_OFF} $*" >&2; }
die()  { err "$*"; exit 1; }

# 收集「不影响部署继续，但需要人工关注」的事项，最后统一汇总，
# 避免刷在几十行日志里被忽略。
WARNINGS=()
note_warn() { warn "$*"; WARNINGS+=("$*"); }

# ---------- 归属台账 ----------
# 记录「哪些东西是本平台装的 / 改的」，供：
#   · 部署期判定：机器上某个残留学（如旧版留下的 libodbc1）是本平台的 → 可以放心打破、按本包重建
#   · 卸载期回收：只卸我们装过的，机器原本就有的绝不动
# 位置 /var/lib/iot-platform/，查看：bash tools/manifest.sh show
# 全程 || true —— 台账只是审计信息，绝不能因为写不进去而中断部署。
MANIFEST="$KIT_DIR/tools/manifest.sh"
record() { bash "$MANIFEST" "$@" >/dev/null 2>&1 || true; }

# ---------- 以运行用户身份执行命令 ----------
# 定义统一收在 tools/lib.sh（runuser → sudo → su 三级回退）。
# 【2026-09-17 修复】以前这段在这里写死，其中 `elif 有 sudo` 分支误写成
# `as_user "$@"`（调用自己）→ 没有 runuser 的机器上会**无限递归**卡死。
# 现在只保留一份定义，并加 e2e 静态断言防复发。
[[ -f "$KIT_DIR/tools/lib.sh" ]] || die "缺少 $KIT_DIR/tools/lib.sh（包不完整）"
source "$KIT_DIR/tools/lib.sh"

# ---------- 0. 环境体检（自动判定，不通过就停止） ----------
# 缺软件不算阻塞（后面会自动装）；内存/磁盘/CPU架构/Python版本/端口冲突才是阻塞项。
# 这里【不需要人去读报告】：体检脚本退出码非 0 就明确停下来，
# 避免装到一半才因为内存不足/端口被占而失败。
log "0/14 环境体检（自动判定）"
if ! bash "$KIT_DIR/check_env.sh" --quiet; then
  die "环境体检不通过，部署已中止（原因见上面的 ✗ 项；解决后重跑即可）"
fi

# ---------- 1. 前置检查 ----------
log "1/14 前置检查"
[[ $EUID -eq 0 ]] || die "请用 root 运行：sudo ./deploy.sh"
[[ -f "$CONFIG_FILE" ]] || die "缺少 $CONFIG_FILE（请先 cp config.env.example config.env 并修改）"

# shellcheck disable=SC1090
set -a; source "$CONFIG_FILE"; set +a

APP_USER="${APP_USER:-yhz}"
APP_HOME="${APP_HOME:-/home/$APP_USER}"
CODE_DIR="${CODE_DIR:-$APP_HOME/iot}"
DB_NAME="${DB_NAME:-nifi}"
DB_USER="${DB_USER:-iot}"
FRONTEND_PORT="${FRONTEND_PORT:-5174}"
BACKEND_PORT="${BACKEND_PORT:-8081}"
NIFI_PORT="${NIFI_PORT:-8080}"
# 前端要能被别的机器用浏览器打开（http://<服务器IP>:5174），所以默认绑 0.0.0.0；
# 后端保持只绑回环，对外只通过 serve.py 的 /api/v1 代理暴露。
# 注意：若前端也绑 127.0.0.1，那么只有本机（或 Nginx 的 80 端口）能访问，
#       而很多内网/防火墙会挡 80，站点就彻底打不开了。
FRONTEND_BIND="${FRONTEND_BIND:-0.0.0.0}"
BACKEND_BIND="${BACKEND_BIND:-127.0.0.1}"
SESSION_TTL="${SESSION_TTL:-86400}"
ALLOW_SELF_REGISTER="${ALLOW_SELF_REGISTER:-false}"
DOMAIN="${DOMAIN:-_}"
NIFI_PROXY_HOST="${NIFI_PROXY_HOST:-127.0.0.1:${NIFI_PORT},localhost:${NIFI_PORT}}"
INSTALL_NGINX="${INSTALL_NGINX:-true}"
INSTALL_NIFI="${INSTALL_NIFI:-true}"
APPLY_PRIVATE_STORAGE_PATCH="${APPLY_PRIVATE_STORAGE_PATCH:-false}"
PRIVATE_DATA_ROOT="${PRIVATE_DATA_ROOT:-$APP_HOME}"
ADMIN_USER="${ADMIN_USER:-humi}"

# 每套实例只建「一个」账号：
#   忽米机器 -> SITE_USER=humi,      SITE_IS_ADMIN=true
#   工厂机器 -> SITE_USER=factory_a, SITE_IS_ADMIN=true(可进内部管理页) 或 false
# SITE_USER 不填时兼容旧的 ADMIN_USER
SITE_USER="${SITE_USER:-$ADMIN_USER}"
SITE_IS_ADMIN="${SITE_IS_ADMIN:-true}"

for cmd in python3 mysql docker; do
  command -v "$cmd" >/dev/null 2>&1 || warn "未找到命令 $cmd（相关步骤会跳过或失败）"
done

# 可选：自动安装系统依赖（新机器建议开；已装好的机器可关以免动到现有环境）
# 交给 install_sysdeps.sh，顺序：
#   ① 软件已齐全 → 直接跳过
#   ② 包内备有本发行版的离线 deb → dpkg 离线安装（零下载）
#   ③ 都没有 → 自动把 apt 源 http:// 换成 https:// 再装
#      （实测很多内网只放行 443，80 被挡会让 apt 连 security.ubuntu.com 全部超时）
if [[ "${INSTALL_SYSTEM_DEPS:-false}" == "true" ]]; then
  log "安装系统依赖（INSTALL_SYSTEM_DEPS=true）"
  bash "$KIT_DIR/install_sysdeps.sh" || warn "系统依赖存在未完成项，请查看上方提示"
fi

# ---------- 2. 密钥与口令 ----------
log "2/14 生成/读取密钥与口令"
gen() { head -c 32 /dev/urandom | base64 | tr -d '\n'; }

if [[ -f "$SECRETS_FILE" ]]; then
  set -a; source "$SECRETS_FILE"; set +a
  log "复用已有密钥 $SECRETS_FILE"
else
  DB_PASS="${DB_PASS:-$(head -c 18 /dev/urandom | base64 | tr -d '\n')}"
  ADMIN_PASS="${ADMIN_PASS:-$(head -c 14 /dev/urandom | base64 | tr -d '\n')}"
  SECRET_KEY="${SECRET_KEY:-$(gen)}"
  META_KEY="${META_KEY:-$(gen)}"
  cat > "$SECRETS_FILE" <<EOF
DB_PASS=$DB_PASS
ADMIN_PASS=$ADMIN_PASS
SECRET_KEY=$SECRET_KEY
META_KEY=$META_KEY
EOF
  chmod 600 "$SECRETS_FILE"
  log "已生成密钥 -> $SECRETS_FILE (权限 600，请妥善保管)"
fi
SECRET_KEY="${SECRET_KEY:-$(gen)}"
META_KEY="${META_KEY:-$(gen)}"

# 本站账号密码：config.env 里 SITE_PASS 留空是常态（setup.sh 不问答），
# 这里必须兜底成随机口令，否则会拿空密码去建号，且界面提示的密码与实际不符。
SITE_PASS="${SITE_PASS:-$ADMIN_PASS}"
[[ -n "$SITE_PASS" ]] || die "SITE_PASS/ADMIN_PASS 都为空，无法创建账号"

# ---------- 3. 用户与目录 ----------
log "3/14 准备运行用户与目录"
if ! id -u "$APP_USER" >/dev/null 2>&1; then
  useradd -m -s /bin/bash "$APP_USER"
  log "已创建用户 $APP_USER"
fi
mkdir -p "$CODE_DIR" "$APP_HOME/real_nifi_data" "$PRIVATE_DATA_ROOT"
record record_dir "$CODE_DIR" "$APP_HOME/real_nifi_data"
# PRIVATE_DATA_ROOT 默认就等于家目录 —— 那种情况【不能】记成「本平台创建的目录」，
# 否则卸载时可能去动家目录（虽然只删空目录，但记错归属本身就是错的）。
[[ "$PRIVATE_DATA_ROOT" != "$APP_HOME" ]] && record record_dir "$PRIVATE_DATA_ROOT"
chown -R "$APP_USER":"$APP_USER" "$APP_HOME" "$CODE_DIR" 2>/dev/null || true

# ---------- 4. 获取代码 ----------
log "4/14 获取代码"
if [[ -n "${GIT_REPO:-}" ]]; then
  if [[ -d "$CODE_DIR/.git" ]]; then
    log "更新已有仓库"
    as_user git -C "$CODE_DIR" fetch --all --prune
    as_user git -C "$CODE_DIR" checkout "${GIT_BRANCH:-main}"
    as_user git -C "$CODE_DIR" pull --ff-only || warn "git pull 失败，沿用现有代码"
  else
    as_user git clone -b "${GIT_BRANCH:-main}" "$GIT_REPO" "$CODE_DIR"
  fi
else
  [[ -f "$CODE_DIR/v1-backend/app/main.py" ]] || die "未配置 GIT_REPO 且 $CODE_DIR 下没有代码"
  warn "GIT_REPO 为空，使用现有代码 $CODE_DIR"
fi
mkdir -p "$CODE_DIR/v1-backend/data"

# ---------- 5. 代码补丁（方案一的关键，默认开启）----------
log "5/14 代码补丁"

# 5.0 禁止后端自动创建默认 admin
#     init_db() 会插入 admin（密码取 IOT_ADMIN_PASSWORD，历史默认 123456）。
#     不拦掉的话：① 与「一套实例=一个账号」冲突，内部管理页会多出 admin；
#                 ② 弱口令风险。账号统一由第 11 步 userctl.py 显式创建。
#     注意：补丁要匹配 db_models.py 里的代码片段，代码演进后可能匹配失败 →
#     这里必须 || warn，不能让 set -e 把整个部署中断。
if [[ "${APPLY_NO_DEFAULT_ADMIN_PATCH:-true}" == "true" ]]; then
  python3 "$KIT_DIR/tools/patch_no_default_admin.py" "$CODE_DIR" \
    || note_warn "「禁用默认 admin」补丁未生效（代码片段可能已变化）→ 实例会多出一个 admin 账号"
else
  note_warn "未打「禁用默认 admin」补丁 → 实例会多出一个 admin 账号（密码=IOT_ADMIN_PASSWORD）"
fi

# 5.1 自助注册开关 —— 不打这个补丁，IOT_ALLOW_SELF_REGISTER 配了也没用，注册仍对外开放
if [[ "${APPLY_SELF_REGISTER_PATCH:-true}" == "true" ]]; then
  python3 "$KIT_DIR/tools/patch_disable_self_register.py" "$CODE_DIR" \
    || note_warn "自助注册开关补丁未生效 → IOT_ALLOW_SELF_REGISTER 不起作用，任何人仍可自助开户（安全隐患）"
else
  note_warn "未打自助注册开关补丁 → 任何人都能自助开户"
fi

# 5.2 下线前端注册入口（配合 5.1，只关后端会让用户看到报错入口）
if [[ "${DISABLE_REGISTER_PAGE:-true}" == "true" ]]; then
  python3 "$KIT_DIR/tools/disable_register_page.py" "$CODE_DIR" \
    || note_warn "前端注册页未成功下线 → 用户仍能看到注册入口"
else
  note_warn "保留前端注册页（DISABLE_REGISTER_PAGE=false）"
fi

# 5.3 私有化存储（可选）
if [[ "$APPLY_PRIVATE_STORAGE_PATCH" == "true" ]]; then
  python3 "$KIT_DIR/tools/patch_private_storage.py" "$CODE_DIR" \
    || note_warn "私有化存储补丁未生效 → 私有化用户数据仍落在本地盘"
else
  log "未启用私有化存储补丁（APPLY_PRIVATE_STORAGE_PATCH=false，属正常）"
fi

# ---------- 6. Python 环境 ----------
log "6/14 Python 虚拟环境与依赖"
VENV="$CODE_DIR/v1-backend/.venv"

# 选解释器：优先挑「离线 wheel 有对应版本目录」的 python3.13 → 3.10，都找不到才退回系统 python3。
# 为什么不能直接用 python3：
#   · 机器上可能装了多个 Python（如 conda + 系统），python3 未必是能用的那个；
#   · Ubuntu 20.04 的 python3 是 3.8，而本平台要求 ≥3.9（pydantic 2.11 的硬要求），
#     直接用它建 venv 会「装依赖时才发现不匹配」，报错很晚也很难懂。
PY_BIN=""; PY_VER="?"
for v in 3.13 3.12 3.11 3.10; do
  [[ -d "$KIT_DIR/offline-assets/wheels/py${v//./}" ]] || continue
  cand=""
  # 优先发行版自带的，其次 PATH 里的
  [[ -x "/usr/bin/python$v" ]] && cand="/usr/bin/python$v" || cand="$(command -v "python$v" || true)"
  if [[ -n "$cand" ]]; then PY_BIN="$cand"; PY_VER="$v"; break; fi
done
if [[ -z "$PY_BIN" ]]; then
  PY_BIN="$(command -v python3 || true)"
  [[ -n "$PY_BIN" ]] && PY_VER="$("$PY_BIN" -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null || echo '?')"
fi
[[ -n "$PY_BIN" ]] || die "找不到 python3，无法创建虚拟环境"
log "使用解释器：$PY_BIN（Python $PY_VER）"

# Python 下限：pydantic 2.11 要求 >=3.9，低于它后端必然起不来
if ! "$PY_BIN" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)' 2>/dev/null; then
  warn "本机 Python $PY_VER 低于 3.9 —— 本平台无法运行（pydantic 2.11 要求 ≥3.9）"
  warn "  解决：安装 Python 3.10~3.13 后重跑；或改用 Ubuntu 22.04(自带3.10) / 24.04(自带3.12)"
  warn "  注：Ubuntu 20.04 自带 3.8，不适合直接部署本平台"
  note_warn "Python $PY_VER 低于 3.9，后端很可能起不来（离线 wheel 只覆盖 3.10~3.13）"
fi

if [[ ! -x "$VENV/bin/python" ]]; then
  as_user "$PY_BIN" -m venv "$VENV"
fi
# ⚠️ 这一步必须容忍失败：断网机器上升级 pip 会失败（要连 PyPI），
#    而它并非必需 —— 本平台依赖全部由包内 wheel 安装（下面用 --no-index）。
#    以前没加护栏：断网机器会在第 6 步直接中断（set -e），这是真隐患。
#    2026-09-17 再修两处观感问题（目标机实测）：断网时默认会重试 5 次、打出满屏的
#      `WARNING: Retrying ... pypi.org timed out`，白等约 75 秒，运维看着像失败。
#    → 改成只试 1 次、5 秒超时，且输出全部进日志文件（终端只在失败时给一行人话）。
PIP_LOG="${PIP_LOG:-/tmp/iot-pip.log}"
as_user "$VENV/bin/pip" install --upgrade pip -q --retries 1 --timeout 5 >>"$PIP_LOG" 2>&1 \
  || warn "升级 pip 失败（离线属正常）→ 继续用现有 pip 从离线 wheel 安装依赖（日志：$PIP_LOG）"

# 优先用本地预编译 wheel 包（离线，零编译）
# 包内按 Python 版本分目录（wheels/py310、py311...），自动挑本机版本 —— 无需预先知道版本
HAVE_PY="$("$VENV/bin/python" -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null)"
PY_NODOT="${HAVE_PY//./}"
WHEELS_DIR="$KIT_DIR/offline-assets/wheels/py${PY_NODOT}"
[[ -d "$WHEELS_DIR" ]] || WHEELS_DIR="$KIT_DIR/offline-assets/wheels"   # 兼容旧布局

if [[ -d "$WHEELS_DIR" ]] && compgen -G "$WHEELS_DIR/*.whl" >/dev/null 2>&1; then
  log "本机 Python $HAVE_PY → 使用离线 wheel：$(basename "$WHEELS_DIR")"
  # CPU 架构一致性
  if [[ -f "$KIT_DIR/offline-assets/ARCH" ]]; then
    WANT_ARCH="$(cat "$KIT_DIR/offline-assets/ARCH")"
    HAVE_ARCH="$(uname -m)"
    if [[ "$WANT_ARCH" != "$HAVE_ARCH" ]]; then
      warn "CPU 架构不匹配：离线 wheel 为 $WANT_ARCH，本机 $HAVE_ARCH —— 可能安装失败"
    else
      log "CPU 架构一致（$HAVE_ARCH）"
    fi
  fi
  if ! as_user "$VENV/bin/pip" install -q --no-index --find-links "$WHEELS_DIR" \
        -r "$KIT_DIR/templates/requirements-runtime.txt"; then
    warn "离线安装失败，回退到联网安装（若本机无外网，后端将无法启动）"
    as_user "$VENV/bin/pip" install -q -r "$KIT_DIR/templates/requirements-runtime.txt" \
      || warn "依赖安装失败"
  fi
else
  warn "包内没有与 Python $HAVE_PY 匹配的离线 wheel"
  warn "  已有版本目录：$(find "$KIT_DIR/offline-assets/wheels" -maxdepth 1 -type d -name 'py*' -printf '%f ' 2>/dev/null)"
  warn "  回退到联网安装（需能访问 PyPI）"
  as_user "$VENV/bin/pip" install -q -r "$KIT_DIR/templates/requirements-runtime.txt" \
    || warn "依赖安装失败"
fi

# ---------- 7. 数据库 ----------
log "7/14 初始化 MySQL"
render() { python3 "$KIT_DIR/tools/render.py" "$1" "$2"; }
# 前端 serve.py 只用标准库，用系统 python3 跑即可；路径由这里探测，不写死在模板里。
# 优先 /usr/bin/python3（发行版自带，最稳定）——避免系统里装了 conda 时误用 conda 的 python。
if [[ -x /usr/bin/python3 ]]; then
  PYTHON3="/usr/bin/python3"
else
  PYTHON3="$(command -v python3 || true)"
  [[ -n "$PYTHON3" ]] || PYTHON3="/usr/bin/python3"
fi
export APP_USER APP_HOME CODE_DIR DB_NAME DB_USER DB_PASS \
       FRONTEND_PORT BACKEND_PORT NIFI_PORT FRONTEND_BIND BACKEND_BIND \
       SESSION_TTL ALLOW_SELF_REGISTER DOMAIN NIFI_PROXY_HOST \
       SECRET_KEY META_KEY SITE_PASS PYTHON3

TMP_SQL="$(mktemp)"
render "$KIT_DIR/templates/init.sql" "$TMP_SQL"
if mysql -u root -e "SELECT 1" >/dev/null 2>&1; then
  # 先把「机器上本来就有」的情况说清楚，避免部署者担心数据被清掉
  DB_EXISTS=$(mysql -u root -N -e "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='${DB_NAME}'" 2>/dev/null || echo 0)
  USR_EXISTS=$(mysql -u root -N -e "SELECT COUNT(*) FROM mysql.user WHERE User='${DB_USER}' AND Host='localhost'" 2>/dev/null || echo 0)
  [[ "$DB_EXISTS" == "1" ]] \
    && log "检测到已有数据库 $DB_NAME：init.sql 全部是 IF NOT EXISTS，现有数据【会保留】"
  [[ "$USR_EXISTS" == "1" ]] \
    && note_warn "MySQL 里已存在用户 ${DB_USER}@localhost：其密码将被重置为本机 config.env 的 DB_PASS。若它另有用途，请先把 config.env 的 DB_USER 改成别的名字"
  mysql -u root < "$TMP_SQL"
elif [[ -n "${MYSQL_ROOT_PASS:-}" ]]; then
  mysql -u root -p"$MYSQL_ROOT_PASS" < "$TMP_SQL"
else
  die "无法以 root 登录 MySQL（请设置 MYSQL_ROOT_PASS 或配置 socket 免密）"
fi
rm -f "$TMP_SQL"
log "数据库 $DB_NAME 与用户 $DB_USER 就绪"

# ---------- 8. systemd 服务 ----------
log "8/14 安装 systemd 服务"
mkdir -p "/etc/systemd/system/iot-backend.service.d"
render "$KIT_DIR/templates/iot-backend.service" "/etc/systemd/system/iot-backend.service"
render "$KIT_DIR/templates/iot-frontend.service" "/etc/systemd/system/iot-frontend.service"
render "$KIT_DIR/templates/iot-backend.service.override.conf" \
       "/etc/systemd/system/iot-backend.service.d/override.conf"
chmod 600 "/etc/systemd/system/iot-backend.service.d/override.conf"
# systemd 操作统一加护栏：在容器等「非 systemd 环境」里 systemctl 根本不存在，
# 不加护栏会因 set -e 在这里静默中断整个部署（也让本脚本无法进容器自测）。
if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload
  systemctl enable iot-backend.service iot-frontend.service >/dev/null 2>&1 \
    || warn "systemctl enable 失败（不影响本次启动）"
else
  note_warn "本机没有 systemctl（非 systemd 环境）→ 两个服务不会开机自启，需改用其它方式托管"
fi
record record_file /etc/systemd/system/iot-backend.service \
                   /etc/systemd/system/iot-frontend.service \
                   /etc/systemd/system/iot-backend.service.d/override.conf
record record_svc iot-backend iot-frontend

# ---------- 9. NiFi ----------
log "9/14 NiFi 容器"
if [[ "$INSTALL_NIFI" == "true" ]] && command -v docker >/dev/null 2>&1; then
  render "$KIT_DIR/templates/docker-compose.yml" "$CODE_DIR/docker-compose.generated.yml"

  # 优先加载离线镜像包（工厂无外网时的唯一可行方式）
  NIFI_TAR=""
  for cand in "$KIT_DIR"/nifi-image.tar "$KIT_DIR"/offline-assets/*.tar \
              "$APP_HOME"/nifi-image.tar "$APP_HOME"/iot-nifi-python-*.tar; do
    [[ -f "$cand" ]] && { NIFI_TAR="$cand"; break; }
  done

  # 幂等：同一个镜像包已经加载过（用「包名:字节数」当指纹，写进清单）就不再 load。
  # docker load 要读 1.6G，重跑几次就白等几分钟；换新包时指纹变化，会自动重新加载。
  IMG_STAMP="/var/lib/iot-platform/nifi-image.loaded"
  IMG_SIG=""
  if [[ -n "$NIFI_TAR" ]]; then
    IMG_SIG="$(basename "$NIFI_TAR"):$(stat -c %s "$NIFI_TAR" 2>/dev/null || echo 0)"
  fi

  if [[ -n "$NIFI_TAR" ]] && docker image inspect iot-nifi-python:latest >/dev/null 2>&1 \
     && [[ -f "$IMG_STAMP" ]] && [[ "$(cat "$IMG_STAMP" 2>/dev/null)" == "$IMG_SIG" ]]; then
    log "镜像已加载过（$(basename "$NIFI_TAR")，包大小未变）→ 跳过 docker load"
  elif [[ -n "$NIFI_TAR" ]]; then
    log "发现离线镜像包 $(basename "$NIFI_TAR")，加载中（不联网）..."
    if docker load -i "$NIFI_TAR"; then
      mkdir -p /var/lib/iot-platform 2>/dev/null || true
      printf '%s' "$IMG_SIG" > "$IMG_STAMP" 2>/dev/null || true
    else
      warn "镜像加载失败"
    fi
  elif docker image inspect iot-nifi-python:latest >/dev/null 2>&1; then
    log "已存在 iot-nifi-python 镜像，跳过构建"
  else
    warn "未找到离线镜像包，尝试 docker build（需要联网拉 apache/nifi 基础镜像）"
    (cd "$CODE_DIR/docker/nifi" && docker build -t iot-nifi-python:latest .) \
      || warn "镜像构建失败 —— 离线环境属正常现象，请先用 export_nifi_image.sh 导出镜像并放到 deploy-kit/nifi-image.tar"
  fi
  if docker compose version >/dev/null 2>&1; then DC="docker compose"; else DC="docker-compose"; fi
  # 【多版本遗留探测】早期版本的安装程序可能用 `docker run` 建过 iot-nifi。
  # 这种容器没有 compose 的管理标签，`compose up -d` 会因「容器名已被占用」失败 →
  # 表面上 NiFi 还在跑，但挂载/参数可能仍是旧那套。这里先探测并给出明确处置办法。
  if docker inspect iot-nifi >/dev/null 2>&1 \
     && [[ "$(docker inspect iot-nifi \
              --format '{{if index .Config.Labels "com.docker.compose.project"}}y{{else}}n{{end}}' \
              2>/dev/null)" == "n" ]]; then
    warn "检测到 iot-nifi 容器【不是 compose 管理的】（旧版本的安装程序用 docker run 建过它）"
    warn "  compose 会因容器名冲突而重建失败 → 处置（flow 存在挂载目录里，不会丢）："
    warn "    docker rm -f iot-nifi  然后重跑： sudo $KIT_DIR/deploy.sh"
    note_warn "iot-nifi 是旧版 docker run 建的、不归 compose 管 → 建议 docker rm -f iot-nifi 后重跑，让 compose 按当前配置重建"
  fi
  (cd "$CODE_DIR" && $DC -f docker-compose.generated.yml up -d) || warn "NiFi 启动失败"
  # 兜底：强制容器开机自启。若容器是早前用 docker run 建的（restart=no），
  # compose 不会改它的重启策略，这里补一刀，保证断电重启后 NiFi 能自动起来。
  docker update --restart unless-stopped iot-nifi >/dev/null 2>&1 \
    && log "已设置 NiFi 容器开机自启（unless-stopped）" \
    || warn "设置 NiFi 容器自启失败，可手工执行：docker update --restart unless-stopped iot-nifi"
  sleep 5
  # worker 脚本放进挂载目录 bin/ 下。
  # ⚠️ 新机器上 real_nifi_data 是空目录，bin/ 不存在，docker cp 会静默失败
  #（开发机上一直有这个目录，所以从来没暴露过）—— 必须先 mkdir。
  mkdir -p "$APP_HOME/real_nifi_data/bin"
  record record_container iot-nifi
  record record_file "$CODE_DIR/docker-compose.generated.yml"
  record record_dir "$APP_HOME/real_nifi_data/bin"
  for w in nifi_upload_convert_worker.py nifi_db_export_worker.py \
           nifi_auto_tagging_worker.py nifi_mysql_export_worker.py; do
    if [[ -f "$CODE_DIR/v1-backend/scripts/$w" ]]; then
      if docker cp "$CODE_DIR/v1-backend/scripts/$w" iot-nifi:/opt/nifi/nifi-current/data/iot/bin/ >/dev/null 2>&1; then
        log "  worker 已放入容器: $w"
      else
        warn "  worker 复制失败: $w（NiFi 打标/导出功能会不可用）"
        note_warn "worker 脚本复制失败: $w"
      fi
    fi
  done
else
  warn "跳过 NiFi（INSTALL_NIFI=false 或无 docker）"
fi

# ---------- 10. 启动后端与前端 ----------
log "10/14 启动服务"
if command -v systemctl >/dev/null 2>&1; then
  systemctl restart iot-backend.service
  systemctl restart iot-frontend.service
else
  warn "本机没有 systemctl → 跳过服务重启（非 systemd 环境；下面的健康检查会等到超时，属预期）"
fi
for i in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:${BACKEND_PORT}/api/v1/health" >/dev/null 2>&1 \
     || curl -sf "http://127.0.0.1:${BACKEND_PORT}/docs" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

# ---------- 11. 创建本站唯一账号 ----------
log "11/14 创建本站账号 $SITE_USER（admin=$SITE_IS_ADMIN）"
SITE_ADMIN_FLAG=""
# 注意：set -e 下 `[ cond ] && cmd` 在 cond 为假时会返回非零导致脚本退出，故补 || true
if [[ "$SITE_IS_ADMIN" == "true" ]]; then SITE_ADMIN_FLAG="--admin"; fi
export CODE_DIR DB_HOST=127.0.0.1 DB_PORT=3306 DB_USER DB_PASS DB_NAME IN_DATA_BASE_DIR="$APP_HOME"
# --keep-password：账号已存在时【不重置密码】。
# 重跑的目的是"把没做完的补完"，不是重新开号；现场人员记的是第一次生成的那个密码。
# 想改密码：sudo deploy-kit/set_password.sh <账号> <新密码>
USERCTL_OUT="$(as_user env CODE_DIR="$CODE_DIR" DB_HOST=127.0.0.1 DB_PORT=3306 \
  DB_USER="$DB_USER" DB_PASS="$DB_PASS" DB_NAME="$DB_NAME" IN_DATA_BASE_DIR="$APP_HOME" \
  "$VENV/bin/python" "$KIT_DIR/tools/userctl.py" create "$SITE_USER" "$SITE_PASS" \
  $SITE_ADMIN_FLAG --keep-password 2>&1)" || note_warn "账号创建/更新失败：$USERCTL_OUT"
# userctl.py 返回的是一行 JSON。以前直接把这行 JSON 打到屏幕上，运维看着像报错 ——
# 这里只输出人话；解析不了（异常）时才把原始输出亮出来。
# 「密码是否被重置」用 output["password"] 的值判断（kept=没动 / set=本次新设）。
SITE_PASS_DISPLAY="$SITE_PASS"
if [[ "$USERCTL_OUT" == *'"password": "kept"'* ]]; then
  SITE_PASS_DISPLAY="不变（沿用上次已生成的密码；重跑不会重置）"
  log "  账号 $SITE_USER 已存在 → 更新完成，密码保持不变"
  log "  （要改密码： sudo $KIT_DIR/set_password.sh $SITE_USER <新密码>）"
elif [[ "$USERCTL_OUT" == *'"action": "created"'* || "$USERCTL_OUT" == *'"action":"created"'* ]]; then
  log "  账号 $SITE_USER 已创建（admin=$SITE_IS_ADMIN）"
elif [[ "$USERCTL_OUT" == *'"username"'* ]]; then
  log "  账号 $SITE_USER 已就绪（admin=$SITE_IS_ADMIN）"
else
  note_warn "账号创建/更新返回了非预期内容：$USERCTL_OUT"
fi
# 记录到 secrets，方便回查
grep -q '^SITE_USER=' "$SECRETS_FILE" 2>/dev/null || echo "SITE_USER=$SITE_USER" >> "$SECRETS_FILE"
grep -q '^SITE_IS_ADMIN=' "$SECRETS_FILE" 2>/dev/null || echo "SITE_IS_ADMIN=$SITE_IS_ADMIN" >> "$SECRETS_FILE"
record record_user "$SITE_USER"
# 账号目录 + 它的四个数据根也记账：卸载时即使 config.env 丢了也能认出这些是本平台的
record record_dir "$APP_HOME/$SITE_USER"
for _d in nifi-data tagged_nifi_data real_nifi_data tagged_real_nifi_data; do
  [[ -d "$APP_HOME/$SITE_USER/$_d" ]] && record record_dir "$APP_HOME/$SITE_USER/$_d"
done

# ---------- 11.1 清理旧版本遗留的默认 admin 账号（弱口令） ----------
# 老版本后端 init_db() 会自动建 admin（密码历史默认 123456）。新版补丁只阻止「再建」，
# 不会删「已经建出来的那一条」——这是「从旧版本升级」特有的缺口：
#   · 破坏「一套实例 = 一个账号」的设计；
#   · 公网机器上多一个弱口令管理员（安全隐患）；
#   · verify.sh / smoke_test.sh 会报「用户总数为 2（单账号模式应为 1）」。
# 工具内部有保护条款：站点账号叫 admin 时不动；密码不是已知默认口令时只告警不删。
if [[ "${REMOVE_DEFAULT_ADMIN:-true}" == "true" ]]; then
  log "11.1 清理旧版遗留的默认 admin 账号（若有）"
  _ra_rc=0
  REMOVE_ADMIN_OUT="$(as_user env CODE_DIR="$CODE_DIR" DB_HOST=127.0.0.1 DB_PORT=3306 \
      DB_USER="$DB_USER" DB_PASS="$DB_PASS" DB_NAME="$DB_NAME" IN_DATA_BASE_DIR="$APP_HOME" \
      SITE_PASS="$SITE_PASS" ADMIN_PASS="${ADMIN_PASS:-}" \
      "$VENV/bin/python" "$KIT_DIR/tools/remove_default_admin.py" \
      --site-user "$SITE_USER" 2>&1)" || _ra_rc=$?
  printf '%s\n' "$REMOVE_ADMIN_OUT" | sed 's/^/       /'
  case "$_ra_rc" in
    0) : ;;
    2) note_warn "检测到名为 admin 的账号，但密码不是已知默认口令 → 未自动删除，请人工确认（是默认账号就删掉/改密码，是真实账号请保留）" ;;
    *) note_warn "清理默认 admin 账号时出错（rc=$_ra_rc，见上方输出）" ;;
  esac
else
  log "11.1 已跳过默认 admin 清理（REMOVE_DEFAULT_ADMIN=false）"
fi

# ---------- 12. Nginx ----------
# Nginx 只是「80 端口的便利入口」，不是必需品：
# 站点本身可以直接用 http://<服务器IP>:5174 访问。
# 所以这一步失败只告警，绝不能让 set -e 把整个部署中断。
log "12/14 Nginx"
if [[ "$INSTALL_NGINX" == "true" ]] && command -v nginx >/dev/null 2>&1; then
  render "$KIT_DIR/templates/nginx.conf" "/etc/nginx/sites-available/iot.conf"
  ln -sf /etc/nginx/sites-available/iot.conf /etc/nginx/sites-enabled/iot.conf
  record record_file /etc/nginx/sites-available/iot.conf /etc/nginx/sites-enabled/iot.conf
  # 默认站点和我们的 80 端口冲突，需要停用；但【不删除】，挪个名字保留，
  # 万一这台机器上还有别的站点在跑，随时能恢复。
  if [[ -e /etc/nginx/sites-enabled/default ]]; then
    if mv -f /etc/nginx/sites-enabled/default /etc/nginx/sites-enabled/default.disabled-by-iot 2>/dev/null; then
      log "已停用 Nginx 默认站点（保留为 default.disabled-by-iot，需要时可改回）"
      note_warn "Nginx 默认站点已被停用（保留为 default.disabled-by-iot），若机器上另有站点请检查"
    else
      warn "无法停用 Nginx 默认站点（可能 80 端口冲突），不影响 5174 直接访问"
    fi
  fi
  if nginx -t >/dev/null 2>&1; then
    systemctl enable --now nginx >/dev/null 2>&1 || true
    systemctl reload nginx >/dev/null 2>&1 || systemctl restart nginx >/dev/null 2>&1 \
      || warn "nginx 启动失败（不影响 http://<IP>:${FRONTEND_PORT} 直接访问）"
    log "Nginx 已就绪（80 端口 → 前端 ${FRONTEND_PORT}）"
  else
    warn "nginx 配置校验未通过，已跳过（不影响 http://<IP>:${FRONTEND_PORT} 直接访问）"
    warn "  排查：nginx -t ；若 80 端口已被占用，可改 ${KIT_DIR}/templates/nginx.conf 的 listen"
  fi
else
  warn "跳过 Nginx（未安装或 INSTALL_NGINX=false）—— 直接用 http://<IP>:${FRONTEND_PORT} 访问"
fi

# ---------- 13. 防火墙自动放行（有防火墙才需要） ----------
# 以前这一步要人去执行 ufw allow / firewall-cmd，很多人装完发现打不开就是这个原因。
# 现在脚本自动做：只放行本平台自己的端口，不碰其他规则。
log "13/14 防火墙放行"
FW_PORTS=("$FRONTEND_PORT")
[[ "$INSTALL_NGINX" == "true" ]] && command -v nginx >/dev/null 2>&1 && FW_PORTS+=(80)
[[ "$INSTALL_NIFI" != "false" ]] && FW_PORTS+=("$NIFI_PORT")
ALLOWED=()
UFW_STATUS="$(command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null || true)"
if grep -qi "Status: active" <<<"$UFW_STATUS"; then
  for pt in "${FW_PORTS[@]}"; do
    if ufw allow "$pt/tcp" >/dev/null 2>&1; then
      log "  ufw 已放行 $pt/tcp"
      ALLOWED+=("$pt")
    else
      warn "  ufw 放行 $pt/tcp 失败，请手工执行：sudo ufw allow $pt/tcp"
      note_warn "防火墙未放行 $pt/tcp（请手工：sudo ufw allow $pt/tcp）"
    fi
  done
elif command -v firewall-cmd >/dev/null 2>&1 && systemctl is-active --quiet firewalld 2>/dev/null; then
  for pt in "${FW_PORTS[@]}"; do
    if firewall-cmd --add-port="$pt/tcp" --permanent >/dev/null 2>&1 \
       && firewall-cmd --reload >/dev/null 2>&1; then
      log "  firewalld 已放行 $pt/tcp"
      ALLOWED+=("$pt")
    else
      warn "  firewalld 放行 $pt/tcp 失败，请手工放行"
      note_warn "防火墙未放行 $pt/tcp（请手工放行）"
    fi
  done
else
  log "  未检测到启用的本机防火墙，跳过（云主机安全组仍需在控制台放行）"
fi
# 只记录【我们实际放行】的端口，卸载时只回收这些 —— 不动机器上原有的防火墙规则
if [[ ${#ALLOWED[@]} -gt 0 ]]; then
  mkdir -p /var/lib/iot-platform 2>/dev/null || true
  printf '%s\n' "${ALLOWED[@]}" > /var/lib/iot-platform/firewall-ports.txt 2>/dev/null || true
fi

# ---------- 14. SQL Server ODBC 驱动（可选，离线 deb）----------
if [[ "${INSTALL_ODBC:-true}" == "true" ]] && [[ -d "$KIT_DIR/offline-assets/deb" ]]; then
  log "14/14 SQL Server ODBC 驱动（附加，可选）"
  # 失败只告警不中断：只有「连 SQL Server 导数据」才需要它。
  # 但必须进「需要你关注」清单，否则会淹没在上面几十行输出里。
  bash "$KIT_DIR/install_odbc.sh" \
    || note_warn "SQL Server ODBC 驱动未装上（仅影响「连 SQL Server 导数据」，其它数据源不受影响）→ 重装：sudo bash $KIT_DIR/install_odbc.sh"
else
  warn "14/14 已跳过 SQL Server ODBC 驱动（INSTALL_ODBC=false 或包内无 deb）"
fi

# ---------- 汇总 ----------
# 先把「不影响跑起来、但需要人工处理」的事项集中列出来，免得淹没在日志里
if [[ ${#WARNINGS[@]} -gt 0 ]]; then
  echo
  echo -e "${C_YELLOW}=========== 以下事项需要你关注 ===========${C_OFF}"
  _i=1
  for _w in "${WARNINGS[@]}"; do
    echo "  ${_i}) ${_w}"
    _i=$((_i + 1))
  done
fi

cat <<EOF

${C_GREEN}=========== 部署完成 ===========${C_OFF}
  前端      http://<服务器IP>:${FRONTEND_PORT}      ← 浏览器打开这个（绑定 ${FRONTEND_BIND}）
  后端      http://127.0.0.1:${BACKEND_PORT}（仅本机，对外只经前端代理）
  NiFi      https://localhost:${NIFI_PORT}/nifi     ← 必须用 localhost，用 IP 会报 Invalid SNI
  域名      ${DOMAIN}

  本站账号      ${SITE_USER}   (admin=${SITE_IS_ADMIN})
  本站密码      ${SITE_PASS_DISPLAY:-$SITE_PASS}
  自助注册      ${ALLOW_SELF_REGISTER}

  数据根        ${APP_HOME}/<用户名>
  密钥文件      ${SECRETS_FILE}
  归属台账      /var/lib/iot-platform（记录了本平台装的包/生成的文件/容器/账号；
                重跑时靠它判定「哪些是本平台的残留」可以打破重建；卸载时靠它只回收自己的）
                查看： bash ${KIT_DIR}/tools/manifest.sh show

${C_YELLOW}下一步${C_OFF}
  1) 自检：  sudo ${KIT_DIR}/verify.sh  &&  sudo ${KIT_DIR}/smoke_test.sh
  2) 登录内部管理页 ${SITE_USER}，确认能看到用户列表
  3) 生产环境请务必配置 HTTPS，并复核 ${SECRETS_FILE} 中的密钥

${C_YELLOW}出问题 / 有 [warn] 怎么办？${C_OFF}
  【不要卸载，直接重跑】安装是幂等的：做过的事不再做，只补没完成的部分
      sudo ${KIT_DIR}/deploy.sh
      · 不再问那 4 个问题（沿用 config.env）· 密码不重置 · 镜像不重复加载
  改密码：  sudo bash ${KIT_DIR}/set_password.sh
  改配置：  sudo ${KIT_DIR}/setup.sh --reconfig   （会重新问那 4 个问题）
  【卸载是最后手段】只在"这台机器彻底不要装了"时才用
      （会删数据，说明见 ${KIT_DIR}/uninstall_all.sh 顶部）

${C_YELLOW}浏览器打不开？${C_OFF}
  1) 先在服务器本机自测：  curl -I http://127.0.0.1:${FRONTEND_PORT}/
  2) 本机防火墙已由脚本自动放行；若是云主机，请在控制台「安全组」放行 ${FRONTEND_PORT}
  3) NiFi 只有连本机才能用 localhost 访问，远程请走 Nginx 的 /nifi/ 或 SSH 隧道
EOF
