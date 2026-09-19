#!/usr/bin/env bash
# ============================================================
# 系统依赖安装（离线优先，HTTPS 源兜底）
#
#   sudo ./install_sysdeps.sh [--apt-only] [--skip-docker]
#
# 要装的：python3 / python3-venv / python3-pip / nginx / mysql-server
#         / docker(io) / curl / ca-certificates
#
# 三种情形，脚本自动选最省事的一种：
#   ① 软件都在  → 直接跳过，什么都不做
#   ② 包内备有本发行版的离线 deb（offline-assets/deb/sysdeps/<id>-<版本>/）
#               → dpkg 离线安装，全程零下载
#   ③ 没有对应离线 deb → 自动把 apt 源从 http:// 换成 https:// 再装
#                        （很多内网/防火墙只放行 443，80 被挡会导致 apt 超时）
#
# 幂等：重复执行安全。
# ============================================================
set -uo pipefail

# 固定 C 排序：否则 en_US 等 locale 会忽略 - 和 _，
# 导致 glob 顺序把 perl 排到 perl-base 前面（曾因此让 mysql-server 配置失败）
export LC_ALL=C
# dpkg 配置阶段不能有任何交互询问（比如 mysql-server 的 preinst 询问数据目录），
# 否则在脚本环境里会卡住或直接配置失败
export DEBIAN_FRONTEND=noninteractive

# ============================================================
# 安装清单（卸载时靠它区分「我们装的」和「机器上原本就有的」）
#   /var/lib/iot-platform/pkg-before.list  装之前的全部已装包
#   /var/lib/iot-platform/pkg-after.list   装之后的全部已装包
#   /var/lib/iot-platform/pkg-new.list     差集 = 本次部署新装的包
#   /var/lib/iot-platform/pre-existing.txt mysql/docker/nginx 在装之前是否存在
# ============================================================
MANIFEST_DIR=/var/lib/iot-platform
init_manifest() {
  mkdir -p "$MANIFEST_DIR" 2>/dev/null || true
  if [[ ! -f "$MANIFEST_DIR/pkg-before.list" ]]; then
    dpkg --get-selections 2>/dev/null | awk '{print $1}' | sort -u \
      > "$MANIFEST_DIR/pkg-before.list" 2>/dev/null || true
  fi
  {
    for s in mysql docker nginx; do
      if command -v "$s" >/dev/null 2>&1; then echo "pre-existing: $s"; else echo "not-present: $s"; fi
    done
  } > "$MANIFEST_DIR/pre-existing.txt" 2>/dev/null || true
}
finalize_manifest() {
  mkdir -p "$MANIFEST_DIR" 2>/dev/null || true
  dpkg --get-selections 2>/dev/null | awk '{print $1}' | sort -u \
    > "$MANIFEST_DIR/pkg-after.list" 2>/dev/null || true
  if [[ -f "$MANIFEST_DIR/pkg-before.list" ]]; then
    # 【只累加、不重写】pkg-new.list 是「本平台装的包」的**总台账**：
    # 里面除了本次差集，还有 install_odbc.sh 和以往部署记下的条目。
    # 以前直接用 comm 差集覆盖它 → 那些记录会被冲掉（卸载时再也回收不了）。
    local diff="$MANIFEST_DIR/.pkg-diff.$$" merged="$MANIFEST_DIR/.pkg-new.$$"
    comm -13 "$MANIFEST_DIR/pkg-before.list" "$MANIFEST_DIR/pkg-after.list" > "$diff" 2>/dev/null || true
    { cat "$MANIFEST_DIR/pkg-new.list" 2>/dev/null || true; cat "$diff" 2>/dev/null || true; } \
      | grep -v '^[[:space:]]*$' | sort -u > "$merged" 2>/dev/null || true
    if [[ -s "$merged" ]]; then
      mv -f "$merged" "$MANIFEST_DIR/pkg-new.list" 2>/dev/null || true
    fi
    rm -f "$diff" "$merged" 2>/dev/null || true
  fi
}

KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SYSDEB_ROOT="$KIT_DIR/offline-assets/deb/sysdeps"

C_G='\033[0;32m'; C_Y='\033[0;33m'; C_R='\033[0;31m'; C_OFF='\033[0m'
log()  { echo -e "${C_G}[sysdeps]${C_OFF} $*"; }
warn() { echo -e "${C_Y}[warn]${C_OFF} $*"; }
err()  { echo -e "${C_R}[err]${C_OFF} $*" >&2; }

APT_ONLY="false"
SKIP_DOCKER="false"
for a in "$@"; do
  case "$a" in
    --apt-only)    APT_ONLY="true" ;;
    --skip-docker) SKIP_DOCKER="true" ;;
    -h|--help)     sed -n '2,20p' "$0"; exit 0 ;;
  esac
done

[[ $EUID -eq 0 ]] || { err "请用 root 运行：sudo ./install_sysdeps.sh"; exit 1; }

# ---------------- 发行版识别 ----------------
ID=""; VERSION_ID=""; PRETTY=""
if [[ -f /etc/os-release ]]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  ID="${ID:-}"; VERSION_ID="${VERSION_ID:-}"; PRETTY="${PRETTY_NAME:-}"
fi
DISTRO_KEY="${ID:-unknown}-${VERSION_ID:-unknown}"
log "本机系统：${PRETTY:-未知}（标识 ${DISTRO_KEY}）"

# ---------------- 需要哪些命令 ----------------
CORE_CMDS=(python3 nginx mysql)
# mysql 在某些发行版叫 mariadb，二者有一个即可
have_mysql() { command -v mysql >/dev/null 2>&1 || command -v mariadb >/dev/null 2>&1; }
# docker compose：v2 是 `docker compose` 子命令，v1 是独立的 docker-compose
have_compose() {
  command -v docker >/dev/null 2>&1 \
    && { docker compose version >/dev/null 2>&1 || command -v docker-compose >/dev/null 2>&1; }
}

missing_cmds() {
  local out=""
  for c in "${CORE_CMDS[@]}"; do
    case "$c" in
      mysql) have_mysql || out="$out mysql" ;;
      *)     command -v "$c" >/dev/null 2>&1 || out="$out $c" ;;
    esac
  done
  # venv 是单独的包（python3-venv），缺了 deploy.sh 第 6 步建不了虚拟环境
  have_venv || out="$out python3-venv"
  command -v curl >/dev/null 2>&1 || out="$out curl"
  if [[ "$SKIP_DOCKER" != "true" ]]; then
    command -v docker >/dev/null 2>&1 || out="$out docker"
    have_compose || out="$out docker-compose"
  fi
  echo "$out"
}

have_venv() { command -v python3 >/dev/null 2>&1 && python3 -c 'import venv, ensurepip' >/dev/null 2>&1; }

# 装好后立刻让 mysql/docker 开机自启（离线路径也会走到这里）
enable_services() {
  systemctl enable --now mysql 2>/dev/null || true
  systemctl enable --now docker 2>/dev/null || true
}

MISSING="$(missing_cmds)"
if [[ -z "${MISSING// /}" ]]; then
  log "基础软件齐全（python3 / nginx / mysql / curl / docker），无需安装 ✓"
  exit 0
fi
log "缺少：$MISSING"

# 记录「安装前」的机器状态：已装包清单 + mysql/docker/nginx 是否原本就存在。
# 卸载脚本靠它区分「我们装的」（要卸）和「机器上原本就有的」（绝不能动）。
init_manifest

# ---------------- apt 源 http -> https ----------------
fix_apt_https() {
  local changed=0 f
  for f in /etc/apt/sources.list \
           /etc/apt/sources.list.d/*.list \
           /etc/apt/sources.list.d/*.sources; do
    [[ -f "$f" ]] || continue
    case "$f" in *.bak.*) continue ;; esac
    grep -qE 'http://' "$f" 2>/dev/null || continue
    cp -n "$f" "${f}.bak.iot" 2>/dev/null || true
    sed -i -E 's#http://#https://#g' "$f"
    log "  源已切换 HTTPS：$f（原文件备份为 ${f}.bak.iot）"
    changed=1
  done
  [[ "$changed" == "1" ]]
}

apt_update() {
  export DEBIAN_FRONTEND=noninteractive
  if apt-get update -qq >/dev/null 2>&1; then
    return 0
  fi
  warn "apt-get update 失败 —— 尝试把软件源切换到 HTTPS（80 端口常被防火墙拦截）"
  if fix_apt_https; then
    if apt-get update -qq >/dev/null 2>&1; then
      log "切换 HTTPS 后 apt 源可用 ✓"
      return 0
    fi
  fi
  warn "apt 源仍不可用（可能完全断网/需代理），尝试直接用离线 deb"
  return 1
}

# ---------------- 离线 deb 安装 ----------------
# 是否已完整安装
_deb_installed_ok() {
  [[ "$(dpkg-query -W -f='${Status}' "$1" 2>/dev/null || true)" == "install ok installed" ]]
}

install_from_debs() {
  local dir="$1" n skipped=0 d name ver cur round
  local list=() names=() older=()
  n="$(ls "$dir"/*.deb 2>/dev/null | wc -l)"
  [[ "$n" -gt 0 ]] || return 1
  log "发现本发行版离线 deb $n 个（$dir），离线安装中（零下载）..."
  : > /tmp/iot-sysdeps.log

  # ---- 第一轮：只补「机器上还没有」的包；已装的一律不动 ----
  # 目标机可能跑着别的业务在用的 mysql/docker，擅自升级会破坏别人的数据。
  # （但依赖版本冲突时会在下面第二轮兜底，见注释）
  for d in "$dir"/*.deb; do
    [[ -f "$d" ]] || continue
    name="$(dpkg-deb -f "$d" Package 2>/dev/null || true)"
    [[ -n "$name" ]] || continue
    if _deb_installed_ok "$name"; then
      ver="$(dpkg-deb -f "$d" Version 2>/dev/null || true)"
      cur="$(dpkg-query -W -f='${Version}' "$name" 2>/dev/null || true)"
      if [[ -n "$cur" && -n "$ver" ]] && ! dpkg --compare-versions "$cur" ge "$ver" 2>/dev/null; then
        older+=("$name（本机 $cur < 包内 $ver）")
      fi
      skipped=$((skipped + 1))
      continue
    fi
    list+=("$d"); names+=("$name")
  done
  log "第一轮：待安装 $((n - skipped)) 个（$skipped 个本机已装，保持原样不动）"
  [[ ${#older[@]} -gt 0 ]] && log "       本机已有但版本较旧（本轮不动）：${older[*]}"

  # 关键：一次性把全部 deb 交给 dpkg，由 dpkg 自己做依赖拓扑排序。
  # 逐个 -i 会被「locale 排序」坑到（en_US 排序会忽略 - 和 _，
  # 于是 perl 会排在 perl-base 前面 -> perl 配置失败 -> 连累 mysql-server）。
  if [[ ${#list[@]} -gt 0 ]]; then
    dpkg -i --force-confold "${list[@]}" >>/tmp/iot-sysdeps.log 2>&1 || true
  fi
  for round in 1 2 3 4 5; do
    dpkg --configure -a >>/tmp/iot-sysdeps.log 2>&1 || true
  done

  # ---- 第二轮（兜底）：只在本轮装的包「没配置成功」时才触发 ----
  # 为什么要兜底：deb 是一个版本一致的闭包，跳过机器上已有的旧包可能造成
  # 依赖冲突。典型例子：容器里已有 perl-base 1.8，包内 perl 硬依赖
  # perl-base(=1.9) -> perl 配置失败 -> 连锁导致 mysql-server 一直停在
  # 「未配置」。这时只能把旧版也一起升上去，否则整组包都装不完。
  # 触发条件很苛刻（只有真的配不上才会走），并会把要升级的包明确列出来。
  local broken=()
  for name in "${names[@]}"; do
    _deb_installed_ok "$name" || broken+=("$name")
  done
  if [[ ${#broken[@]} -gt 0 ]]; then
    # 只要还有「装了但没配置好」的包，就必须整组重装一遍 ——
    # 否则 mysql-server 这种关键包会一直停在未配置状态，后端初始化数据库必失败。
    warn "以下包未能完成配置：${broken[*]}"
    warn "  → 第二轮：用完整 deb 集重装并重新配置"
    if [[ ${#older[@]} -gt 0 ]]; then
      warn "     本轮会同时升级机器上已有的旧版包（共 ${#older[@]} 个）："
      for o in "${older[@]}"; do warn "       $o"; done
      warn "  ↑ 若其中有你不想动的（如正在用的 mysql-server），请先 Ctrl+C 手工处理。"
    fi
    dpkg -i --force-confold "$dir"/*.deb >>/tmp/iot-sysdeps.log 2>&1 || true
    for round in 1 2 3 4 5; do
      dpkg --configure -a >>/tmp/iot-sysdeps.log 2>&1 || true
    done
  fi

  # ---- 最后：仍处于 unpacked 状态的包，逐个重试一遍 ----
  for d in "$dir"/*.deb; do
    [[ -f "$d" ]] || continue
    name="$(dpkg-deb -f "$d" Package 2>/dev/null || true)"
    [[ -n "$name" ]] || continue
    _deb_installed_ok "$name" && continue
    dpkg -i --force-confold "$d" >>/tmp/iot-sysdeps.log 2>&1 || true
  done
  dpkg --configure -a >>/tmp/iot-sysdeps.log 2>&1 || true

  log "离线安装结束（$skipped 个本机已装未动）"
  return 0
}

# ---------------- 主流程 ----------------
# 机器上可能残留「已解包未配置」的包（比如上次 apt/dpkg 中断留下的）。
# 不先清掉，后面所有 dpkg -i 都会因为「依赖的包尚未配置」而连锁失败
# （实测：mysql-server、unixodbc、odbcinst 一起卡住，连累 ODBC 装不上）。
if command -v dpkg >/dev/null 2>&1; then
  if dpkg -C >/dev/null 2>&1; then
    log "dpkg 状态干净（没有未配置完成的残留包）"
  else
    warn "检测到有「已解包未配置」的残留包，先执行 dpkg --configure -a 清理..."
    dpkg --configure -a >>/tmp/iot-sysdeps.log 2>&1 || true
    dpkg -C >/dev/null 2>&1 && log "  清理完成 ✓" || warn "  清理后仍有残留（详见 /tmp/iot-sysdeps.log）"
  fi
fi

HOST_ARCH="$(dpkg --print-architecture 2>/dev/null || echo unknown)"
if [[ "$APT_ONLY" != "true" && -d "$SYSDEB_ROOT/$DISTRO_KEY" ]]; then
  PACKED_ARCH="$(cat "$SYSDEB_ROOT/$DISTRO_KEY/ARCH" 2>/dev/null || echo amd64)"
  if [[ "$PACKED_ARCH" != "$HOST_ARCH" ]]; then
    warn "离线 deb 架构为 $PACKED_ARCH，本机为 $HOST_ARCH —— 跳过离线安装，改用 apt 源"
  else
    install_from_debs "$SYSDEB_ROOT/$DISTRO_KEY"
    finalize_manifest
    if [[ -z "$(missing_cmds | tr -d ' ')" ]]; then
      enable_services
      finalize_manifest
      log "离线安装完成，基础软件齐全 ✓"
      exit 0
    fi
    warn "离线 deb 安装后仍有缺失：$(missing_cmds) —— 转用 apt 源安装"
  fi
elif [[ "$APT_ONLY" != "true" && -d "$SYSDEB_ROOT" ]]; then
  warn "包内没有 $DISTRO_KEY 的离线 deb（现有：$(ls "$SYSDEB_ROOT" 2>/dev/null | tr '\n' ' '))"
  warn "  → 改用 apt 源在线安装"
fi

# 组装包名（发行版不同，包名不同）
PKGS=()
command -v python3 >/dev/null 2>&1 || PKGS+=(python3)
have_venv          || PKGS+=(python3-venv python3-pip)
have_mysql         || PKGS+=(mysql-server)
command -v nginx   >/dev/null 2>&1 || PKGS+=(nginx)
command -v curl    >/dev/null 2>&1 || PKGS+=(curl)
if [[ "$SKIP_DOCKER" != "true" ]]; then
  command -v docker >/dev/null 2>&1 || PKGS+=(docker.io)
fi
# 编译器是兜底：万一某个 wheel 和本机 Python 不匹配，pip 需要现场编译源码包
PKGS+=(ca-certificates gcc g++ make)

if ! command -v apt-get >/dev/null 2>&1; then
  # 非 Debian/Ubuntu 系（CentOS/麒麟/统信 等）：本包的离线 deb 和 apt 兜底都用不上
  warn "本机不是 Debian/Ubuntu 系（没有 apt-get），离线 deb 与 apt 兜底都无法使用"
  warn "  → 请让 IT 预装以下软件后重跑本安装程序（脚本会检测到已装并跳过）："
  warn "       python3(≥3.9) + python3-venv、mysql-server、nginx、docker、docker compose"
  err "系统软件无法自动安装，仍缺失：$(missing_cmds)"
  exit 1
fi

if apt_update; then
  log "apt 安装：${PKGS[*]}"
  # --no-upgrade：已装过的包绝不升级，避免动到机器上正在跑的旧版 mysql/docker 等
  apt-get install -y -qq --no-upgrade --no-install-recommends "${PKGS[@]}" \
    || warn "部分系统包安装失败（可看 /var/log/apt/term.log）"
fi

# docker compose：不同发行版包名不同，逐个尝试
if [[ "$SKIP_DOCKER" != "true" ]] && command -v docker >/dev/null 2>&1 \
   && ! docker compose version >/dev/null 2>&1 && ! command -v docker-compose >/dev/null 2>&1; then
  apt-get install -y -qq --no-upgrade docker-compose-v2 >/dev/null 2>&1 \
    || apt-get install -y -qq --no-upgrade docker-compose >/dev/null 2>&1 \
    || warn "docker compose 未装上（可后补；只影响 NiFi 容器）"
fi

systemctl enable --now mysql 2>/dev/null || true
systemctl enable --now docker 2>/dev/null || true

# 安装动作全部结束，落盘「本次新装的包」清单
finalize_manifest

# ---------------- 收尾核对 ----------------
# 【关键】服务端包必须处于「已配置(ii)」状态。
# 只看 `command -v mysql` 是不够的 —— mysql 命令是客户端，服务端
# mysql-server-8.0 停在「已解包未配置(iU)」时照样有这个命令，
# 但后面初始化数据库一定会失败。
SRV_BAD=""
for srv in mysql-server mysql-server-8.0 mariadb-server; do
  st="$(dpkg-query -W -f='${db:Status-Abbrev}' "$srv" 2>/dev/null || true)"
  [[ -z "$st" ]] && continue                       # 没装这个包，跳过
  if [[ "$st" != ii* ]]; then SRV_BAD="$SRV_BAD $srv"; fi
done
if [[ -n "$SRV_BAD" ]]; then
  err "以下服务端包已解包但【未配置完成】：$SRV_BAD"
  err "  这样 MySQL 服务起不来，部署到「初始化数据库」那步必然失败。"
  err "  处理：① 查原因  tail -60 /tmp/iot-sysdeps.log"
  err "        ② 重新配置  sudo dpkg --configure -a"
  err "        ③ 仍失败则重跑本安装程序，或手工：sudo apt install -y --reinstall mysql-server"
  err "  （见上方 /tmp/iot-sysdeps.log 的 [ERROR] 行）"
  exit 1
fi

REST="$(missing_cmds)"
if [[ -z "${REST// /}" ]]; then
  log "系统依赖安装完成，全部就绪 ✓"
  exit 0
fi

err "以下仍未安装成功：$REST"
echo "      排查建议："
echo "        1) 网络：curl -sS https://mirrors.aliyun.com/ubuntu/ -o /dev/null -w '%{http_code}\\n'   # 期望 200"
echo "        2) 源文件：cat /etc/apt/sources.list（或 .d/ 下的 .list/.sources），应为 https://"
echo "        3) 或让 IT 预装：apt install -y python3 python3-venv nginx mysql-server docker.io"
exit 1
