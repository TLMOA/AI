#!/usr/bin/env bash
# ============================================================
# 环境体检 —— 部署前自动判断这台机器「能不能装、装得对不对」
#
# 只检测不安装，安全可反复运行。
#   sudo ./check_env.sh              # 完整报告
#   sudo ./check_env.sh --quiet      # 只输出结论（供 setup.sh/deploy.sh 自动判断用）
#
# 结论是【机器可读】的：
#   * 退出码：0 = 可以部署；1 = 有阻塞项（会打印原因）
#   * 最后一行：CHECK_ENV_VERDICT=OK  /  CHECK_ENV_VERDICT=BLOCKED
#   setup.sh / deploy.sh 会自动调用它并据此决定是否继续，
#   不需要人去读输出再判断。
#
# 检查五类问题：
#   1) 系统资源（CPU架构/内存/磁盘）—— 不够会直接判定为「不能部署」
#   2) 依赖软件（python/mysql/docker/nginx）—— 缺了会自动装，不算阻塞项
#   3) 端口占用 —— 自动区分「本平台自己的进程（可重启）」和「别的程序（冲突）」
#   4) 版本匹配 —— 离线 wheel 与 Python 版本 / CPU 架构强绑定
#   5) 现有环境 —— 已有 MySQL 库 / NiFi 容器的处理提示
# ============================================================
set -uo pipefail

KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
C_G='\033[0;32m'; C_Y='\033[0;33m'; C_R='\033[0;31m'; C_OFF='\033[0m'
MISSING=(); PROBLEMS=()
QUIET=0
for a in "$@"; do
  case "$a" in
    --quiet|-q) QUIET=1 ;;
  esac
done

have() { command -v "$1" >/dev/null 2>&1; }
# 是否 root：非 root 时看不到别的进程占用的端口，只能给出「未知」
IS_ROOT=0
[[ $EUID -eq 0 ]] && IS_ROOT=1

if [[ "$QUIET" != "1" ]]; then
  echo "======================================"
  echo "  部署环境体检"
  echo "======================================"
fi

row() {                      # 静默模式下不打印明细，但照常收集
  local name="$1" ok="$2" ver="$3"
  if [[ "$ok" == "yes" ]]; then
    [[ "$QUIET" != "1" ]] && printf "  %-16s ${C_G}%-6s${C_OFF} %s\n" "$name" "已有" "$ver"
  else
    [[ "$QUIET" != "1" ]] && printf "  %-16s ${C_R}%-6s${C_OFF} %s\n" "$name" "缺失" "$ver"
    MISSING+=("$name")
  fi
}

# ---------- 1. 系统资源 ----------
if [[ "$QUIET" != "1" ]]; then echo -e "\n${C_Y}【1】系统资源${C_OFF}"; fi
ARCH="$(uname -m)"
[[ "$QUIET" != "1" ]] && printf "  %-16s %s\n" "CPU 架构" "$ARCH"

MEM_MB=$(free -m 2>/dev/null | awk '/^Mem:/{print $2}')
if [[ -n "${MEM_MB:-}" ]]; then
  if [[ "$MEM_MB" -lt 7000 ]]; then
    [[ "$QUIET" != "1" ]] && printf "  %-16s ${C_R}%s${C_OFF}（建议 ≥8G）\n" "内存" "${MEM_MB}MB"
    PROBLEMS+=("内存不足 ${MEM_MB}MB（NiFi 需要约 8G）")
  else
    [[ "$QUIET" != "1" ]] && printf "  %-16s ${C_G}%s${C_OFF}\n" "内存" "${MEM_MB}MB"
  fi
fi

# 磁盘：要分两处看 —— /home 放代码与数据；/var/lib/docker 放 NiFi 镜像（1.6G，加载后膨胀）
disk_check() {
  local path="$1" label="$2" need="$3" avail
  avail="$(df -BG --output=avail "$path" 2>/dev/null | tail -1 | tr -d 'G ')"
  [[ -n "$avail" ]] || return 0
  if [[ "$avail" -lt "$need" ]]; then
    [[ "$QUIET" != "1" ]] && printf "  %-16s ${C_R}%s${C_OFF} 可用（%s，建议 ≥%sG）\n" "$label" "${avail}G" "$path" "$need"
    PROBLEMS+=("$label 空间不足（$path 仅剩 ${avail}G，需要 ≥${need}G）")
  else
    [[ "$QUIET" != "1" ]] && printf "  %-16s ${C_G}%s${C_OFF} 可用（%s）\n" "$label" "${avail}G" "$path"
  fi
}
disk_check /home "磁盘 /home" 20
disk_check /var/lib/docker "磁盘 docker" 10

# ---------- 2. 依赖软件（缺了会自动装，不算阻塞项） ----------
if [[ "$QUIET" != "1" ]]; then echo -e "\n${C_Y}【2】依赖软件${C_OFF}"; fi
if have python3; then row "python3" yes "$(python3 -V 2>&1)"; else row "python3" no "必需"; fi

if have mysql; then
  if mysql -u root -e "SELECT 1" >/dev/null 2>&1; then
    row "mysql" yes "$(mysql -V 2>/dev/null | head -1)（root 免密）"
  else
    row "mysql" yes "$(mysql -V 2>/dev/null | head -1)（root 需密码 → 填 MYSQL_ROOT_PASS）"
  fi
else
  row "mysql" no "认证数据库，必需"
fi

if have docker && docker info >/dev/null 2>&1; then
  row "docker" yes "$(docker --version 2>/dev/null | head -1)"
else
  row "docker" no "NiFi 容器依赖"
fi

if docker compose version >/dev/null 2>&1; then
  row "docker compose" yes "$(docker compose version 2>/dev/null | head -1)"
elif have docker-compose; then
  row "docker-compose" yes "旧版 CLI（兼容）"
else
  row "docker compose" no "容器编排"
fi

if have nginx; then row "nginx" yes "$(nginx -v 2>&1 | head -1)"; else row "nginx" no "反向代理（可选）"; fi

# 系统软件（mysql/docker/nginx）的离线 deb 是否覆盖本发行版
DISTRO_KEY=""
if [[ -f /etc/os-release ]]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  DISTRO_KEY="${ID:-unknown}-${VERSION_ID:-unknown}"
fi
SYSDEB_DIR="$KIT_DIR/offline-assets/deb/sysdeps/$DISTRO_KEY"
SYSDEB_OK=0
if [[ -d "$SYSDEB_DIR" ]] && compgen -G "$SYSDEB_DIR/*.deb" >/dev/null 2>&1; then
  SYSDEB_OK=1
  [[ "$QUIET" != "1" ]] && printf "  %-16s ${C_G}%-6s${C_OFF} %s 个离线 deb（%s）→ 断网也能装系统软件\n" \
    "系统离线包" "已有" "$(ls "$SYSDEB_DIR"/*.deb | wc -l)" "${DISTRO_KEY}"
else
  [[ "$QUIET" != "1" ]] && printf "  %-16s ${C_Y}%-6s${C_OFF} 无 %s 的离线 deb → 缺失软件时会自动把 apt 源换成 https:// 在线装\n" \
    "系统离线包" "无" "${DISTRO_KEY:-未知}"
fi

# ODBC 驱动：仅 SQL Server 数据源需要（可选，不算缺失）
if [[ "$QUIET" != "1" ]]; then
  ODBC_OUT="$(have odbcinst && odbcinst -q -d 2>/dev/null || true)"
  if grep -qi "SQL Server" <<<"$ODBC_OUT"; then
    printf "  %-16s ${C_G}%-6s${C_OFF} 已装 %s\n" "ODBC" "已有" "$(odbcinst -q -d 2>/dev/null | head -1 | tr -d '[]')"
  else
    printf "  %-16s ${C_Y}%-6s${C_OFF} 仅「连 SQL Server 导数据」才需要\n" "ODBC" "缺失"
  fi
fi

# ---------- 3. 端口占用（自动区分：自己的进程 vs 别人的程序） ----------
if [[ "$QUIET" != "1" ]]; then echo -e "\n${C_Y}【3】端口占用${C_OFF}"; fi
check_port() {
  local p="$1" name="$2" line proc
  line="$(ss -lntp 2>/dev/null | grep -E "[:.]${p}[[:space:]]" | head -1)"
  if [[ -z "$line" ]]; then
    [[ "$QUIET" != "1" ]] && printf "  %-6s ${C_G}%-6s${C_OFF} %s\n" "$p" "空闲" "$name"
    return
  fi
  # 从 ss 输出里取进程名（需要 root 才能拿到 PID/进程名）
  proc="$(sed -n 's/.*users:(("\([^"]*\)".*/\1/p' <<<"$line")"
  if [[ -z "$proc" && "$IS_ROOT" == "1" ]]; then
    local pid
    pid="$(sed -n 's/.*pid=\([0-9]*\).*/\1/p' <<<"$line")"
    [[ -n "$pid" ]] && proc="$(ps -o comm= -p "$pid" 2>/dev/null || true)"
  fi
  if [[ -z "$proc" ]]; then
    # 非 root 看不到进程名：不武断判定为冲突，提示换 root 再测
    [[ "$QUIET" != "1" ]] && printf "  %-6s ${C_Y}%-6s${C_OFF} %s（已占用，但非 root 看不到进程，请用 sudo 重跑确认）\n" "$p" "占用" "$name"
    return
  fi
  case "$proc" in
    *serve.py*|*uvicorn*|*mysqld*|*mariadbd*|*nginx*|*python*|*docker-proxy*)
      [[ "$QUIET" != "1" ]] && printf "  %-6s ${C_G}%-6s${C_OFF} %s（占用者 %s，属本平台/数据库，部署时会自动重启）\n" \
        "$p" "自用" "$name" "$proc"
      ;;
    *)
      printf "  %-6s ${C_R}%-6s${C_OFF} %s 被其他程序占用：%s\n" "$p" "冲突" "$name" "$proc"
      PROBLEMS+=("端口 $p（$name）被其他程序 $proc 占用，需先停掉或改 config.env 里的端口")
      ;;
  esac
}
check_port 5174 "前端"
check_port 8081 "后端"
check_port 8080 "NiFi"
check_port 3306 "MySQL"

# 防火墙：deploy.sh 会自动放行自己的端口；这里只告知状态
FW_HINT=""
UFW_STATUS="$(command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null || true)"
if grep -qi "Status: active" <<<"$UFW_STATUS"; then
  FW_HINT="ufw"
elif command -v firewall-cmd >/dev/null 2>&1 && systemctl is-active --quiet firewalld 2>/dev/null; then
  FW_HINT="firewalld"
fi
if [[ "$QUIET" != "1" ]]; then
  if [[ -n "$FW_HINT" ]]; then
    printf "  %-16s ${C_Y}%-6s${C_OFF} %s 已启用 → 部署时会自动放行 5174/8080\n" "防火墙" "注意" "$FW_HINT"
  else
    printf "  %-16s ${C_G}%-6s${C_OFF} 未检测到启用的本机防火墙\n" "防火墙" "正常"
  fi
fi

# ---------- 4. 版本匹配（离线 wheel 绑定） ----------
if [[ "$QUIET" != "1" ]]; then echo -e "\n${C_Y}【4】版本匹配（离线 wheel 按版本分目录）${C_OFF}"; fi
ARCH_FILE="$KIT_DIR/offline-assets/ARCH"

AVAIL=""
for d in "$KIT_DIR"/offline-assets/wheels/py*; do
  [[ -d "$d" ]] || continue
  v="$(basename "$d")"; v="${v#py}"
  AVAIL="$AVAIL ${v:0:1}.${v:1}"
done

# 部署脚本会优先挑「离线 wheel 有对应目录」的解释器（3.13→3.10），而不是盲用 python3
FOUND_PY=""
for v in 3.13 3.12 3.11 3.10; do
  [[ -d "$KIT_DIR/offline-assets/wheels/py${v//./}" ]] || continue
  cand=""
  [[ -x "/usr/bin/python$v" ]] && cand="/usr/bin/python$v" || cand="$(command -v "python$v" 2>/dev/null || true)"
  [[ -n "$cand" ]] && { FOUND_PY="$v（$cand）"; break; }
done

if [[ -n "$FOUND_PY" ]]; then
  [[ "$QUIET" != "1" ]] && printf "  %-16s ${C_G}匹配${C_OFF} 将使用 Python %s，离线 wheel 齐全\n" "Python 版本" "$FOUND_PY"
else
  [[ "$QUIET" != "1" ]] && printf "  %-16s ${C_Y}不匹配${C_OFF} 本机没有 python3.10~3.13；包内 wheel 版本：%s\n" \
    "Python 版本" "${AVAIL:-无}"
  PROBLEMS+=("没有可用的 Python 3.10~3.13（应用要求 ≥3.9；Ubuntu 20.04 自带 3.8 不满足）")
fi

if [[ -f "$ARCH_FILE" ]]; then
  WANT_A="$(cat "$ARCH_FILE")"
  if [[ "$WANT_A" == "$ARCH" ]]; then
    [[ "$QUIET" != "1" ]] && printf "  %-16s ${C_G}匹配${C_OFF} %s\n" "CPU 架构" "$ARCH"
  else
    [[ "$QUIET" != "1" ]] && printf "  %-16s ${C_R}不匹配${C_OFF} 离线wheel=%s，本机=%s\n" "CPU 架构" "$WANT_A" "$ARCH"
    PROBLEMS+=("CPU 架构不匹配（离线包为 $WANT_A，本机 $ARCH）")
  fi
fi

# ---------- 5. 现有环境 ----------
if [[ "$QUIET" != "1" ]]; then
  echo -e "\n${C_Y}【5】现有环境${C_OFF}"
  DOCKER_PS="$(have docker && docker ps -a --format '{{.Names}}' 2>/dev/null || true)"
  if grep -q '^iot-nifi$' <<<"$DOCKER_PS"; then
    printf "  %-16s ${C_Y}已存在${C_OFF} 已有 iot-nifi 容器（部署时会复用/覆盖）\n" "iot-nifi"
  else
    printf "  %-16s ${C_G}无冲突${C_OFF}\n" "iot-nifi"
  fi
  DB_LIST="$(have mysql && mysql -u root -e "SHOW DATABASES LIKE 'nifi'" 2>/dev/null || true)"
  if grep -q nifi <<<"$DB_LIST"; then
    printf "  %-16s ${C_Y}已存在${C_OFF} MySQL 已有 nifi 库（部署会保留现有数据）\n" "nifi 库"
  else
    printf "  %-16s ${C_G}无冲突${C_OFF}\n" "nifi 库"
  fi
fi

# ---------- 自动判定 ----------
# 缺软件【不算】阻塞项（会自动装）；但有阻塞项就明确说「不能部署」。
if [[ ${#MISSING[@]} -gt 0 ]]; then
  # 只有「缺软件 + 机器上没有任何包管理器 + 也没有本发行版离线 deb」才算没救
  if ! have apt-get && ! have dnf && ! have yum && ! have zypper && [[ "$SYSDEB_OK" != "1" ]]; then
    PROBLEMS+=("缺少系统软件（${MISSING[*]}），且机器上没有任何包管理器、离线 deb 也不覆盖本发行版")
  fi
fi

echo
if [[ ${#PROBLEMS[@]} -gt 0 ]]; then
  if [[ "$QUIET" != "1" ]]; then
    echo -e "${C_R}自动判定：不能部署（${#PROBLEMS[@]} 个阻塞项，必须先解决）${C_OFF}"
    for p in "${PROBLEMS[@]}"; do echo "  ✗ $p"; done
    if [[ ${#MISSING[@]} -gt 0 ]]; then
      echo
      echo -e "${C_Y}另外缺少软件（部署时会自动装，不阻塞）：${MISSING[*]}${C_OFF}"
    fi
    echo
    echo "提示：离线资源已备（多版本 Python wheel / NiFi 镜像 / SQL Server 驱动 / 系统软件 deb）。"
  else
    echo -e "${C_R}环境体检不通过：${C_OFF}"
    for p in "${PROBLEMS[@]}"; do echo "  ✗ $p"; done
  fi
  echo "CHECK_ENV_VERDICT=BLOCKED"
  exit 1
fi

if [[ "$QUIET" != "1" ]]; then
  if [[ ${#MISSING[@]} -gt 0 ]]; then
    echo -e "${C_Y}缺少软件 ${#MISSING[@]} 项：${MISSING[*]}${C_OFF}"
    echo "  → 包内有本发行版离线 deb：部署时自动离线安装（零下载）"
    echo "  → 没有的话：自动把 apt 源 http:// 换成 https:// 再在线安装"
    echo "  → 完全断网且无离线 deb：请 IT 先装"
    echo "     （apt install -y python3 python3-venv nginx mysql-server docker.io）"
  fi
  echo -e "${C_G}自动判定：可以部署${C_OFF}"
  echo "提示：离线资源已备（多版本 Python wheel / NiFi 镜像 / SQL Server 驱动 / 系统软件 deb）。"
else
  echo -e "${C_G}环境体检通过，可以部署${C_OFF}"
fi
echo "CHECK_ENV_VERDICT=OK"
exit 0
