#!/usr/bin/env bash
# 彻底卸载 IoT 平台（把部署装上去的东西全部清掉）
#   sudo bash uninstall_all.sh           # 交互确认
#   sudo bash uninstall_all.sh --yes     # 不询问
#   sudo bash uninstall_all.sh --keep-docker --keep-mysql   # 保留 Docker/MySQL
# ⚠️ 会删除：服务、MySQL(含数据)、Docker(含镜像)、Nginx、ODBC、/home/yhz 整个家目录
#   如果机器上还跑着别的东西（别的网站/数据库/容器），不要在这台机器上执行。
set -uo pipefail
export DEBIAN_FRONTEND=noninteractive

C_R='\033[0;31m'; C_Y='\033[0;33m'; C_G='\033[0;32m'; C_OFF='\033[0m'
log()  { echo -e "${C_G}[uninstall]${C_OFF} $*"; }
warn() { echo -e "${C_Y}[warn]${C_OFF} $*"; }
err()  { echo -e "${C_R}[err]${C_OFF} $*" >&2; }
bad_venv=0

APP_USER="${IOT_APP_USER:-yhz}"
KEEP_DOCKER=0; KEEP_MYSQL=0; KEEP_NGINX=0; ASSUME_YES=0
DEL_USER=0; PURGE_HOME=0
for a in "$@"; do
  case "$a" in
    --keep-docker) KEEP_DOCKER=1 ;;
    --keep-mysql)  KEEP_MYSQL=1 ;;
    --keep-nginx)  KEEP_NGINX=1 ;;
    --del-user)    DEL_USER=1 ;;
    --purge-home)  PURGE_HOME=1 ;;
    --yes|-y)      ASSUME_YES=1 ;;
  esac
done
[[ $EUID -eq 0 ]] || { err "请用 root 运行：sudo bash $0"; exit 1; }

echo
echo -e "${C_R}========================================================"
echo -e "  即将彻底卸载 IoT 平台，以下内容会被【永久删除】："
echo -e "========================================================${C_OFF}"
echo "  · 服务   iot-backend / iot-frontend"
echo "  · MySQL  数据库与全部数据（/var/lib/mysql）"
[[ "$KEEP_DOCKER" == "1" ]] || echo "  · Docker 全部容器与镜像（/var/lib/docker）"
echo "  · Nginx  本平台站点配置"
echo "  · ODBC   msodbcsql17 / unixodbc 系列"
echo "  · 文件   /home/$APP_USER 整个家目录（含 iot、iot111、密码文件）"
echo "  · 用户   $APP_USER"
echo
if [[ "$ASSUME_YES" != "1" ]]; then
  read -rp "确认卸载？输入大写 YES 继续: " _c
  [[ "$_c" == "YES" ]] || { echo "已取消。"; exit 1; }
fi

MANIFEST_DIR=/var/lib/iot-platform
NEW_LIST="$MANIFEST_DIR/pkg-new.list"
PRE_LIST="$MANIFEST_DIR/pre-existing.txt"
FW_FILE="$MANIFEST_DIR/firewall-ports.txt"
HAVE_MANIFEST=0
[[ -s "$NEW_LIST" ]] && HAVE_MANIFEST=1
# 某系统包是否是【本次部署安装的】：
#   有清单 → 查清单；没清单（老版本装的）→ 保守视为不是我们装的（保留并提示）
ours_pkg() {
  if [[ "$HAVE_MANIFEST" == "1" ]]; then grep -qx "$1" "$NEW_LIST" 2>/dev/null; else return 1; fi
}
pre_existing() { grep -q "pre-existing: $1" "$PRE_LIST" 2>/dev/null; }
# 只卸【精确包名】的包 —— 绝不用正则/通配符。
# 教训：`purge 'gcc*'` 是正则，会匹配 libgcc-s1（系统基础库）→ 直接把系统搞坏
purge_exact() {
  local pkgs=() p
  for p in "$@"; do dpkg -s "$p" >/dev/null 2>&1 && pkgs+=("$p"); done
  [[ ${#pkgs[@]} -eq 0 ]] && return 0
  apt-get purge -y "${pkgs[@]}" >/dev/null 2>&1 || true
}

step() { echo; echo "========== $* =========="; }

step "1/9 停止并删除 iot-backend / iot-frontend 服务"
systemctl disable --now iot-backend iot-frontend >/dev/null 2>&1 || true
rm -f /etc/systemd/system/iot-backend.service /etc/systemd/system/iot-frontend.service
rm -rf /etc/systemd/system/iot-backend.service.d
systemctl daemon-reload >/dev/null 2>&1 || true
systemctl reset-failed >/dev/null 2>&1 || true
log "服务已删除"

step "2/9 删除 NiFi 容器与镜像"
if command -v docker >/dev/null 2>&1; then
  docker rm -f iot-nifi >/dev/null 2>&1 && log "  已删容器 iot-nifi" || warn "  无 iot-nifi 容器"
  docker rmi -f iot-nifi-python:latest >/dev/null 2>&1 && log "  已删镜像" || warn "  无镜像"
  docker network rm iot_default >/dev/null 2>&1 || true
fi

step "3/9 MySQL（按清单判断：机器上原有的不动）"
if [[ "$KEEP_MYSQL" == "1" ]]; then
  warn "  --keep-mysql：跳过"
elif pre_existing mysql && ! ours_pkg mysql-server-8.0; then
  warn "  机器上【原本就有】MySQL（不是本次部署装的）→ 保留，数据不动"
  warn "  平台建的 nifi 库仍在其中；如需清理请手工处理"
elif ! pre_existing mysql && [[ "$HAVE_MANIFEST" == "1" ]]; then
  log "  MySQL 是本次部署安装的 → 卸载（含数据）"
  systemctl stop mysql >/dev/null 2>&1 || true
  purge_exact mysql-server mysql-server-8.0 mysql-server-core-8.0 \
              mysql-client mysql-client-8.0 mysql-client-core-8.0 mysql-common
  rm -rf /var/lib/mysql /var/lib/mysql-files /var/log/mysql /etc/mysql /run/mysqld
  log "  已删除"
else
  warn "  无安装清单，无法确认 MySQL 是不是本次装的 → 保留"
  warn "  如确认要卸：sudo apt-get purge -y 'mysql-server*' && sudo rm -rf /var/lib/mysql"
fi

step "4/9 Docker（按清单判断：机器上原有的不动）"
if [[ "$KEEP_DOCKER" == "1" ]]; then
  warn "  --keep-docker：跳过"
elif pre_existing docker && ! ours_pkg docker.io; then
  warn "  机器上【原本就有】Docker → 保留"
elif ! pre_existing docker && [[ "$HAVE_MANIFEST" == "1" ]]; then
  log "  Docker 是本次部署安装的 → 卸载"
  systemctl stop docker docker.socket containerd >/dev/null 2>&1 || true
  purge_exact docker.io docker-cli containerd containerd.io runc \
              docker-compose-v2 docker-compose
  rm -rf /var/lib/docker /var/lib/containerd /etc/docker
  log "  已删除"
else
  warn "  无安装清单 → 保留 Docker"
fi

step "5/9 Nginx（按清单判断；本平台站点配置必删）"
if [[ -e /etc/nginx/sites-enabled/default.disabled-by-iot ]]; then
  mv -f /etc/nginx/sites-enabled/default.disabled-by-iot /etc/nginx/sites-enabled/default 2>/dev/null || true
  log "  已恢复 Nginx 默认站点"
fi
rm -f /etc/nginx/sites-enabled/iot.conf /etc/nginx/sites-available/iot.conf
log "  本平台站点配置已删除"
if [[ "$KEEP_NGINX" == "1" ]]; then
  warn "  --keep-nginx：跳过卸载"
elif pre_existing nginx && ! ours_pkg nginx-core; then
  warn "  机器上【原本就有】Nginx → 保留（本平台站点配置已移除）"
elif ! pre_existing nginx && [[ "$HAVE_MANIFEST" == "1" ]]; then
  log "  Nginx 是本次部署安装的 → 卸载"
  purge_exact nginx nginx-common nginx-core
  rm -rf /etc/nginx
  log "  已删除"
else
  warn "  无安装清单 → 保留 Nginx（站点配置已移除，不影响其他站点）"
fi

step "6/9 卸载 ODBC 驱动（SQL Server 数据源用）"
purge_exact msodbcsql17 unixodbc unixodbc-common odbcinst odbcinst1debian2 \
            libodbc2 libodbcinst2 libodbc1 libltdl7
rm -f /etc/odbcinst.ini /etc/odbc.ini
log "  ODBC 驱动已删除"

step "7/9 编译链与 Python venv（按清单判断，机器上原有的保留）"
# 【严禁正则卸载】`purge 'gcc*'` 在 apt 里是正则，会匹配 libgcc-s1（系统基础库），
# 把整台机器搞坏 —— 必须按清单里「本次部署新装」的精确包名卸。
REMOVED_GCC=0
for p in gcc g++ make cpp; do
  if ours_pkg "$p"; then
    apt-get purge -y "$p" >/dev/null 2>&1 || true
    REMOVED_GCC=1
  fi
done
[[ "$REMOVED_GCC" == "1" ]] && log "  编译器是本次装的 → 已卸" || log "  编译器非本次所装（或无清单）→ 保留"
if command -v python3 >/dev/null 2>&1 && python3 -c 'import ensurepip' >/dev/null 2>&1; then
  if ours_pkg python3-venv || ours_pkg python3.10-venv; then
    apt-get purge -y python3-venv python3.10-venv python3.11-venv \
                     python3.12-venv python3.13-venv >/dev/null 2>&1 || true
    log "  python3-venv 是本次装的 → 已卸"
  else
    log "  python3-venv 非本次所装 → 保留"
  fi
fi
if ours_pkg python3-pip; then apt-get purge -y python3-pip >/dev/null 2>&1 || true; fi
apt-get autoremove --purge -y >/dev/null 2>&1 || true

step "6.5/9 回收部署时放行的防火墙端口 + 恢复 apt 源"
# 只回收【安装时我们实际放行】的端口（记录在防火墙清单里），不动机器上原有规则
if [[ -f "$FW_FILE" ]]; then
  if command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null | grep -qi "Status: active"; then
    while read -r pt; do
      [[ -z "$pt" ]] && continue
      ufw delete allow "$pt/tcp" >/dev/null 2>&1 && log "  已回收 ufw 放行 $pt/tcp"
    done < "$FW_FILE"
  elif command -v firewall-cmd >/dev/null 2>&1 && systemctl is-active --quiet firewalld 2>/dev/null; then
    while read -r pt; do
      [[ -z "$pt" ]] && continue
      firewall-cmd --remove-port="$pt/tcp" --permanent >/dev/null 2>&1 && \
        firewall-cmd --reload >/dev/null 2>&1 && log "  已回收 firewalld 放行 $pt/tcp"
    done < "$FW_FILE"
  fi
else
  log "  无防火墙放行记录（或非本平台放行），跳过"
fi
# 安装时若把 apt 源从 http 切换过 https，会留有 .bak.iot 备份 → 恢复原文件
for b in /etc/apt/sources.list.bak.iot /etc/apt/sources.list.d/*.bak.iot; do
  [[ -e "$b" ]] || continue
  mv -f "$b" "${b%.bak.iot}" && log "  已恢复 apt 源 $(basename "${b%.bak.iot}")"
done

step "8/9 清理孤儿依赖与系统账号/debconf 残留"
apt-get autoremove --purge -y >/dev/null 2>&1 || true
# 装包时自动创建的系统账号（purge 不一定删得掉）。
# 只清理「对应软件确实是本次装的」的账号；软件还在就绝不动账号
for u in mysql docker; do
  if [[ "$u" == "mysql" ]] && dpkg -s mysql-server-8.0 >/dev/null 2>&1; then continue; fi
  if [[ "$u" == "docker" ]] && dpkg -s docker.io >/dev/null 2>&1; then continue; fi
  if getent passwd "$u" >/dev/null 2>&1; then
    deluser --system "$u" >/dev/null 2>&1 || deluser "$u" >/dev/null 2>&1 || true
    log "  已删除系统账号 $u"
  fi
  if getent group "$u" >/dev/null 2>&1; then
    delgroup "$u" >/dev/null 2>&1 || true
  fi
done
# 清掉 ODBC 许可的 debconf 残留答案
if command -v debconf-communicate >/dev/null 2>&1; then
  echo "purge msodbcsql17" | debconf-communicate >/dev/null 2>&1 || true
  echo "purge msodbcsql"   | debconf-communicate >/dev/null 2>&1 || true
fi
log "  已清理"

step "9/9 删除程序文件与数据目录"
# 【默认只删平台的东西，保留整个家目录】
#   /home/yhz 里除了平台，往往还有用户自己的东西：miniconda3、.bashrc、.ssh 等，
#   全删会把现场人员的登录环境一起抹掉。
#   · 默认：删 iot、4 个数据根、本站账号目录；保留家目录其余内容与用户
#   · --purge-home：连 /home/yhz 整个家目录一起删
#   · --del-user：  连用户也删
PURGE_HOME=0
for a in "$@"; do [[ "$a" == "--purge-home" ]] && PURGE_HOME=1; done

# 先趁 config.env 还在，读出本站账号名（它有一个同名的数据根目录）
SITE_USER=""
for cf in "/home/$APP_USER/iot/deploy-kit/config.env" "/home/$APP_USER/iot/deploy-kit/.deploy-secrets"; do
  [[ -f "$cf" ]] || continue
  SITE_USER="$(grep -E '^SITE_USER=' "$cf" 2>/dev/null | head -1 | cut -d= -f2- || true)"
  [[ -n "$SITE_USER" ]] && break
done

if [[ "$PURGE_HOME" == "1" ]]; then
  rm -rf "/home/$APP_USER"
  log "  已删除整个家目录 /home/$APP_USER（--purge-home）"
else
  PLATFORM_DIRS=(iot real_nifi_data tagged_real_nifi_data nifi-data tagged_nifi_data iot111)
  [[ -n "$SITE_USER" ]] && PLATFORM_DIRS+=("$SITE_USER")
  for d in "${PLATFORM_DIRS[@]}"; do
    if [[ -e "/home/$APP_USER/$d" ]]; then
      rm -rf "/home/$APP_USER/$d"
      log "  已删除 /home/$APP_USER/$d"
    fi
  done
  log "  家目录 /home/$APP_USER 已保留（miniconda3、.bashrc、.ssh 等未动）"
fi

if [[ "$DEL_USER" == "1" ]] && id "$APP_USER" >/dev/null 2>&1; then
  pkill -u "$APP_USER" >/dev/null 2>&1 || true
  sleep 2
  userdel "$APP_USER" >/dev/null 2>&1 && log "  已删除用户 $APP_USER" || warn "  用户删除失败"
elif id "$APP_USER" >/dev/null 2>&1; then
  log "  保留了用户 $APP_USER（默认不删，--del-user 才删）"
fi

step "卸载完成，残留自检"
LEFT=""
systemctl cat iot-backend >/dev/null 2>&1 && LEFT="$LEFT 服务iot-backend"
[[ -d /var/lib/mysql ]] && LEFT="$LEFT MySQL数据目录"
[[ -d /var/lib/docker ]] && LEFT="$LEFT Docker目录"
[[ -d /home/$APP_USER/iot ]] && LEFT="$LEFT 程序目录iot"
if [[ -z "$LEFT" ]]; then
  echo -e "${C_G}  检查通过：平台上装的东西已全部清除 ✓${C_OFF}"
  echo
  echo "  家目录 /home/$APP_USER 已保留（miniconda3、.bashrc 等未动）"
  echo "  重新部署：sudo ./iot-install.run"
  echo "  （想连整个家目录也删：sudo bash $0 --purge-home --del-user）"
else
  echo -e "${C_Y}  以下未清除（可能是别的东西，未自动处理）：$LEFT${C_OFF}"
fi
