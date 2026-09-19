#!/usr/bin/env bash
# ============================================================
# 归属台账（Ownership Ledger）
#
# 解决的问题：
#   目标机器上可能混着「上一次部署留下的东西」和「本来就有的东西」。
#   要按当前这个安装包的声明去「打破重建」时，必须先能回答一句话：
#       这个东西到底是本平台装的，还是机器上本来就有的？
#   台账就是这份记录。部署期用它判定，卸载期用它回收。
#
# 目录：/var/lib/iot-platform/
#   pkg-new.list                     本平台安装/管理过的 apt 包（累加，不重写）
#   files-rendered-by-iot.txt        本平台生成/覆盖过的文件（systemd/nginx/compose…）
#   dirs-created-by-iot.txt          本平台创建的目录
#   services-enabled-by-iot.txt      本平台开启过的自启服务
#   containers-by-iot.txt            本平台创建的容器
#   users-created-by-iot.txt         本平台创建的账号
#   pre-existing.txt / firewall-ports.txt / nifi-image.loaded    （原有）
#
# 用法（都从别的脚本里调用，失败绝不影响部署）：
#   bash manifest.sh record_pkg  libodbc1 unixodbc
#   bash manifest.sh record_file /etc/systemd/system/iot-backend.service
#   bash manifest.sh record_dir  /home/yhz/real_nifi_data
#   bash manifest.sh record_svc  iot-backend iot-frontend
#   bash manifest.sh record_container iot-nifi
#   bash manifest.sh record_user humi
#   bash manifest.sh is_ours_pkg libodbc1      # 退出码 0 = 是本平台装的
#   bash manifest.sh show                      # 打印台账概览
# ============================================================
set -uo pipefail

MANIFEST_DIR="${IOT_MANIFEST_DIR:-/var/lib/iot-platform}"
PKG_LIST="$MANIFEST_DIR/pkg-new.list"
FILE_LIST="$MANIFEST_DIR/files-rendered-by-iot.txt"
DIR_LIST="$MANIFEST_DIR/dirs-created-by-iot.txt"
SVC_LIST="$MANIFEST_DIR/services-enabled-by-iot.txt"
CTR_LIST="$MANIFEST_DIR/containers-by-iot.txt"
USR_LIST="$MANIFEST_DIR/users-created-by-iot.txt"

_ensure() { [[ -d "$MANIFEST_DIR" ]] || mkdir -p "$MANIFEST_DIR" 2>/dev/null || true; }

# 累加 + 去重。用 grep -F 精确整行匹配 —— 绝不用正则/通配符
# （教训：`purge 'gcc*'` 是正则，会匹配到系统基础库 libgcc-s1，直接把系统搞坏）
_add() {
  local f="$1"; shift
  local x
  _ensure
  for x in "$@"; do
    [[ -n "$x" ]] || continue
    grep -qxF "$x" "$f" 2>/dev/null || echo "$x" >> "$f" 2>/dev/null || true
  done
}

record_pkg()       { _add "$PKG_LIST" "$@"; }
record_file()      { _add "$FILE_LIST" "$@"; }
record_dir()       { _add "$DIR_LIST" "$@"; }
record_svc()       { _add "$SVC_LIST" "$@"; }
record_container() { _add "$CTR_LIST" "$@"; }
record_user()      { _add "$USR_LIST" "$@"; }

# 归属判定：台账里有记录 = 本平台装的（可以放心打破、按本包重建）
is_ours_pkg()  { grep -qxF "$1" "$PKG_LIST"  2>/dev/null; }
is_ours_file() { grep -qxF "$1" "$FILE_LIST" 2>/dev/null; }
is_ours_dir()  { grep -qxF "$1" "$DIR_LIST"  2>/dev/null; }

show() {
  local f n
  echo "归属台账：$MANIFEST_DIR"
  for f in "$PKG_LIST" "$FILE_LIST" "$DIR_LIST" "$SVC_LIST" "$CTR_LIST" "$USR_LIST"; do
    if [[ -f "$f" ]]; then
      n="$(wc -l < "$f" 2>/dev/null || echo 0)"
      printf '  %-32s %s 条\n' "$(basename "$f")" "$n"
    fi
  done
}

case "${1:-show}" in
  record_pkg)       shift; record_pkg "$@" ;;
  record_file)      shift; record_file "$@" ;;
  record_dir)       shift; record_dir "$@" ;;
  record_svc)       shift; record_svc "$@" ;;
  record_container) shift; record_container "$@" ;;
  record_user)      shift; record_user "$@" ;;
  is_ours_pkg)      is_ours_pkg "${2:-}" ;;
  is_ours_file)     is_ours_file "${2:-}" ;;
  is_ours_dir)      is_ours_dir "${2:-}" ;;
  show|*)           show ;;
esac
