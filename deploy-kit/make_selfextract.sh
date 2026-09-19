#!/usr/bin/env bash
# ============================================================
# 把离线包封装成「自解压安装器」
#
#   ./make_selfextract.sh [输入tgz] [输出文件]
#
# 产物是一个可执行文件（如 iot-install.run），拷到新机器后：
#
#   sudo ./iot-install.run
#
# 它会自动：解压到 /home/yhz → 启动部署向导。
# 即「一条命令」完成，对方不需要懂 tar 和解压。
# ============================================================
set -euo pipefail

KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_BASE="${2:-/home/yhz/iot-install.run}"

C_G='\033[0;32m'; C_Y='\033[0;33m'; C_R='\033[0;31m'; C_OFF='\033[0m'
log()  { echo -e "${C_G}[selfextract]${C_OFF} $*"; }
warn() { echo -e "${C_Y}[warn]${C_OFF} $*"; }
err()  { echo -e "${C_R}[err]${C_OFF} $*" >&2; }

# 找输入包
PKG="${1:-}"
if [[ -z "$PKG" ]]; then
  PKG="$(ls -t /home/yhz/iot-offline-*.tgz 2>/dev/null | head -1 || true)"
fi
[[ -n "$PKG" && -f "$PKG" ]] || { err "找不到离线包（用法：./make_selfextract.sh <包.tgz> [输出文件]）"; exit 1; }
[[ "$PKG" == *.tgz || "$PKG" == *.tar.gz ]] || { err "输入不是 .tgz/.tar.gz"; exit 1; }

SIZE=$(du -h "$PKG" | cut -f1)
log "输入包：$PKG ($SIZE)"
log "输出：$OUT_BASE"

TMP_OUT="${OUT_BASE}.tmp"
rm -f "$TMP_OUT"

# ---------- 写头部（自解压逻辑） ----------
cat > "$TMP_OUT" <<'HEADER'
#!/usr/bin/env bash
# ============================================================
#  IoT 智慧平台 · 自解压安装器
#
#  用法:  sudo ./iot-install.run
#
#  可选:  IOT_INSTALL_DIR=/other/path sudo ./iot-install.run
#         （默认解压到 /home/yhz，除非必要请勿修改）
# ============================================================
set -euo pipefail

TARGET="${IOT_INSTALL_DIR:-/home/yhz}"

echo "======================================"
echo "  IoT 智慧平台 安装程序"
echo "======================================"

if [[ $EUID -ne 0 ]]; then
  echo "[err] 请用 root 运行：sudo $0" >&2
  exit 1
fi

if [[ -e "$TARGET/iot" ]]; then
  echo "[提示] 目标目录 $TARGET/iot 已存在"
  echo "       常见原因：上次部署的残留，或你把手动解压的文件放在了这里。"
  echo "       选 y 会把程序文件解压/更新进去，原有文件不会丢失，功能正常。"
  read -rp "       继续吗？[y/n，默认 y]: " _ans
  case "${_ans:-y}" in y|Y|yes) ;; *) echo "已取消"; exit 0 ;; esac
fi

echo "[install] 解压到 $TARGET ..."

# 关键顺序：先建运行用户（由 useradd -m 创建家目录），再解压。
# 若反过来（先 mkdir /home/yhz 再 useradd -m），部分系统会因家目录已存在而报错。
RUN_USER="${IOT_APP_USER:-yhz}"
if ! id -u "$RUN_USER" >/dev/null 2>&1; then
  if useradd -m -s /bin/bash "$RUN_USER" 2>/dev/null; then
    echo "       已创建运行用户 $RUN_USER"
  else
    echo "       创建用户 $RUN_USER 失败（可能已存在或权限不足），改用目录方式继续"
  fi
else
  echo "       运行用户 $RUN_USER 已存在"
fi

mkdir -p "$TARGET"

# 从本文件自身的标记行之后提取归档
ARCHIVE_START=$(awk '/^__IOT_ARCHIVE_BELOW__$/{print NR+1; exit 0}' "$0")
if [[ -z "${ARCHIVE_START:-}" ]]; then
  echo "[err] 归档标记缺失，文件可能已损坏" >&2
  exit 1
fi
tail -n +"$ARCHIVE_START" "$0" | tar xzf - -C "$TARGET"

if [[ ! -x "$TARGET/iot/deploy-kit/setup.sh" ]]; then
  echo "[err] 解压后未找到部署向导，请确认包完整性" >&2
  exit 1
fi

# 修正属主，避免后端进程（以 RUN_USER 身份运行）读不到文件
chown -R "$RUN_USER:$RUN_USER" "$TARGET/iot" 2>/dev/null || true

echo "[install] 启动部署向导..."
cd "$TARGET/iot/deploy-kit"
exec ./setup.sh

exit 0
__IOT_ARCHIVE_BELOW__
HEADER

# ---------- 追加归档数据 ----------
log "拼接归档数据（约需 1 分钟）..."
cat "$PKG" >> "$TMP_OUT"
chmod +x "$TMP_OUT"
mv -f "$TMP_OUT" "$OUT_BASE"

# ---------- 校验（只检查头部；不能对整个文件做 bash -n，因为尾部是二进制归档） ----------
_AS=$(awk '/^__IOT_ARCHIVE_BELOW__$/{print NR+1; exit 0}' "$OUT_BASE")
if [[ -n "${_AS:-}" ]] && head -n $((_AS-1)) "$OUT_BASE" | bash -n 2>/dev/null; then
  log "头部语法校验通过（归档起始行 $_AS）"
else
  warn "头部语法校验未通过，请检查"
fi

OUT_SIZE=$(du -h "$OUT_BASE" | cut -f1)
echo
echo "======================================"
log "自解压安装器已生成：$OUT_BASE ($OUT_SIZE)"
echo
echo "  拷到新机器后，只需要执行一条命令："
echo "      sudo ./iot-install.run"
echo
echo "  它会自动解压 + 启动部署向导。"
echo "======================================"
