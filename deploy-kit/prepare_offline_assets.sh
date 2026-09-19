#!/usr/bin/env bash
# ============================================================
# 离线资源准备 —— 在【能联网的机器】上运行一次
#
# 目标：做出一个「万能包」—— 目标机器环境完全未知，拿到就能部署。
#
#   ./prepare_offline_assets.sh
#
# 核心：为【多个 Python 版本】分别编译 wheel，一起打进包。
#       部署时 deploy.sh 自动检测本机 Python 版本，挑对应目录安装。
#       因此不需要事先知道目标机器是什么版本。
#
# 默认覆盖：3.10 / 3.11 / 3.12 / 3.13（Ubuntu 20.04~24.04 及更新）
# 自定义：  ./prepare_offline_assets.sh --python-versions "3.9 3.10 3.11"
#
# 产物（deploy-kit/offline-assets/）：
#   wheels/py310/   wheels/py311/   wheels/py312/   wheels/py313/
#   ARCH                          CPU 架构标记
#   iot-nifi-python-<日期>.tar     NiFi 容器镜像（约 1.6GB）
# ============================================================
set -euo pipefail

KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$KIT_DIR/.." && pwd)"
OUT_DIR="$KIT_DIR/offline-assets"
WHEELS_ROOT="$OUT_DIR/wheels"
VENV="$PROJECT_DIR/v1-backend/.venv"
REQ="$KIT_DIR/templates/requirements-runtime.txt"
STAMP="$(date +%Y%m%d)"
NIFI_IMG="iot-nifi-python:latest"

C_G='\033[0;32m'; C_Y='\033[0;33m'; C_R='\033[0;31m'; C_OFF='\033[0m'
log()  { echo -e "${C_G}[assets]${C_OFF} $*"; }
warn() { echo -e "${C_Y}[warn]${C_OFF} $*"; }
err()  { echo -e "${C_R}[err]${C_OFF} $*" >&2; }

# ---------- 参数 ----------
PY_VERSIONS=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --python-version|--python-versions)
      PY_VERSIONS="$PY_VERSIONS ${2:-}"; shift 2 ;;
    --python-version=*|--python-versions=*)
      PY_VERSIONS="$PY_VERSIONS ${1#*=}"; shift ;;
    *) warn "忽略未知参数: $1"; shift ;;
  esac
done
# 默认覆盖主流发行版版本
[[ -n "${PY_VERSIONS// /}" ]] || PY_VERSIONS="3.10 3.11 3.12 3.13"

echo "======================================"
echo "  离线资源准备（多版本万能包）"
echo "======================================"
echo "  目标 Python 版本: $PY_VERSIONS"
echo

# ---------- 联网检查 ----------
log "检查网络连通性"
if ! (curl -sI --max-time 8 https://pypi.org >/dev/null 2>&1); then
  err "无法访问 pypi.org —— 本脚本需在能联网的机器上运行"
  exit 1
fi

mkdir -p "$OUT_DIR"
PY="$VENV/bin/python"
[[ -x "$PY" ]] || PY="$(command -v python3)"
CUR_PY="$("$PY" -c 'import sys;print("%d.%d"%sys.version_info[:2])')"
log "本机 Python: $CUR_PY"

rm -rf "$WHEELS_ROOT"; mkdir -p "$WHEELS_ROOT"

# ---------- 逐版本编译 wheel ----------
FAILED_VERSIONS=""
for v in $PY_VERSIONS; do
  [[ -n "$v" ]] || continue
  VER_NODOT="${v//./}"
  DEST="$WHEELS_ROOT/py${VER_NODOT}"
  mkdir -p "$DEST"
  log "---- 准备 Python $v 的 wheel ----"

  if [[ "$v" == "$CUR_PY" ]]; then
    # 与本机同版本：直接用本机环境编译（最快）
    if "$PY" -m pip wheel -q -r "$REQ" -w "$DEST" 2>/dev/null; then
      log "Python $v：本机编译完成（$(find "$DEST" -name '*.whl' | wc -l) 个 wheel）"
    else
      warn "Python $v：本机编译失败"; FAILED_VERSIONS="$FAILED_VERSIONS $v"
    fi
    continue
  fi

  # 不同版本：用 docker 起该版本环境编译
  if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
    warn "docker 不可用，跳过 Python $v"
    FAILED_VERSIONS="$FAILED_VERSIONS $v"
    continue
  fi
  IMG="python:${v}-slim"
  log "拉取 $IMG 并编译（首次较慢）..."
  if docker run --rm \
        -v "$DEST:/wheels" \
        -v "$REQ:/req.txt:ro" \
        "$IMG" \
        bash -c "apt-get update -qq && apt-get install -y -qq gcc g++ curl >/dev/null 2>&1; pip install -q --upgrade pip; pip wheel -q -r /req.txt -w /wheels" 2>/dev/null; then
    log "Python $v：容器编译完成（$(find "$DEST" -name '*.whl' | wc -l) 个 wheel）"
  else
    warn "Python $v：容器编译失败"
    FAILED_VERSIONS="$FAILED_VERSIONS $v"
  fi
done

# ---------- 校验 ----------
echo
log "各版本 wheel 统计"
for d in "$WHEELS_ROOT"/py*; do
  [[ -d "$d" ]] || continue
  n=$(find "$d" -name '*.whl' | wc -l)
  src=$(find "$d" -type f ! -name '*.whl' | wc -l)
  printf "  %-8s %3d 个 wheel" "$(basename "$d")" "$n"
  [[ "$src" -gt 0 ]] && printf "  ${C_Y}(%d 个源码包, 需目标机 gcc)${C_OFF}" "$src"
  echo
done
[[ -n "${FAILED_VERSIONS// /}" ]] && warn "以下版本未成功：$FAILED_VERSIONS"

uname -m > "$OUT_DIR/ARCH"

# ---------- NiFi 镜像 ----------
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  if ! docker image inspect "$NIFI_IMG" >/dev/null 2>&1; then
    warn "本地没有 $NIFI_IMG，尝试构建..."
    (cd "$PROJECT_DIR/docker/nifi" && docker build -t "$NIFI_IMG" .) || { err "镜像构建失败"; exit 1; }
  fi
  NIFI_TAR="$OUT_DIR/iot-nifi-python-${STAMP}.tar"
  # 清理历史镜像包，否则多个 1.6G 文件会被一起打进交付包，体积翻倍
  rm -f "$OUT_DIR"/iot-nifi-python-*.tar
  log "导出 NiFi 镜像 -> $NIFI_TAR（约 1.6GB）"
  docker save -o "$NIFI_TAR" "$NIFI_IMG"
fi

# ---------- 汇总 ----------
echo
echo "======================================"
echo -e "${C_G}离线资源准备完成${C_OFF}"
echo "  wheel 版本目录 : $(find "$WHEELS_ROOT" -maxdepth 1 -type d -name 'py*' | wc -l) 个"
echo "  wheel 总大小   : $(du -sh "$WHEELS_ROOT" | cut -f1)"
echo "  CPU 架构       : $(cat "$OUT_DIR/ARCH")"
[[ -f "$OUT_DIR/iot-nifi-python-${STAMP}.tar" ]] && \
  echo "  NiFi 镜像      : $(du -sh "$OUT_DIR/iot-nifi-python-${STAMP}.tar" | cut -f1)"
echo
echo "  部署时会自动检测目标机 Python 版本，选对应 wheel 安装 —— 无需预先知道版本"
echo
echo "下一步： ./make_offline_package.sh"
echo "======================================"
