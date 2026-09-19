#!/usr/bin/env bash
# ============================================================
# 预下载「SQL Server ODBC（unixODBC）依赖闭包」（供完全离线部署用）
#
#   ./prepare_odbc_assets.sh                    # 默认 22.04 24.04
#   ./prepare_odbc_assets.sh 20.04 22.04 24.04
#
# 产物：offline-assets/deb/odbc/ubuntu-<版本>/*.deb
#   install_odbc.sh 按本机发行版挑目录，dpkg -i 离线安装，全程不需要外网。
#
# 【为什么必须有这个脚本】
#   以前这套资产是【手工】在 20.04/22.04 上抓的，结果只有 ubuntu-22.04 一套完整闭包；
#   20.04/24.04 只剩驱动本体 → 目标机必须联网 apt 才能装 SQL Server ODBC。
#   现在按发行版一次抓全，和 prepare_sysdeps.sh 的思路完全一致。
#
# 注意：unixODBC 的包名在 20.04 与 22.04+ 是两套体系，绝不能互相混用（会抢同一个 .so）。
# ============================================================
set -uo pipefail

KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_ROOT="$KIT_DIR/offline-assets/deb/odbc"
DEB_DIR="$KIT_DIR/offline-assets/deb"

cname() {
  case "$1" in
    20.04) echo focal ;;
    22.04) echo jammy ;;
    24.04) echo noble ;;
    *)     echo jammy ;;
  esac
}

VERSIONS=("$@")
[[ ${#VERSIONS[@]} -gt 0 ]] || VERSIONS=(22.04 24.04)

ARCH="$(dpkg --print-architecture 2>/dev/null || uname -m)"
warn_note() { echo -e "\033[0;33m[warn]\033[0m $*"; }

mkdir -p "$OUT_ROOT"
echo "[prepare-odbc] CPU 架构：$ARCH"
echo "[prepare-odbc] 待处理：${VERSIONS[*]}"

FETCHER="$KIT_DIR/tools/container_fetch_odbc.sh"
[[ -f "$FETCHER" ]] || { echo "缺少 $FETCHER"; exit 1; }

# 复用宿主机 CA：有些办公网/沙箱有 TLS 代理，容器默认不信任其证书链
CA_ARG=()
if [[ -f /etc/ssl/certs/ca-certificates.crt ]]; then
  CA_ARG=(-v /etc/ssl/certs/ca-certificates.crt:/etc/ssl/certs/ca-certificates.crt:ro)
fi

for v in "${VERSIONS[@]}"; do
  key="ubuntu-$v"
  dest="$OUT_ROOT/$key"
  code="$(cname "$v")"
  drv="$DEB_DIR/msodbcsql17_${v}_amd64.deb"      # 包内已有的驱动本体（可能没有）
  echo
  echo "[prepare-odbc] === Ubuntu $v ($code) -> $dest ==="
  mkdir -p "$dest"
  rm -f "$dest"/*.deb

  DRV_ARG=()
  if [[ -f "$drv" ]]; then
    DRV_ARG=(-v "$drv:/drv.deb:ro")
    echo "  包内驱动：$(basename "$drv")"
  else
    warn_note "包内没有 msodbcsql17_${v}_amd64.deb，将只抓 unixODBC 闭包"
  fi

  # focal：Ubuntu 官方 unixODBC(2.3.6) 比包内通用包(2.3.11-1) 更旧，
  # 回退抓到会降级 → 不允许，宁可失败后重跑
  FB=""
  [[ "$code" == "focal" ]] && FB="no-fallback"

  docker run --rm \
    -v "$dest:/out" \
    "${CA_ARG[@]}" \
    "${DRV_ARG[@]}" \
    -v "$FETCHER:/fetch.sh:ro" \
    "ubuntu:$v" bash /fetch.sh "$code" "$v" /drv.deb $FB
  rc=$?
  [[ "$rc" -eq 0 ]] || warn_note "Ubuntu $v 抓取未成功（退出码 $rc），见上面容器输出"

  # 下面一律用 bash 内建匹配（compgen / 数组），不用 ls —— 某些环境里
  # `ls 通配 >/dev/null` 会返回非零，导致「明明抓到了却判成没抓到」的假告警。
  shopt -s nullglob
  debs=("$dest"/*.deb)
  shopt -u nullglob

  has_drv=0; has_ux=0
  for f in "${debs[@]}"; do
    case "$(basename "$f")" in
      msodbcsql17*) has_drv=1 ;;
      unixodbc_*)   has_ux=1 ;;
    esac
  done

  # 兜底：容器里若没抓到驱动（微软源没发布该发行版），把包内那份补进目录
  if [[ "$has_drv" -eq 0 && -f "$drv" ]]; then
    cp "$drv" "$dest/"
    has_drv=1
    debs=("${debs[@]}" "$dest/$(basename "$drv")")
    echo "  已补入包内驱动：$(basename "$drv")"
  fi

  n="${#debs[@]}"
  if [[ "$n" -eq 0 ]]; then
    warn_note "Ubuntu $v 未抓到任何 deb（容器拉取或源不可用），已清理 $dest"
    rm -rf "$dest"
    continue
  fi

  echo "$ARCH" > "$dest/ARCH"
  # 自检：驱动与 unixODBC 必须同时在场，否则这套闭包装了也没用
  if [[ "$has_drv" -eq 1 && "$has_ux" -eq 1 ]]; then
    echo "  自检通过：驱动 + unixODBC 齐备（deb $n 个）"
  else
    warn_note "Ubuntu $v 这套里缺驱动或缺 unixodbc（deb $n 个，has_drv=$has_drv has_ux=$has_ux）—— 请人工看一眼"
  fi
done

echo
echo "[prepare-odbc] 完成，产物："
du -sh "$OUT_ROOT"/* 2>/dev/null
