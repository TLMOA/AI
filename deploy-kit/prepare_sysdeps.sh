#!/usr/bin/env bash
# ============================================================
# 预下载「系统依赖」的 deb 闭包（供完全离线部署用）
#
#   ./prepare_sysdeps.sh               # 默认只做 Ubuntu 22.04
#   ./prepare_sysdeps.sh 20.04 22.04 24.04
#
# 产物：offline-assets/deb/sysdeps/ubuntu-<版本>/*.deb
#   deploy 时 install_sysdeps.sh 会按本机发行版自动挑选，dpkg 离线安装。
#
# 为什么在容器里做：--download-only 会把「依赖闭包」一起抓下来，
#   在干净的 ubuntu:<版本> 容器里抓，闭包才完整（和裸机差不多）。
# 源用阿里云 HTTPS（国内快，且 80 端口被挡的内网也能用）。
# ============================================================
set -uo pipefail

KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_ROOT="$KIT_DIR/offline-assets/deb/sysdeps"

# 需要预置的系统包
PKGS="python3 python3-venv python3-pip nginx mysql-server docker.io curl ca-certificates"

cname() {
  case "$1" in
    20.04) echo focal ;;
    22.04) echo jammy ;;
    24.04) echo noble ;;
    *)     echo jammy ;;
  esac
}

# docker compose 的包名各版本不同（20.04 是 v1 的 docker-compose，22.04+ 是 v2 的 docker-compose-v2）
compose_pkg() {
  case "$1" in
    20.04) echo docker-compose ;;
    *)     echo docker-compose-v2 ;;
  esac
}

VERSIONS=("$@")
[[ ${#VERSIONS[@]} -gt 0 ]] || VERSIONS=(22.04)

ARCH="$(dpkg --print-architecture 2>/dev/null || uname -m)"

warn_note() { echo -e "\033[0;33m[warn]\033[0m $*"; }

mkdir -p "$OUT_ROOT"
echo "[prepare-sysdeps] CPU 架构：$ARCH"
echo "[prepare-sysdeps] 待处理：${VERSIONS[*]}"
echo "[prepare-sysdeps] 容器内会依次尝试阿里云/清华/中科大/华为/官方 多个镜像源"

FETCHER="$KIT_DIR/tools/container_fetch_debs.sh"
[[ -f "$FETCHER" ]] || { echo "缺少 $FETCHER"; exit 1; }

# 复用宿主机的 CA 证书：有些办公网/沙箱有 TLS 代理，
# 容器默认不信任其证书链，挂载宿主机的证书包即可正常走 HTTPS。
CA_ARG=()
if [[ -f /etc/ssl/certs/ca-certificates.crt ]]; then
  CA_ARG=(-v /etc/ssl/certs/ca-certificates.crt:/etc/ssl/certs/ca-certificates.crt:ro)
fi

for v in "${VERSIONS[@]}"; do
  key="ubuntu-$v"
  dest="$OUT_ROOT/$key"
  code="$(cname "$v")"
  compose="$(compose_pkg "$v")"
  echo
  echo "[prepare-sysdeps] === Ubuntu $v ($code) -> $dest ==="
  mkdir -p "$dest"
  rm -f "$dest"/*.deb

  docker run --rm \
    -v "$dest:/out" \
    "${CA_ARG[@]}" \
    -v "$FETCHER:/fetch.sh:ro" \
    "ubuntu:$v" bash /fetch.sh "$code" "$PKGS $compose"

  # 抓不到就清掉空目录，免得部署时误判「有离线包」
  n="$(ls "$dest"/*.deb 2>/dev/null | wc -l)"
  if [[ "$n" -eq 0 ]]; then
    warn_note "Ubuntu $v 未抓到任何 deb（容器拉取或源不可用），已清理 $dest"
    rm -rf "$dest"
    continue
  fi

  # 架构标记：安装时用来确认离线 deb 和本机 CPU 架构一致（不一致就不乱装）
  echo "$ARCH" > "$dest/ARCH"
  echo "  架构标记: $ARCH   (deb $n 个)"
done

echo
echo "[prepare-sysdeps] 完成，产物："
du -sh "$OUT_ROOT"/* 2>/dev/null
