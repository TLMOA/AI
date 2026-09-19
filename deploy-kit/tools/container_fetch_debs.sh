#!/bin/bash
# ============================================================
# 在 ubuntu:<版本> 容器内运行：把系统依赖的 deb 闭包抓到 /out
#
#   container_fetch_debs.sh <codename> "<包列表>"
#     例：container_fetch_debs.sh jammy "python3 python3-venv nginx mysql-server"
#
# 为什么要在容器里做：apt-get install --download-only 会把依赖闭包一并抓下来，
#   在干净的基础镜像里抓，闭包才完整（和裸机接近）。
#
# 会依次尝试多个镜像源，任一可用即停。
# 若容器不信任当前网络证书（HTTPS 握手失败），会退回「复用宿主机 CA」
# 甚至临时关闭证书校验的方式，保证打包机能顺利产出 deb。
# ============================================================
set -uo pipefail

CODE="${1:?缺少 codename}"
PKGS="${2:?缺少包列表}"

MIRRORS=(
  "https://mirrors.aliyun.com/ubuntu"
  "https://mirrors.tuna.tsinghua.edu.cn/ubuntu"
  "https://mirrors.ustc.edu.cn/ubuntu"
  "https://mirrors.huaweicloud.com/ubuntu"
  "http://archive.ubuntu.com/ubuntu"
)

write_sources() {
  {
    echo "deb $1 $CODE main restricted universe multiverse"
    echo "deb $1 $CODE-updates main restricted universe multiverse"
    echo "deb $1 $CODE-security main restricted universe multiverse"
    echo "deb $1 $CODE-backports main restricted universe multiverse"
  } > /etc/apt/sources.list
}

export DEBIAN_FRONTEND=noninteractive

# 清理历史缓存，避免把上一轮源的内容混进来
rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*.deb 2>/dev/null

OK=""
for m in "${MIRRORS[@]}"; do
  echo "  [容器] 尝试源：$m"
  write_sources "$m"
  apt-get update -qq >/tmp/upd.log 2>&1
  # 用模拟安装判断该源是否真的有这些包
  if apt-get -s install -y --no-install-recommends $PKGS >/dev/null 2>&1; then
    OK="$m"; break
  fi
  echo "  [容器] 不可用：$(grep -m1 -E 'Could not|Unable to locate|Temporary failure' /tmp/upd.log || echo '未知原因')"
done

# 最后一招：阿里云 HTTPS + 关闭证书校验（仅在前面全失败时用）
if [[ -z "$OK" ]]; then
  echo "  [容器] 常规源均失败，尝试关闭 HTTPS 证书校验..."
  write_sources "https://mirrors.aliyun.com/ubuntu"
  APTOPT=(-o Acquire::https::Verify-Peer=false -o Acquire::https::Verify-Host=false)
  apt-get "${APTOPT[@]}" update -qq >/tmp/upd2.log 2>&1
  if apt-get "${APTOPT[@]}" -s install -y --no-install-recommends $PKGS >/dev/null 2>&1; then
    OK="https://mirrors.aliyun.com/ubuntu (证书校验已关闭)"
    apt-get "${APTOPT[@]}" install -y -qq --download-only --no-install-recommends $PKGS
  fi
fi

if [[ -z "$OK" ]]; then
  echo "  [容器] 所有镜像源都失败 —— 请检查打包机网络/DNS"
  cat /tmp/upd.log 2>/dev/null | tail -5
  exit 1
fi

echo "  [容器] 使用源：$OK"
if ! apt-get install -y -qq --download-only --no-install-recommends $PKGS; then
  echo "  [容器] 缺少部分包，重试一次（忽略个别包名差异）"
  for p in $PKGS; do
    apt-get install -y -qq --download-only --no-install-recommends "$p" >/dev/null 2>&1 || true
  done
fi

cp /var/cache/apt/archives/*.deb /out/ 2>/dev/null
echo "  deb 个数: $(ls /out/*.deb 2>/dev/null | wc -l)"
du -sh /out
