#!/bin/bash
# ============================================================
# 在 ubuntu:<版本> 容器内运行：抓「SQL Server ODBC 依赖闭包」的 deb 到 /out
#
#   container_fetch_odbc.sh <codename> <版本号> [容器内驱动 deb 路径]
#     例：container_fetch_odbc.sh jammy 22.04 /drv.deb
#
# 为什么要在容器里做：apt --download-only 会把「依赖闭包」一并抓下来，
#   在干净的 ubuntu:<版本> 容器里抓，闭包才完整（和裸机接近），
#   而且不同发行版必须各抓一套 —— unixODBC 的包名在 20.04 与 22.04+ 完全不同：
#     20.04(focal) ：libodbc1 (+odbcinst1debian2)
#     22.04(jammy)/24.04(noble)：libodbc2 + libodbcinst2 + odbcinst + unixodbc-common
#   拿 20.04 那套去 22.04 装，两个包会抢同一个 /usr/lib/x86_64-linux-gnu/libodbc.so.2.0.0
#   → dpkg「正试图覆盖…」→ unixODBC 装坏（真机实测过）。
#
# 抓取策略（两条腿）：
#   ① 微软源里【有】该发行版的 msodbcsql17 → 直接抓 msodbcsql17 的完整闭包（含驱动）
#   ② 微软源里【没有】（典型：24.04 只发布了 msodbcsql18）→ 抓 unixODBC 闭包，
#      再用包内那份驱动 deb（在打包机上补进目录）
# ============================================================
set -uo pipefail

CODE="${1:?缺少 codename}"
MSVER="${2:?缺少版本号}"
DRV_IN="${3:-}"

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
rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*.deb 2>/dev/null

# ---------- 1) 选一个可用的 Ubuntu 源 ----------
OK=""
for m in "${MIRRORS[@]}"; do
  echo "  [容器] 尝试源：$m"
  write_sources "$m"
  apt-get update -qq >/tmp/upd.log 2>&1 || true
  if apt-get -s install -y --no-install-recommends unixodbc >/dev/null 2>&1; then
    OK="$m"; break
  fi
  echo "  [容器] 不可用：$(grep -m1 -E 'Could not|Unable to locate|Temporary failure' /tmp/upd.log || echo '未知原因')"
done

if [[ -z "$OK" ]]; then
  echo "  [容器] 常规源均失败，尝试关闭 HTTPS 证书校验..."
  write_sources "https://mirrors.aliyun.com/ubuntu"
  APTOPT=(-o Acquire::https::Verify-Peer=false -o Acquire::https::Verify-Host=false)
  apt-get "${APTOPT[@]}" update -qq >/tmp/upd2.log 2>&1 || true
  if apt-get "${APTOPT[@]}" -s install -y --no-install-recommends unixodbc >/dev/null 2>&1; then
    OK="https://mirrors.aliyun.com/ubuntu (证书校验已关闭)"
    APT_EXTRA=("${APTOPT[@]}")
  fi
fi
[[ -n "$OK" ]] || { echo "  [容器] 所有镜像源都失败 —— 请检查打包机网络/DNS"; tail -5 /tmp/upd.log 2>/dev/null; exit 1; }
echo "  [容器] Ubuntu 源：$OK"

# 驱动 .so 真正链接、但 control 文件【没声明】的运行时库：
#   libmsodbcsql-17.so 需要 libgssapi_krb5.so.2（Kerberos GSSAPI，驱动自己 Depends 里只写了 libkrb5-3）+
#   libssl.so.3（由 openssl 提供）。不显式带上，装完 odbcinst -q -d 依然 "通过"，
#   但真去连 SQL Server 时会报「驱动加载失败」—— 属于典型的"看着装好了其实不能用"。
RUNTIME_LIBS="libgssapi-krb5-2 openssl"

# ---------- 2) 加微软源（抓驱动本体用） ----------
# 两个必备前提（踩过：
#   ① 基础镜像默认【不带 CA 证书】→ 访问 https://packages.microsoft.com 直接握手失败
#      （apt 只会默默忽略该源，然后"看起来一切正常"地抓到一份不含驱动的闭包）
#   ② apt 必须能验证该源的 GPG 签名才采信它的索引（实测 [trusted=yes] 并不能绕过
#      InRelease 的签名校验，会报 NO_PUBKEY EB3E94ADBE1229CF 并忽略整个源）
apt-get install -y -qq --no-install-recommends curl gnupg >/dev/null 2>&1 || true
# ca-certificates 只在【确实没有 CA 包】时装：打包脚本会挂载宿主机的 CA（只读），
# 此时再装它会因 postinst 写不了那个只读文件而让 apt 返回非 0（假失败，看着吓人）。
if [[ ! -s /etc/ssl/certs/ca-certificates.crt ]]; then
  apt-get install -y -qq --no-install-recommends ca-certificates >/dev/null 2>&1 || true
  update-ca-certificates >/dev/null 2>&1 || true
fi
rm -f /var/cache/apt/archives/*.deb 2>/dev/null

if [[ -s /etc/ssl/certs/ca-certificates.crt ]]; then
  echo "  [容器] CA 证书就绪"
else
  echo "  [容器] ⚠️ 容器内没有 CA 证书 → 抓不了微软源（打包机请挂载宿主机的 ca-certificates.crt）"
fi

MSKEY_OK=""
if command -v curl >/dev/null 2>&1 && command -v gpg >/dev/null 2>&1; then
  curl -fsSL https://packages.microsoft.com/keys/microsoft.asc 2>/dev/null \
    | gpg --dearmor --yes -o /etc/apt/trusted.gpg.d/microsoft.gpg 2>/dev/null && MSKEY_OK=1
fi
# 兜底：curl/gpg 不在就用 apt-key（20.04/22.04 有；24.04 已移除 apt-key）
if [[ -z "$MSKEY_OK" ]] && command -v apt-key >/dev/null 2>&1; then
  apt-key adv --fetch-keys https://packages.microsoft.com/keys/microsoft.asc >/dev/null 2>&1 && MSKEY_OK=1
fi
# 光看退出码不够：curl 失败时管道里 gpg 仍可能返回 0（导出一份空 keyring）
if [[ -n "$MSKEY_OK" && -s /etc/apt/trusted.gpg.d/microsoft.gpg ]]; then
  echo "  [容器] 已导入微软源公钥（$(stat -c %s /etc/apt/trusted.gpg.d/microsoft.gpg) 字节）"
else
  echo "  [容器] ⚠️ 微软源公钥导入失败 —— 将无法从微软源抓驱动/闭包"
fi

echo "deb [trusted=yes] https://packages.microsoft.com/ubuntu/${MSVER}/prod $CODE main" \
  > /etc/apt/sources.list.d/mssql-release.list

# 微软源的 CDN 偶尔会抽一下 → 重试 3 次再判定，避免「静默降级到兜底路径」
HAS_MS=""
for i in 1 2 3; do
  apt-get update -qq >/tmp/ms.log 2>&1 || true
  # ⚠️ 这里【不能】写 `apt-cache policy x | grep -q ...`：grep -q 命中即退出 →
  #    apt-cache 还在写后续行就吃到 SIGPIPE → 在 set -o pipefail 下整条管道被判失败
  #    （实测 rc=141，导致"明明有候选版本却判定没有"，行为时好时坏）。
  #    改成先把输出收进变量，再对 here-string 做 grep。
  _POLICY="$(apt-cache policy msodbcsql17 2>/dev/null || true)"
  if grep -qE 'Candidate: [0-9]' <<<"$_POLICY"; then
    HAS_MS=1; break
  fi
  echo "  [容器] 微软源第 $i 次未就绪，重试..."
  sleep 3
done
if [[ -z "$HAS_MS" ]]; then
  echo "  [容器] 微软源不可用，最后几行日志："
  grep -aE 'W:|E:' /tmp/ms.log 2>/dev/null | tail -3 | sed 's/^/      /'
  # focal（20.04）上「回退去抓 Ubuntu 官方 unixODBC」会装到 2.3.6，比包内通用包的
  # 2.3.11-1【更旧】→ dpkg 拒绝降级 → 一堆告警。所以这里宁可不产出，让打包机重试。
  if [[ "${4:-}" == "no-fallback" ]]; then
    echo "  [容器] 本发行版不允许回退（回退会产出比包内更旧的 unixODBC）→ 放弃本发行版"
    exit 2
  fi
fi

# ---------- 3) 抓闭包 ----------
# ⚠️ 这里【不能】用 `apt-get install --download-only`：它会跳过【容器里已经装好】的依赖
#    （libc6 / libltdl7 / libreadline8 / libkrb5-3 / openssl … 基础镜像里都有），
#    结果是抓出来的闭包只有六七个包 —— 而目标机上未必装过这些，一装就缺依赖。
#    所以改成：先把【递归依赖清单】取全，再逐个 apt-get download（不看本机装没装）。
rm -rf /var/lib/apt/lists/partial/* 2>/dev/null || true
mkdir -p /dl && rm -f /dl/*.deb
cd /dl || exit 1     # apt-get download 落在当前目录

if [[ -n "$HAS_MS" ]]; then
  echo "  [容器] 微软源有本发行版的 msodbcsql17 → 抓「驱动 + 依赖」完整闭包"
  ROOT_PKGS="msodbcsql17 $RUNTIME_LIBS"
else
  echo "  [容器] 微软源无本发行版的 msodbcsql17 → 只抓 unixODBC 闭包（驱动用包内那份）"
  DEPS=""
  if [[ -n "$DRV_IN" && -f "$DRV_IN" ]]; then
    # 取出驱动 deb 声明的依赖（去掉版本约束），保证闭包覆盖驱动所需的一切
    DEPS="$(dpkg-deb -f "$DRV_IN" Depends 2>/dev/null | sed 's/([^)]*)//g; s/,/ /g' || true)"
  fi
  ROOT_PKGS="unixodbc odbcinst $RUNTIME_LIBS $DEPS"
fi

export LC_ALL=C     # 排序稳定（en_US 下 perl 会排到 perl-base 前面那类坑）
PKG_LIST="$(apt-cache depends --recurse --no-recommends --no-suggests --no-conflicts \
            --no-breaks --no-replaces --no-enhances $ROOT_PKGS 2>/dev/null \
            | grep '^[a-zA-Z0-9]' | sort -u || true)"
echo "  [容器] 递归依赖清单：$(grep -c . <<<"$PKG_LIST") 个包名"
for p in $PKG_LIST; do
  apt-get download "$p" >/dev/null 2>&1 || true
done

cp /dl/*.deb /out/ 2>/dev/null
echo "  [容器] deb 个数: $(ls /out/*.deb 2>/dev/null | wc -l)"

# ---------- 4) 自检：驱动 .so 的动态库依赖是否齐全 ----------
# 【为什么必须做】odbcinst -q -d 只能证明「驱动注册了」，不能证明「驱动能加载」。
# 驱动的 control 里没写全它真正链接的库（如 libgssapi_krb5.so.2），漏掉时安装照样绿，
# 真连 SQL Server 才报「驱动加载失败」。这一关把闭包装进容器跑 ldd，能提前抓住。
if [[ -n "$DRV_IN" && -f "$DRV_IN" ]]; then
  dpkg -i --force-depends /dl/*.deb >/dev/null 2>&1 || true
  dpkg -i --force-depends "$DRV_IN" >/dev/null 2>&1 || true
  dpkg --configure -a >/dev/null 2>&1 || true
  _SO="$(ls /opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17*.so* 2>/dev/null | head -1 || true)"
  if [[ -n "$_SO" ]]; then
    _MISS="$(ldd "$_SO" 2>/dev/null | grep 'not found' || true)"
    if [[ -n "$_MISS" ]]; then
      echo "  [容器] ⚠️ 驱动仍缺运行时库（请把对应包补进 RUNTIME_LIBS 后重跑）："
      printf '%s\n' "$_MISS" | sed 's/^/      /'
    else
      echo "  [容器] 自检通过：驱动 .so 的动态库依赖完整（ldd 无 not found）"
    fi
  fi
fi

du -sh /out
