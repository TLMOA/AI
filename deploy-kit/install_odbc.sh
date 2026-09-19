#!/usr/bin/env bash
# ============================================================
# 离线安装 SQL Server ODBC 驱动（可选）
#
# 只有「从 SQL Server 导数据」才需要。其它数据源（MySQL/PostgreSQL/
# Oracle/Hive/HBase/HDFS）靠 Python 驱动即可，不需要本脚本。
#
#   sudo ./install_odbc.sh
#
# 原理：包内已备好各 Ubuntu 版本的 msodbcsql17 + 依赖 deb，
#       按本机发行版自动挑选，dpkg -i 安装，全程不需要外网。
# ============================================================
set -uo pipefail

KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEB_DIR="$KIT_DIR/offline-assets/deb"
MANIFEST="$KIT_DIR/tools/manifest.sh"          # 归属台账（记录/判定「什么是我们装的」）
# 台账里有没有这个包的记录？有 = 本平台装的 → 可以放心打破
_is_ours() { bash "$MANIFEST" is_ours_pkg "$1" 2>/dev/null; }
C_G='\033[0;32m'; C_Y='\033[0;33m'; C_R='\033[0;31m'; C_OFF='\033[0m'

log()  { echo -e "${C_G}[odbc]${C_OFF} $*"; }
warn() { echo -e "${C_Y}[warn]${C_OFF} $*"; }
err()  { echo -e "${C_R}[err]${C_OFF} $*" >&2; }

[[ $EUID -eq 0 ]] || { err "请用 root 运行：sudo ./install_odbc.sh"; exit 1; }
[[ -d "$DEB_DIR" ]] || { err "找不到离线 deb 目录：$DEB_DIR"; exit 1; }

# 已装就跳过（先收进变量再 grep：`cmd | grep -q` 在 pipefail 下会被 SIGPIPE 误判成「没装」）
DRV_LIST="$(command -v odbcinst >/dev/null 2>&1 && odbcinst -q -d 2>/dev/null || true)"
if grep -qi "SQL Server" <<<"$DRV_LIST"; then
  log "ODBC Driver for SQL Server 已安装，无需重复操作"
  odbcinst -q -d | sed 's/^/       /'
  exit 0
fi

# 识别发行版
VER=""
if [[ -f /etc/os-release ]]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  VER="${VERSION_ID:-}"
fi
log "检测到系统版本：${VER:-未知}"

DRV_DEB=""
if [[ -n "$VER" && -f "$DEB_DIR/msodbcsql17_${VER}_amd64.deb" ]]; then
  DRV_DEB="$DEB_DIR/msodbcsql17_${VER}_amd64.deb"
else
  # 回退：选取包内任意一个并在完成后提示
  DRV_DEB="$(ls "$DEB_DIR"/msodbcsql17_*_amd64.deb 2>/dev/null | head -1 || true)"
  if [[ -z "$DRV_DEB" ]]; then
    err "包内没有 msodbcsql17 的 deb，请先在打包机跑 prepare_offline_assets.sh"
    exit 1
  fi
  warn "没有与 ${VER:-未知} 精确匹配的 deb，将尝试使用 $(basename "$DRV_DEB")"
fi

# 1) 优先用「本发行版专属」的 ODBC deb 目录。
# 【为什么】unixODBC 在 20.04 和 22.04 里包名不一样：
#   20.04(focal)：libodbc1（里面含 libodbcinst.so.2）
#   22.04(jammy)/24.04(noble)：拆成 libodbc2 + libodbcinst2 + odbcinst1debian2
# 20.04 时代的 deb 在 22.04 上装不上（真机实测：odbcinst 依赖
# odbcinst1debian2 不存在 → 连锁 unixodbc/msodbcsql17 全部配置失败）。
DISTRO_KEY=""
if [[ -f /etc/os-release ]]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  DISTRO_KEY="${ID}-${VERSION_ID}"
fi
DISTRO_DIR="$KIT_DIR/offline-assets/deb/odbc/$DISTRO_KEY"

# 【现代 unixODBC 体系】ubuntu 22.04 起把 unixODBC 拆成 libodbc2 + libodbcinst2 + odbcinst1debian2，
# 与 20.04 风格的 libodbc1 **互斥**（争同一个文件 /usr/lib/x86_64-linux-gnu/libodbc.so.2.0.0）。
# 所以这些发行版上【绝不】回退到通用包，否则会把 unixODBC 整套装坏（2026-09-15 真机实测）。
MODERN_ODBC=false
case "$DISTRO_KEY" in
  ubuntu-22.04|ubuntu-24.04|debian-12|debian-13) MODERN_ODBC=true ;;
esac

# ============================================================
# 输出策略（这脚本要装 39 个包，dpkg 的输出有几百行）：
#   · dpkg / apt 的原始输出【全部写进日志文件】，不糊到屏幕上
#   · 终端只打：正在做什么、结论是什么、失败了怎么办
#   · 失败时才回显日志里最关键的几行，运维一眼能看懂
# ============================================================
ODBC_LOG="${ODBC_LOG:-/tmp/iot-odbc-install.log}"
: > "$ODBC_LOG" 2>/dev/null || true        # 每轮从空日志开始（覆盖上次的）

# 列出闭包里「还没装上」的包（输出为空 = 全部就绪）
_closure_pending() {
  local f p st out=""
  for f in "$DISTRO_DIR"/*.deb; do
    [[ -f "$f" ]] || continue
    p="$(dpkg-deb -f "$f" Package 2>/dev/null || true)"
    [[ -n "$p" ]] || continue
    st="$(dpkg-query -W -f='${Status}' "$p" 2>/dev/null || true)"
    grep -q "install ok installed" <<<"$st" || out="$out $p"
  done
  printf '%s' "${out# }"
}

# 失败时只回显关键几行（错误/覆盖/依赖），没有就回显末尾几行
_tail_key_lines() {
  local lines=""
  [[ -s "$ODBC_LOG" ]] || return 0
  lines="$(grep -aE "错误|覆盖|依赖|未配置|已损坏|error|Error" "$ODBC_LOG" 2>/dev/null | tail -n 8 || true)"
  [[ -n "$lines" ]] || lines="$(tail -n 8 "$ODBC_LOG" 2>/dev/null || true)"
  [[ -n "$lines" ]] && printf '%s\n' "$lines" | sed 's/^/       /'
  return 0
}

# 【先清理残留】机器上可能有「已解包未配置」的包（真机实测：unixodbc/odbcinst 卡在
# 未配置 → msodbcsql17 报「依赖尚未配置」装不上）。先统一配置一遍再装。
export DEBIAN_FRONTEND=noninteractive
if command -v dpkg >/dev/null 2>&1; then
  dpkg --configure -a >>"$ODBC_LOG" 2>&1 || true
fi

# 【升级路径 · 真机实测踩到的坑 2026-09-15】
# 22.04/24.04 的 unixODBC 是 libodbc2 体系；但本平台【早期版本】的安装脚本在 22.04 上走过
# 「通用包」回退路径，装上了 20.04 风格的 libodbc1（版本 2.3.11-1，来自 offline-assets/deb/ 根目录）。
# libodbc1 与 libodbc2 抢同一个文件 /usr/lib/x86_64-linux-gnu/libodbc.so.2.0.0 →
#   dpkg: 正试图覆盖 ... 它同时被包含于软件包 libodbc1:amd64
# → libodbc2 装不上 → unixodbc 依赖不满足「尚未配置」→ msodbcsql17 也「尚未配置」
# → 最后 odbcinst -q -d 看不到 SQL Server 驱动（真机症状：日志里一连串「仍未被配置」）。
# 处理：只清理【版本与本包通用包一致】的 libodbc1（证明是本平台装的），不动别人装的东西。
OUR_L1_VER=""
_OUR_L1_DEB="$(ls "$DEB_DIR"/libodbc1_*.deb 2>/dev/null | head -1 || true)"
if [[ -n "$_OUR_L1_DEB" ]] && command -v dpkg-deb >/dev/null 2>&1; then
  OUR_L1_VER="$(dpkg-deb -f "$_OUR_L1_DEB" Version 2>/dev/null || true)"
fi
[[ -n "$OUR_L1_VER" ]] || OUR_L1_VER="2.3.11-1"   # 兜底：本平台早期通用包的版本号

_L1_STATUS="$(command -v dpkg-query >/dev/null 2>&1 && dpkg-query -W -f='${Status}' libodbc1 2>/dev/null || true)"
if [[ "$MODERN_ODBC" == "true" ]] && grep -qE "^install ok" <<<"$_L1_STATUS"; then
  _L1="$(dpkg-query -W -f='${Version}' libodbc1 2>/dev/null || true)"
  # 归属判定：两条轨道，任一命中即认定「是本平台装的」
  #   ① 台账（/var/lib/iot-platform/pkg-new.list）里有 libodbc1 → 铁证（旧部署自己记下的）
  #   ② 版本与包内那份 focal 通用包一致 → 旁证（老机器/老脚本可能没记台账）
  # 两条都不满足 = 这是别人的东西 → 只告警，绝不擅动（会被人家别的程序依赖）
  _WHY=""
  if _is_ours libodbc1; then
    _WHY="归属台账里有记录 —— 是本平台早期部署装的"
  elif [[ "$_L1" == "$OUR_L1_VER" ]]; then
    _WHY="版本 $_L1 与包内 focal 通用包一致（台账无记录，按版本旁证）"
  fi
  if [[ -n "$_WHY" ]]; then
    warn "检测到本平台早期部署留下的 libodbc1 ($_L1)：$_WHY"
    warn "  它与 $DISTRO_KEY 的 libodbc2 抢同一个 .so → 按本包声明【打破它】，改用专属闭包重建"
    dpkg --purge --force-depends libodbc1 >>"$ODBC_LOG" 2>&1 || true
    dpkg --configure -a >>"$ODBC_LOG" 2>&1 || true
  else
    warn "机器上已有 libodbc1 ($_L1)，与 $DISTRO_KEY 的 libodbc2 冲突，但它【不是本平台装的】"
    warn "  （台账无记录，版本也不是包内的 $OUR_L1_VER）→ 未自动移除，以免弄坏依赖它的别的程序"
    warn "  若确认它没别处依赖，人工执行： sudo dpkg --purge --force-depends libodbc1   然后重跑本脚本"
  fi
fi

# ============================================================
# 【必须排在「装闭包」之前】预接受 msodbcsql17 的许可协议（EULA）
#   msodbcsql17 的 preinst（**解包阶段**就会执行）里有：
#       db_get msodbcsql/ACCEPT_EULA
#       if [ "$RET" != "true" ] && [ "$ACCEPT_EULA" != "y" ] && [ "$ACCEPT_EULA" != "Y" ]; then ... exit 1
#   没预接受的话，闭包里的 msodbcsql17 会在解包阶段就被拒 → 留下「未安装」状态 →
#   后面就会冒出一串「以下包未装好：msodbcsql17」的误导告警（2026-09-15 容器实测定位）。
#   注意 debconf 模板前缀是 msodbcsql/ ，不是 msodbcsql17/（写错名字会照样弹窗）。
# ============================================================
if command -v debconf-set-selections >/dev/null 2>&1; then
  echo "msodbcsql msodbcsql/ACCEPT_EULA boolean true" | debconf-set-selections 2>/dev/null || true
  echo "msodbcsql msodbcsql/accept_eula boolean true" | debconf-set-selections 2>/dev/null || true
fi
export ACCEPT_EULA=Y
log "已预接受 ODBC 驱动许可协议（EULA），安装过程不会弹窗"

if [[ -d "$DISTRO_DIR" ]] && compgen -G "$DISTRO_DIR/*.deb" >/dev/null 2>&1; then
  _N_DEB="$(ls "$DISTRO_DIR"/*.deb | wc -l)"
  log "使用 $DISTRO_KEY 专属 ODBC deb（$_N_DEB 个，依赖+驱动一体）"
  log "  正在安装依赖包（dpkg 输出已写入 $ODBC_LOG）..."
  # 一次性全部交给 dpkg（含依赖和驱动），依赖顺序由 dpkg 处理
  dpkg -i "$DISTRO_DIR"/*.deb >>"$ODBC_LOG" 2>&1 || true
  # 【多轮 configure】闭包里 msodbcsql17 依赖 unixodbc，而 dpkg 是按「给定顺序」配置的 ——
  # 一轮往往配不完（容器实测：msodbcsql17 排在 unixodbc 前面 → 要等下一轮才配好）。
  # 这不是错误，多跑几轮就干净了；否则会冒出「依赖包仍未就绪」这种吓人又误导的告警。
  for _r in 1 2 3 4 5; do
    dpkg --configure -a >>"$ODBC_LOG" 2>&1 || true
  done
  # 兜底：若还有包没装上（典型是「正试图覆盖 xxx，它同时被包含于软件包 yyy」这类文件冲突），
  # 就用 --force-overwrite 整组重来一遍（后装的包覆盖同名文件）。
  _PEND="$(_closure_pending)"
  if [[ -n "$_PEND" ]]; then
    warn "以下包未装好：$_PEND"
    warn "  → 用 --force-overwrite 整组重试一次（详情见 $ODBC_LOG）"
    dpkg -i --force-overwrite "$DISTRO_DIR"/*.deb >>"$ODBC_LOG" 2>&1 || true
    for _r in 1 2 3 4 5; do
      dpkg --configure -a >>"$ODBC_LOG" 2>&1 || true
    done
    _PEND="$(_closure_pending)"
  fi
  if [[ -n "$_PEND" ]]; then
    warn "依赖包仍未就绪：$_PEND（继续尝试装驱动；若最终失败见脚本末尾提示）"
  else
    log "  依赖包已就绪 ✓"
  fi
else
  warn "没有 $DISTRO_KEY 专属的 ODBC deb（现有目录：$(ls "$KIT_DIR/offline-assets/deb/odbc" 2>/dev/null | tr '\n' ' ')）"
  warn "  打包方可执行 bash prepare_odbc_assets.sh $VER 生成该发行版的完整闭包（断网机器也能装）"
  if [[ "$MODERN_ODBC" == "true" ]]; then
    # 22.04/24.04 这类发行版自带 libodbc2 体系，通用包里的 libodbc1 会和它抢同一个 .so，
    # 装上去只会把 unixODBC 弄坏（真机实测），所以这里坚决不回退，只给可行的人工做法。
    warn "$DISTRO_KEY 属于 libodbc2 拆分体系，通用包（20.04 风格 libodbc1）会与它冲突，故【不回退】"
    warn "请改用下面两条（需要能连软件源；本场景无法离线完成）："
    echo "        sudo apt-get install -y unixodbc"
    echo "        sudo dpkg -i $DEB_DIR/$(basename "$DRV_DEB")"
  else
    log "回退到通用包（可能不匹配本发行版）..."
    # ---- 旧逻辑：通用目录（20.04 时代的包）----
    for f in libodbc1 odbcinst unixodbc; do
      deb="$(ls "$DEB_DIR"/${f}_*.deb 2>/dev/null | head -1 || true)"
      [[ -n "$deb" ]] || continue
      dpkg -i "$deb" >>"$ODBC_LOG" 2>&1 || true
    done
    dpkg --configure -a >>"$ODBC_LOG" 2>&1 || true
  fi
fi

# 2) 安装驱动本体（EULA 已在「装闭包之前」预接受；正常路径下闭包已把驱动装好，
#    这里只作兜底与自愈：换发行版 / 换包 / 上一步部分失败时，靠这一条补上）
# 若上面的专属目录已经把驱动装好了，这里会显示"已是最新"，无副作用
if [[ -d "$DISTRO_DIR" ]] && compgen -G "$DISTRO_DIR/msodbcsql17*.deb" >/dev/null 2>&1; then
  DRV_DEB="$(ls "$DISTRO_DIR"/msodbcsql17*.deb | head -1)"
  log "驱动来自专属目录：$(basename "$DRV_DEB")"
fi

log "安装驱动 $(basename "$DRV_DEB")（已预接受 EULA，不会弹窗）..."
if ! dpkg -i "$DRV_DEB" >>"$ODBC_LOG" 2>&1; then
  warn "驱动安装报告依赖问题，尝试修复已装依赖..."
  if [[ -d "$DISTRO_DIR" ]] && compgen -G "$DISTRO_DIR/*.deb" >/dev/null 2>&1; then
    dpkg -i --force-overwrite "$DISTRO_DIR"/*.deb >>"$ODBC_LOG" 2>&1 || true
  elif [[ "$MODERN_ODBC" != "true" ]]; then
    # 只有老体系才允许用通用目录兜底；22.04+ 用通用包会把 unixODBC 装坏
    dpkg -i "$DEB_DIR"/*.deb >>"$ODBC_LOG" 2>&1 || true
  fi
fi
dpkg --configure -a >>"$ODBC_LOG" 2>&1 || true

# 把实际安装的包记入【归属台账】（部署期判定归属、卸载期回收都靠它）
_OWNED=()
for p in msodbcsql17 unixodbc unixodbc-common odbcinst odbcinst1debian2 \
         libodbc2 libodbcinst2 libodbc1 libltdl7; do
  dpkg -s "$p" >/dev/null 2>&1 || continue
  _OWNED+=("$p")
done
if [[ ${#_OWNED[@]} -gt 0 ]]; then
  bash "$MANIFEST" record_pkg "${_OWNED[@]}" 2>/dev/null || true
  log "  已记入归属台账：${#_OWNED[@]} 个包（$MANIFEST show 可查看）"
fi
# 驱动的注册文件也记一笔（ODBC 的 odbcinst.ini/driver 目录），供卸载与审计
bash "$MANIFEST" record_file /etc/odbcinst.ini /etc/odbc.ini /etc/ODBCDataSources 2>/dev/null || true

# 3) 验证
echo
DRV_LIST="$(command -v odbcinst >/dev/null 2>&1 && odbcinst -q -d 2>/dev/null || true)"
if grep -qi "SQL Server" <<<"$DRV_LIST"; then
  log "安装成功，已注册的 ODBC 驱动："
  odbcinst -q -d | sed 's/^/       /'
  echo
  log "现在可以在「数据库导出」里选 SQL Server 数据源了"
else
  err "安装后仍未检测到 SQL Server ODBC 驱动"
  _PEND="$(_closure_pending)"
  [[ -n "$_PEND" ]] && err "  未装好的依赖包：$_PEND"
  err "  日志里的关键几行："
  _tail_key_lines
  echo "      完整日志：$ODBC_LOG"
  echo "      手修（最常见原因：机器上残留 20.04 风格的 libodbc1，与 libodbc2 抢同一个 .so）："
  echo "        sudo dpkg --purge --force-depends libodbc1"
  echo "        sudo bash $KIT_DIR/install_odbc.sh"
  echo "      若仍失败，把下面两条的输出发给我们："
  echo "        dpkg -l | grep -E 'msodbc|unixodbc|libodbc' ; dpkg --audit"
  exit 1
fi
