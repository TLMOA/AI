#!/usr/bin/env bash
# ============================================================
# 生成离线交付包（给没有外网 / 不给 SSH key 的工厂机器）
#
# 用法:
#   ./make_offline_package.sh [输出目录]      # 默认输出到 /home/yhz
#
# 产物:
#   iot-offline-<日期>.tgz   +   校验文件 .sha256
#
# 特点:
#   - 打的是「当前工作区文件」，因此**不需要先 git commit**
#     今天对 auth.py 的修复会直接包含进包里
#   - 自动排除：虚拟环境、git 历史、缓存、日志
#   - **自动排除 v1-backend/data/app.db**（含本机用户账号，绝不能带给工厂）
#   - 保留 real_nifi_conf/（NiFi flow 配置，部署必需）
# ============================================================
set -euo pipefail

KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$KIT_DIR/.." && pwd)"
PROJECT_NAME="$(basename "$PROJECT_DIR")"
# 参数: ./make_offline_package.sh [输出目录] [--no-nifi-image] [--with-hadoop]
#   默认：把 NiFi 镜像一并打进包 —— 只交付【一个文件】，工厂端最省事
#   --no-nifi-image  镜像不打进包（包从 ~1.7G 降到 ~108M），镜像单独传
#   --with-hadoop    额外包含 Hadoop 镜像（+2.7G），仅 HDFS 数据源需要
WITH_HADOOP=false
WITH_IMAGE=true
WITH_SYSDEPS=true
OUT_DIR="/home/yhz"
for a in "$@"; do
  case "$a" in
    --with-hadoop)   WITH_HADOOP=true ;;
    --no-nifi-image) WITH_IMAGE=false ;;
    --no-sysdeps)    WITH_SYSDEPS=false ;;
    *) OUT_DIR="$a" ;;
  esac
done
STAMP="$(date +%Y%m%d)"
PKG_NAME="iot-offline-${STAMP}.tgz"
PKG_PATH="${OUT_DIR}/${PKG_NAME}"

C_GREEN='\033[0;32m'; C_YELLOW='\033[0;33m'; C_OFF='\033[0m'
log()  { echo -e "${C_GREEN}[package]${C_OFF} $*"; }
warn() { echo -e "${C_YELLOW}[warn]${C_OFF} $*"; }
err()  { echo -e "\033[0;31m[err]\033[0m $*" >&2; }

[[ -d "$PROJECT_DIR/v1-backend" ]] || { echo "[err] 未找到 v1-backend，脚本需放在 <项目>/deploy-kit 下"; exit 1; }

log "项目目录: $PROJECT_DIR"

# ---------- 安全检查 ----------
if [[ -f "$PROJECT_DIR/v1-backend/data/app.db" ]]; then
  CNT=$(sqlite3 "$PROJECT_DIR/v1-backend/data/app.db" "SELECT COUNT(*) FROM iot_users;" 2>/dev/null || echo "?")
  warn "检测到本机用户库 app.db（用户数: $CNT）—— 已排除，不会打进交付包"
fi

# ---------- 打包 ----------
log "打包中（排除 .git/venv/缓存/app.db/日志）..."
if [[ "$WITH_HADOOP" == "true" ]]; then
  warn "--with-hadoop：将包含 2.7G 的 hadoop-stack.tar，包会非常大"
  HADOOP_EXCLUDE=()
else
  HADOOP_EXCLUDE=(--exclude="${PROJECT_NAME}/docker/hadoop/images/*.tar")
fi

# NiFi 镜像：默认打包在一起（一个文件交付），可用 --no-nifi-image 排除
if [[ "$WITH_IMAGE" == "true" ]]; then
  IMAGE_TARS=("$KIT_DIR"/offline-assets/*.tar)
  if [[ -f "${IMAGE_TARS[0]}" ]]; then
    log "包含 NiFi 镜像（约 1.6G）—— 交付只需一个文件"
    IMAGE_EXCLUDE=()
  else
    warn "未找到 NiFi 镜像（需先跑 prepare_offline_assets.sh），本次不含镜像"
    IMAGE_EXCLUDE=()
  fi
else
  warn "--no-nifi-image：镜像不打包，需单独拷贝"
  IMAGE_EXCLUDE=(--exclude="${PROJECT_NAME}/deploy-kit/offline-assets/*.tar")
fi

# 系统软件离线 deb（mysql/docker/nginx/python3）：默认打进包，目标机可完全断网部署
if [[ "$WITH_SYSDEPS" == "true" ]] && [[ -d "$KIT_DIR/offline-assets/deb/sysdeps" ]]; then
  SYSDEPS_EXCLUDE=()
  SYSDEPS_DESC="$(du -sh "$KIT_DIR/offline-assets/deb/sysdeps" | cut -f1)"
  # 注意：用 glob 展开而不是 ls —— 某些受限执行环境里 `ls > 文件` 会返回 255，
  # 配合 set -e 会让脚本静默退出（cmd | tr 这种管道方式才正常）。
  SYSDEPS_LIST="$(printf '%s ' "$KIT_DIR"/offline-assets/deb/sysdeps/*/)"
  log "包含系统软件离线 deb（$SYSDEPS_DESC）：$SYSDEPS_LIST"
else
  warn "--no-sysdeps 或未生成：系统软件需目标机已装 / 能连 apt 源（http 被挡时会自动换 https）"
  SYSDEPS_EXCLUDE=(--exclude="${PROJECT_NAME}/deploy-kit/offline-assets/deb/sysdeps")
fi

# ---------- 白名单：只打包【运行必需】的顶层目录 ----------
# 为什么用白名单：
#   之前用「打整个项目 + 排除几项」的黑名单方式，结果把下列东西也打进了交付包：
#     .codebuddy/    ← AI 对话记忆，含内部 IP、SSH 信息等敏感内容（严重泄露！）
#     .github/ .pytest_cache/ 每日工作进度/ docs-archive/ V3执行清单/ V4执行清单/
#     test-scripts/ contract-tests/ test-data/ meta_backups/ nifi_data/ 等开发资料
#     以及大量 .md/.docx/.pdf 开发文档
#   白名单能从根本上杜绝「漏排除」，确保只交付运行所需内容。
INCLUDE_ITEMS=(
  "v1-backend"       # 后端服务
  "v1-frontend"      # 前端页面
  "docker"           # NiFi 镜像构建上下文
  "real_nifi_conf"   # NiFi flow 与配置（容器挂载用）
  "deploy-kit"       # 部署工具包
)

log "白名单打包，只含: ${INCLUDE_ITEMS[*]}"
for item in "${INCLUDE_ITEMS[@]}"; do
  [[ -e "$PROJECT_DIR/$item" ]] || { err "缺少必需目录: $item，无法打包"; exit 1; }
done

# 保持解压后为 iot/xxx 结构：给每项加 PROJECT_NAME/ 前缀
PREFIXED=()
for item in "${INCLUDE_ITEMS[@]}"; do
  PREFIXED+=("${PROJECT_NAME}/${item}")
done

tar czf "$PKG_PATH" \
  -C "$(dirname "$PROJECT_DIR")" \
  --exclude="${PROJECT_NAME}/v1-backend/.venv" \
  --exclude="${PROJECT_NAME}/v1-backend/venv" \
  --exclude="${PROJECT_NAME}/v1-backend/data/app.db" \
  --exclude="${PROJECT_NAME}/v1-backend/data/generated" \
  --exclude="${PROJECT_NAME}/real_nifi_conf/archive" \
  --exclude="${PROJECT_NAME}/real_nifi_conf/*.bak*" \
  --exclude="${PROJECT_NAME}/real_nifi_conf/*.bad.*" \
  --exclude="${PROJECT_NAME}/real_nifi_conf/*.fix*" \
  --exclude="__pycache__" \
  --exclude="*.pyc" \
  --exclude="*.log" \
  ${IMAGE_EXCLUDE[@]+"${IMAGE_EXCLUDE[@]}"} \
  ${HADOOP_EXCLUDE[@]+"${HADOOP_EXCLUDE[@]}"} \
  ${SYSDEPS_EXCLUDE[@]+"${SYSDEPS_EXCLUDE[@]}"} \
  "${PREFIXED[@]}"

# ---------- 校验值 ----------
(cd "$OUT_DIR" && sha256sum "$PKG_NAME" > "${PKG_NAME}.sha256")

SIZE=$(du -h "$PKG_PATH" | cut -f1)
log "完成: $PKG_PATH ($SIZE)"
log "校验: ${PKG_PATH}.sha256"

# ---------- 交付清单 ----------
cat <<EOF

===== 交付 =====
只需给工厂这一个文件（已含：代码 + 部署工具 + Python依赖wheel$([[ "$WITH_IMAGE" == "true" ]] && echo " + NiFi镜像") + 系统软件离线deb）：
    $(basename "$PKG_PATH")

需要一起交给现场的技术文档（唯一一份，顶部就是极简部署步骤）：
    deploy-kit/部署文档.txt

===== 工厂机器上（两步）=====
  # 第1步：解压（必须落到 /home/yhz）
  sudo tar xzf ${PKG_NAME} -C /home/yhz

  # 第2步：向导部署（自动检测IP、建账号、装依赖、起服务、加载镜像）
  cd /home/yhz/iot/deploy-kit && sudo ./setup.sh

  # 验收（可选，建议做）
  sudo ./verify.sh && sudo ./smoke_test.sh

===== 想一步到位，复制这一行 =====
  sudo tar xzf ${PKG_NAME} -C /home/yhz && cd /home/yhz/iot/deploy-kit && sudo ./setup.sh

===== 系统软件（mysql/docker/nginx/python3）怎么来 =====
  安装程序自动检测，按下面顺序处理，通常无需人工干预：
    ① 已装好          → 跳过
    ② 包内备有本发行版 → dpkg 离线安装（零下载，需 prepare_sysdeps.sh 先生成）
    ③ 都没有          → 自动把 apt 源 http:// 换成 https:// 再在线装
  先跑 ./check_env.sh 可查看缺什么。
EOF
