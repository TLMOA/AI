#!/usr/bin/env bash
# ============================================================
# 公共函数库（被 deploy.sh / verify.sh / set_password.sh / add_factory.sh source）
#
# 【为什么单独放一份】
#   「切换到运行用户执行」这段逻辑以前在 4 个脚本里各抄了一遍，结果漂移出两个真 bug：
#     · deploy.sh 那份在 `elif 有 sudo` 分支里误写成递归调用自己 → 没有 runuser 的机器上死递归；
#     · verify.sh / set_password.sh / add_factory.sh 直接写死 `sudo -u` → 没装 sudo 的机器直接失败。
#   统一成一份定义，从根上避免这类复制粘贴漂移。
#
# 【为什么不用 sudo】
#   有些机器根本没装 sudo（最小安装、容器、非 Ubuntu 系统），而 util-linux 自带的
#   runuser 到处都有 ⇒ 三级回退：runuser → sudo → su
# ============================================================

# 以运行用户身份执行命令。依赖调用方已设置 APP_USER。
as_user() {
  local u="${APP_USER:-}"
  [[ -n "$u" ]] || { echo "[err] as_user: 未设置 APP_USER" >&2; return 1; }
  # runuser 只有 root 能用（非 root 会直接报「非 root 用户不能使用」）→ 先判 EUID，
  # 免得非 root 场景下既报错、又没走到 sudo/su 回退
  if [[ $EUID -eq 0 ]] && command -v runuser >/dev/null 2>&1; then
    runuser -u "$u" -- "$@"
  elif command -v sudo >/dev/null 2>&1; then
    sudo -u "$u" -- "$@"
  elif command -v su >/dev/null 2>&1; then
    local _cmd="" _a
    for _a in "$@"; do _cmd="$_cmd$(printf '%q ' "$_a")"; done
    su -s /bin/bash "$u" -c "$_cmd"
  else
    echo "[err] 本机没有 runuser/sudo/su，无法切换到 $u 执行：$*" >&2
    return 1
  fi
}
