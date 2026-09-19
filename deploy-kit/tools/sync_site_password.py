#!/usr/bin/env python3
"""把「本站账号的新密码」同步进 config.env / .deploy-secrets。

【为什么需要它】
  set_password.sh 改的是【数据库里】的密码；而 verify.sh / smoke_test.sh 是拿
  config.env 的 SITE_PASS（缺省回退 .deploy-secrets 的 ADMIN_PASS）去登录做自检的。
  两边不同步 → 改完密码后自检立刻报「登录失败」，运维会误以为部署坏了。
  （2026-09-17 目标机实测踩到：改密码 → verify.sh `[FAIL] 登录失败`）

  所以改密码必须同时更新这两个文件里对应的键。

用法:
  sync_site_password.py <kit_dir> <新密码> [--site-user <账号>]
    <kit_dir>     含 config.env 与 .deploy-secrets 的目录（通常 deploy-kit/）
    --site-user   站点账号名；若与 config.env 里的 SITE_USER 不一致则跳过
                  （说明改的是别的账号，不该动站点密码）

说明：不回显密码；保留原文件权限；键不存在时追加。
"""
import os
import sys
from pathlib import Path


def _upsert(path: Path, key: str, value: str) -> str:
    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines()
        mode = path.stat().st_mode & 0o777
    else:
        lines, mode = [], 0o600

    out, hit = [], False
    for ln in lines:
        if ln.startswith(key + "="):
            out.append("%s=%s" % (key, value))
            hit = True
        else:
            out.append(ln)
    if not hit:
        out.append("%s=%s" % (key, value))

    path.write_text("\n".join(out) + "\n", encoding="utf-8")
    os.chmod(path, mode)
    return "更新" if hit else "新增"


def main() -> int:
    argv = sys.argv[1:]
    site_user = ""
    override_conf = None
    pos = []
    i = 0
    while i < len(argv):
        if argv[i] == "--site-user" and i + 1 < len(argv):
            site_user = argv[i + 1]; i += 2
        elif argv[i] == "--override-conf" and i + 1 < len(argv):
            override_conf = Path(argv[i + 1]); i += 2
        elif argv[i].startswith("--"):
            i += 1
        else:
            pos.append(argv[i]); i += 1

    if len(pos) < 2:
        print("usage: sync_site_password.py <kit_dir> <新密码> "
              "[--site-user <账号>] [--override-conf <override.conf 路径>]", file=sys.stderr)
        return 2

    kit = Path(pos[0])
    new_pass = pos[1]
    config = kit / "config.env"
    secrets = kit / ".deploy-secrets"

    # 改的不是站点账号 → 不动站点密码
    if site_user:
        cur = ""
        if config.exists():
            for ln in config.read_text(encoding="utf-8").splitlines():
                if ln.startswith("SITE_USER="):
                    cur = ln.split("=", 1)[1].strip()
                    break
        if cur and cur != site_user:
            print("[skip] 改的是账号「%s」，与站点账号「%s」不同 → 不动站点密码" % (site_user, cur))
            return 0

    if not config.exists():
        print("[err] 找不到 %s" % config, file=sys.stderr)
        return 1

    a = _upsert(config, "SITE_PASS", new_pass)
    b = _upsert(secrets, "ADMIN_PASS", new_pass)

    # 已渲染的 systemd override.conf 里也有 Environment=IOT_ADMIN_PASSWORD=<旧口令>，
    # 一起刷新，免得旧口令以明文形式长期留在 /etc 下（该值只在「未打禁止默认 admin 补丁」
    # 时才会被用到，所以改了不重启也不影响运行；重启后自然生效）。
    if override_conf:
        # 相对路径按调用者的当前目录解释（不强行拼到 kit 下，避免与直觉不符）
        conf = override_conf
        c = "（未找到，跳过）"
        if conf.exists():
            txt = conf.read_text(encoding="utf-8")
            if "IOT_ADMIN_PASSWORD=" in txt:
                out = []
                for ln in txt.splitlines():
                    if ln.startswith("Environment=IOT_ADMIN_PASSWORD="):
                        out.append("Environment=IOT_ADMIN_PASSWORD=%s" % new_pass)
                    else:
                        out.append(ln)
                conf.write_text("\n".join(out) + "\n", encoding="utf-8")
                c = "已刷新（重启后端后生效）"
            else:
                c = "（文件里没有该项，跳过）"
        print("[ok] systemd override.conf：%s" % c)
    print("[ok] 已同步密码到 config.env(SITE_PASS %s) 与 .deploy-secrets(ADMIN_PASS %s)" % (a, b))
    print("     这样 verify.sh / smoke_test.sh 的自检登录才能用同一个密码（口令本身不显示）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
