#!/usr/bin/env python3
"""关闭前端自助注册入口（方案一）。

做两件事：
  1) register.html        -> 重命名 register.html.disabled（静态服务器不再暴露）
  2) login.html 的注册按钮 -> 【整行】包进 HTML 注释（保留原代码，便于还原）

为什么必须做：只关后端不够（用户仍能看到入口、点开报错）；
             只删前端更不够（别人可直接 curl 注册接口）。
             两者要配合 tools/patch_disable_self_register.py 一起用。

【2026-09-16 修复】旧版实现只把 `<button class="btn" id="registerBtn"` 这半截
替换成了 `<!-- ... -->`，注释在行中间就闭合了，行尾的
`style="background:#28a745;margin-top:8px">注册</button>` 全部露在注释外面 ——
浏览器把它当正文渲染，登录页出现乱码（目标机截图实锤）。
现在改成整行包进注释，并能【自动识别并修复】旧版留下的残缺行
（老机器上不用卸载重装，重跑本脚本即可修好）。

幂等，支持 --revert。

用法: disable_register_page.py <code_dir> [--revert]
"""
import re
import shutil
import sys
from pathlib import Path

MARK = "deploy-kit:register-disabled"

# 旧版补丁的残缺形态：注释在 id="registerBtn" 后就闭合，后面的属性与文字露在外面
BROKEN_RE = re.compile(
    r"<!-- " + re.escape(MARK) + r" <button class=\"btn\" id=\"registerBtn\" -->"
    r"\s*(.*?)\s*<!-- /" + re.escape(MARK) + r" -->",
    re.S,
)


def _apply_login(txt):
    """返回 (新文本, 动作)。动作: repair（修复旧版残缺行）/ wrap（整行注释）/ skip / none"""
    if MARK in txt:
        m = BROKEN_RE.search(txt)
        if not m:
            return txt, "skip"
        # 把残缺行还原成完整按钮，再按新格式整行包进注释
        orig = '<button class="btn" id="registerBtn" ' + m.group(1).strip()
        fixed = "<!-- %s %s -->" % (MARK, orig)
        return txt[: m.start()] + fixed + txt[m.end():], "repair"

    lines = txt.splitlines(keepends=True)
    for i, ln in enumerate(lines):
        if 'id="registerBtn"' in ln:
            stripped = ln.strip()
            # 保留原始行首缩进：这样 --revert 还原后能与原文件逐字节一致
            lead = ln[: len(ln) - len(ln.lstrip())]
            eol = "\n" if ln.endswith("\n") else ""
            lines[i] = lead + "<!-- %s %s -->%s" % (MARK, stripped, eol)
            return "".join(lines), "wrap"
    return txt, "none"


def _revert_login(txt):
    """先把可能存在的旧版残缺行修成规整整行注释，再整体解除注释"""
    txt, _ = _apply_login(txt)
    pat = re.compile(r"<!-- " + re.escape(MARK) + r" (.*?) -->", re.S)
    new_txt, n = pat.subn(lambda mo: mo.group(1), txt)
    return new_txt, n


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    revert = "--revert" in sys.argv
    if not args:
        print("usage: disable_register_page.py <code_dir> [--revert]", file=sys.stderr)
        sys.exit(2)

    fe = Path(args[0]) / "v1-frontend"
    if not fe.is_dir():
        print(f"[err] 找不到 {fe}", file=sys.stderr)
        sys.exit(1)

    reg_html = fe / "register.html"
    reg_disabled = fe / "register.html.disabled"
    login_html = fe / "login.html"

    if revert:
        if reg_disabled.exists() and not reg_html.exists():
            shutil.move(str(reg_disabled), str(reg_html))
            print("[ok] 已恢复 register.html")
        if login_html.exists():
            txt = login_html.read_text(encoding="utf-8")
            new_txt, n = _revert_login(txt)
            if n:
                login_html.write_text(new_txt, encoding="utf-8")
                print("[ok] 已恢复 login.html 注册按钮")
        return

    # 1) 下线注册页
    if reg_html.exists():
        if reg_disabled.exists():
            reg_disabled.unlink()
        shutil.move(str(reg_html), str(reg_disabled))
        print(f"[ok] register.html -> {reg_disabled.name}")
    else:
        print("[skip] register.html 不存在（已处理过）")

    # 2) 注释掉登录页的注册按钮（整行；并修复旧版残缺行）
    if login_html.exists():
        txt = login_html.read_text(encoding="utf-8")
        new_txt, act = _apply_login(txt)
        if act == "repair":
            login_html.write_text(new_txt, encoding="utf-8")
            print("[fix] login.html 旧版补丁留下的半截注释已修复（此前登录页会显示乱码）")
        elif act == "wrap":
            login_html.write_text(new_txt, encoding="utf-8")
            print("[ok] login.html 注册按钮已注释")
        elif act == "skip":
            print("[skip] login.html 注册按钮已处理")
        else:
            print("[warn] login.html 未找到 registerBtn，跳过")
    else:
        print("[warn] 未找到 login.html")


if __name__ == "__main__":
    main()
