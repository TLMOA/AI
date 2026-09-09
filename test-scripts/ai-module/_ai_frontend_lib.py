#!/usr/bin/env python3
"""AI 训练网站前端自动化库：通过 playwright-cli 操作前端 3002 页面"""
import subprocess, time, os, re, sys, json

PCLI = "/home/yhz/.nvm/versions/node/v22.22.2/bin/playwright-cli"
SNAP_DIR = "/home/yhz/iot/.playwright-cli"
FE = "http://127.0.0.1:3002"

def run(args, timeout=30):
    r = subprocess.run([PCLI] + args, capture_output=True, text=True, timeout=timeout)
    return r.stdout, r.stderr, r.returncode

def snap(filename=None):
    """snapshot 默认保存到当前工作目录（playwright-cli 行为），优先找最近生成的 yml"""
    if filename:
        run(["snapshot", "--filename=" + filename]); time.sleep(0.6)
        p = filename if os.path.exists(filename) else os.path.join(SNAP_DIR, filename)
    else:
        run(["snapshot"]); time.sleep(0.6)
        # 搜索当前目录和 .playwright-cli 目录下最近的 yml
        candidates = []
        for base in [os.getcwd(), SNAP_DIR]:
            if not os.path.isdir(base):
                continue
            for f in os.listdir(base):
                if (f.startswith("page-") or f.startswith("snap")) and f.endswith(".yml"):
                    candidates.append(os.path.join(base, f))
        if not candidates:
            return ""
        p = max(candidates, key=os.path.getmtime)
    try:
        with open(p, encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""

def ref(text):
    """按可见文本找 ref"""
    s = snap()
    for line in s.split("\n"):
        if text in line and "[ref=" in line:
            m = re.search(r'\[ref=([^\]]+)\]', line)
            if m:
                return m.group(1)
    return ""

def refs(text):
    """按可见文本找所有 ref"""
    s = snap()
    out = []
    for line in s.split("\n"):
        if text in line and "[ref=" in line:
            m = re.search(r'\[ref=([^\]]+)\]', line)
            if m:
                out.append(m.group(1))
    return out

def click(rid, wait=1.2):
    if rid:
        run(["click", rid]); time.sleep(wait); return True
    return False

def click_text(text, wait=1.2):
    return click(ref(text), wait)

def fill(rid, val):
    if rid:
        run(["fill", rid, str(val)]); time.sleep(0.3); return True
    return False

def fill_id(dom_id, val):
    run(["eval", f"async ()=>{{const e=document.getElementById('{dom_id}'); if(e){{e.value='{val}'; e.dispatchEvent(new Event('input')); e.dispatchEvent(new Event('change')); return 'ok';}} return 'no';}}"])
    time.sleep(0.3)

def check(rid):
    """勾选 checkbox"""
    if rid:
        run(["check", rid]); time.sleep(0.4); return True
    return False

def uncheck(rid):
    if rid:
        run(["uncheck", rid]); time.sleep(0.4); return True
    return False

def select_option(rid, value):
    """el-select 下拉选择（通过点击下拉再点选项）"""
    if rid:
        run(["click", rid]); time.sleep(1.0)
        s = snap()
        # 在 popper 中找选项文本
        m = re.search(rf'([^\n]*{re.escape(value)}[^\n]*\[ref=([^\]]+)\])\n', s)
        if m:
            run(["click", m.group(2)]); time.sleep(0.8); return True
        # 尝试直接 eval 点击
        run(["eval", f"async ()=>{{const items=document.querySelectorAll('.el-select-dropdown__item'); for(const it of items){{if(it.textContent.includes('{value}')){{it.click(); return 'clicked';}}}} return 'notfound';}}"])
        time.sleep(0.8); return True
    return False

def eval_js(js):
    r = run(["eval", js]); return r[0]

def page_text():
    s = snap()
    # 提取文本行
    lines = []
    for line in s.split("\n"):
        line = line.strip()
        if line and not line.startswith("-"):
            continue
        # 去掉 ref 标记
        txt = re.sub(r'\[ref=[^\]]+\]', '', line)
        txt = txt.replace("- ", "").strip()
        if txt:
            lines.append(txt)
    return "\n".join(lines)

def el_select_by_text(value, wait=1.0):
    """操作 Element Plus 下拉：点击当前 combobox，再点选项"""
    rid = ref(value) or ref("请选择")
    run(["click", rid]); time.sleep(wait)
    # 选项在 body 的 popper 中
    s = snap()
    # 查找可见的选项
    for line in s.split("\n"):
        if value in line and ("option" in line or "listitem" in line or "generic" in line) and "[ref=" in line:
            m = re.search(r'\[ref=([^\]]+)\]', line)
            if m:
                run(["click", m.group(1)]); time.sleep(0.8)
                return True
    return False

def wait_text(text, timeout=60, interval=2):
    """轮询等待页面出现某文本"""
    end = time.time() + timeout
    while time.time() < end:
        if text in snap():
            return True
        time.sleep(interval)
    return False

def wait_ref(text, timeout=30, interval=1.5):
    end = time.time() + timeout
    while time.time() < end:
        r = ref(text)
        if r:
            return r
        time.sleep(interval)
    return ""