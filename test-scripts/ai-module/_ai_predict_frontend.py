#!/usr/bin/env python3
"""通过前端在线推理测试：选择预测文件夹+文件，提交预测，抓取结果"""
import subprocess, time, os, re, sys, json, glob

sys.path.insert(0, "/home/yhz/iot")
import _ai_frontend_lib as L

def goto_predict_tab():
    """切到在线推理测试 tab"""
    s = L.snap()
    m = re.search(r'tab "在线推理测试" \[ref=([^\]]+)\]', s)
    if m:
        L.click(m.group(1), 2)
        return True
    # 尝试 DOM
    L.eval_js("async ()=>{const tabs=document.querySelectorAll('[role=tab]'); for(const t of tabs){ if(t.textContent.includes('在线推理测试')){ t.click(); return 'ok'; } } return 'no';}")
    time.sleep(2)
    return True

def _open_select_generic():
    """点击页面中任意 generic [cursor=pointer] 下拉容器（训练/预测文件夹）"""
    s = L.snap()
    lines = s.split('\n')
    for i, line in enumerate(lines):
        if 'combobox' in line:
            for j in range(i - 1, max(0, i - 4), -1):
                mm = re.search(r'generic \[ref=([^\]]+)\] \[cursor=pointer\]', lines[j])
                if mm:
                    L.click(mm.group(1), 1.5)
                    return True
            break
    return False

def select_predict_folder():
    """选择预测文件夹 admin/tagged_nifi_data → pima_predict"""
    if not _open_select_generic():
        return False
    s = L.snap()
    opts = re.findall(r'option "([^"]*)" \[ref=([^\]]+)\]', s)
    for text, rid in opts:
        if text == 'admin/tagged_nifi_data':
            L.click(rid, 1.5)
            return True
    return False

def click_predict_subdir():
    """进入 pima_predict 子目录"""
    s = L.snap()
    m = re.search(r'button "📁 pima_predict" \[ref=([^\]]+)\]', s)
    if m:
        L.click(m.group(1), 1.5)
        return True
    return False

def _open_file_select():
    """打开文件选择下拉（页面中第 2 个含 combobox 的 select）"""
    s = L.snap()
    lines = s.split('\n')
    combos = []
    for i, line in enumerate(lines):
        if 'combobox' in line:
            for j in range(i - 1, max(0, i - 4), -1):
                mm = re.search(r'generic \[ref=([^\]]+)\] \[cursor=pointer\]', lines[j])
                if mm and mm.group(1) not in combos:
                    combos.append(mm.group(1))
                    break
    if len(combos) >= 2:
        L.click(combos[1], 1.5)
        return True
    return False

def select_predict_file():
    """选择 pima_test.csv"""
    if not _open_file_select():
        return False
    s = L.snap()
    opts = re.findall(r'option "([^"]*)" \[ref=([^\]]+)\]', s)
    for text, rid in opts:
        if 'pima_test.csv' in text:
            L.click(rid, 1)
            return True
    return False

def submit_predict():
    """点击提交预测"""
    r = L.eval_js("async ()=>{const btns=[...document.querySelectorAll('button')].filter(x=>x.textContent.includes('提交至已部署')); if(btns.length && !btns[0].disabled){btns[0].click(); return 'clicked';} return 'disabled';}")
    return '"clicked"' in str(r)

def get_predict_result(timeout=180):
    """等待预测结果，返回 (fault_count, total, raw_text)"""
    end = time.time() + timeout
    last = ""
    while time.time() < end:
        s = L.snap()
        last = s
        # 多模型结果：诊断结果报告
        m = re.search(r'诊断结果报告[^\n]*', s)
        # 故障数
        m2 = re.search(r'(\d+)\s*/\s*(\d+)\s*条故障', s)
        if m2:
            return int(m2.group(1)), int(m2.group(2)), s
        if "全部正常" in s:
            m3 = re.search(r'全部正常 \((\d+) 条\)', s)
            return 0, int(m3.group(1)) if m3 else 0, s
        if "预测失败" in s or "未找到数据集" in s:
            m4 = re.search(r'(未找到数据集[^\n]*)', s)
            return None, None, m4.group(1) if m4 else s
        time.sleep(3)
    return None, None, last

def main():
    print("=== 前端在线推理预测 ===", flush=True)
    if not goto_predict_tab():
        print("FAIL: 无法进入在线推理测试", flush=True); sys.exit(1)
    if not select_predict_folder():
        print("FAIL: 无法选择预测文件夹", flush=True); sys.exit(1)
    time.sleep(1)
    if not click_predict_subdir():
        print("FAIL: 无法进入 pima_predict", flush=True); sys.exit(1)
    time.sleep(1)
    if not select_predict_file():
        print("FAIL: 无法选择预测文件", flush=True); sys.exit(1)
    time.sleep(0.5)
    if not submit_predict():
        print("FAIL: 提交按钮不可用", flush=True); sys.exit(1)
    print("已提交预测", flush=True)
    fault, total, raw = get_predict_result(180)
    print(f"预测结果: fault={fault} total={total}", flush=True)
    with open("/tmp/ai_predict_result.json", "w") as f:
        json.dump({"fault": fault, "total": total, "raw": raw[:2000]}, f, ensure_ascii=False)

if __name__ == "__main__":
    main()