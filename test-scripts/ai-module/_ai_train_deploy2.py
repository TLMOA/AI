#!/usr/bin/env python3
"""通过前端训练单个模型并部署（一体化，训练完成后立即部署）"""
import subprocess, time, os, re, sys, json, glob

sys.path.insert(0, "/home/yhz/iot")
import _ai_frontend_lib as L

def eval_clean(js):
    r = L.eval_js(js)
    return str(r)

def goto_home():
    L.run(["goto", "http://127.0.0.1:3002"]); time.sleep(4)
    L.eval_js("async ()=>{const e=document.querySelector('input[placeholder=输入用户名]'); if(e){e.value='admin'; e.dispatchEvent(new Event('input')); e.dispatchEvent(new Event('change')); return 'ok';} return 'no';}")
    time.sleep(2)

def _checkbox_ref(text):
    s = L.snap()
    lines = s.split('\n')
    for i, line in enumerate(lines):
        if text in line and 'checkbox' in line:
            for j in range(i - 1, max(0, i - 3), -1):
                mm = re.search(r'generic \[ref=([^\]]+)\]', lines[j])
                if mm:
                    return mm.group(1)
            break
    return ""

def select_folder_and_file():
    """选择数据集并勾选文件。返回 True 表示已就绪"""
    s = L.snap()
    # 已在 pima_diabetes 且文件可见
    if 'pima_train.csv' in s:
        # 确保文件勾选
        rid = _checkbox_ref('pima_train.csv')
        if rid:
            L.click(rid, 0.8)
        return True
    # 在 admin/nifi-data 根目录（有 pima_diabetes 子目录）
    if 'pima_diabetes' in s:
        m = re.search(r'button "📁 pima_diabetes" \[ref=([^\]]+)\]', s)
        if m:
            L.click(m.group(1), 1.5)
            s = L.snap()
            rid = _checkbox_ref('pima_train.csv')
            if rid:
                L.click(rid, 0.8)
            return True
    # 需要重新选文件夹
    # 打开下拉
    lines = s.split('\n')
    sel_ref = ""
    for i, line in enumerate(lines):
        if 'combobox' in line:
            for j in range(i - 1, max(0, i - 4), -1):
                mm = re.search(r'generic \[ref=([^\]]+)\] \[cursor=pointer\]', lines[j])
                if mm:
                    sel_ref = mm.group(1)
                    break
            break
    if not sel_ref:
        return False
    L.click(sel_ref, 1.5)
    s = L.snap()
    opts = re.findall(r'option "([^"]*)" \[ref=([^\]]+)\]', s)
    for text, rid in opts:
        if text == 'admin/nifi-data':
            L.click(rid, 1.5)
            break
    time.sleep(1.5)
    s = L.snap()
    m = re.search(r'button "📁 pima_diabetes" \[ref=([^\]]+)\]', s)
    if m:
        L.click(m.group(1), 1.5)
    time.sleep(1.5)
    s = L.snap()
    rid = _checkbox_ref('pima_train.csv')
    if rid:
        L.click(rid, 0.8)
    return True

def select_model(model):
    """只勾选指定模型"""
    models = ["xgboost", "LightGBM", "CatBoost", "hgb (梯度提升树)",
              "TabNet (表格注意力)", "TabTransformer (表格架构)", "Deep-MLP (深度全连接)",
              "1D-CNN (卷积神经网络)", "Bi-LSTM (双向长短期记忆)", "Bi-GRU (双向门控循环)"]
    for m in models:
        rid = _checkbox_ref(m)
        if not rid:
            continue
        checked = L.eval_js(f"async ()=>{{const labels=document.querySelectorAll('.el-checkbox'); for(const l of labels){{ if(l.textContent.trim()==='{m}'){{ return l.className.includes('is-checked')?'1':'0'; }} }} return '0';}}")
        is_checked = '"1"' in str(checked)
        want = (m == model)
        if want and not is_checked:
            L.click(rid, 0.5)
        elif not want and is_checked:
            L.click(rid, 0.5)
    return True

def click_train():
    r = L.eval_js("async ()=>{const btns=[...document.querySelectorAll('button')].filter(x=>x.textContent.includes('一键训练')); if(btns.length && !btns[0].disabled){btns[0].click(); return 'clicked';} return 'disabled';}")
    return '"clicked"' in str(r)

def latest_task():
    fs = sorted(glob.glob("/home/yhz/solo_ai_iot/banckend/storage/tasks/admin/*.json"),
                key=lambda f: os.path.getmtime(f), reverse=True)
    return os.path.splitext(os.path.basename(fs[0]))[0] if fs else ""

def wait_train(task_id, timeout=2400):
    import requests
    end = time.time() + timeout
    while time.time() < end:
        try:
            r = requests.get(f"http://127.0.0.1:8002/api/train/status/{task_id}?user_id=admin", timeout=10).json()
            d = r.get("data", {})
            st = d.get("status")
            if st in ("completed", "failed"):
                return d
            print(f"[{time.strftime('%H:%M:%S')}] {st} {d.get('stage','')}", flush=True)
        except Exception:
            pass
        time.sleep(6)
    return None

def wait_and_click_deploy(timeout=120):
    """等待 leaderboard 出现并点击部署按钮"""
    end = time.time() + timeout
    while time.time() < end:
        s = L.snap()
        # 部署按钮可能 disabled 或可用
        for line in s.split('\n'):
            if '一键部署至 IoT' in line and '[ref=' in line:
                m = re.search(r'\[ref=([^\]]+)\]', line)
                if m:
                    if 'disabled' in line:
                        time.sleep(2)
                        break
                    L.click(m.group(1), 3)
                    return True
        time.sleep(3)
    return False

def main():
    model = sys.argv[1]
    timeout = int(sys.argv[2]) if len(sys.argv) > 2 else 2400
    print(f"=== 训练+部署: {model} ===", flush=True)
    goto_home()
    if not select_folder_and_file():
        print("FAIL: 选择数据集失败", flush=True); sys.exit(1)
    select_model(model)
    time.sleep(0.8)
    if not click_train():
        print("FAIL: 训练按钮不可用", flush=True); sys.exit(1)
    time.sleep(3)
    tid = latest_task()
    print("task:", tid, flush=True)
    data = wait_train(tid, timeout)
    if not data or data.get("status") != "completed":
        print("FAIL: 训练未完成", flush=True)
        print(json.dumps(data, ensure_ascii=False)[:500] if data else "no data", flush=True)
        sys.exit(1)
    print("=== 训练完成 ===", flush=True)
    print(json.dumps(data, ensure_ascii=False), flush=True)
    ok = wait_and_click_deploy(90)
    print("deployed:", ok, flush=True)
    res = {"model": model, "train": data, "deployed": ok}
    with open(f"/tmp/ai_train_result_{model}.json", "w") as f:
        json.dump(res, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()