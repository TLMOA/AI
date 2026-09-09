#!/usr/bin/env python3
"""AI 训练网站全流程测试：对每个模型 训练→部署→预测，记录结果"""
import subprocess, time, os, re, sys, json, glob

sys.path.insert(0, "/home/yhz/iot")
import _ai_frontend_lib as L

RESULTS = []
MODELS = ["xgboost", "LightGBM", "CatBoost", "hgb (梯度提升树)",
          "TabNet (表格注意力)", "TabTransformer (表格架构)", "Deep-MLP (深度全连接)",
          "1D-CNN (卷积神经网络)", "Bi-LSTM (双向长短期记忆)", "Bi-GRU (双向门控循环)"]

def eval_clean(js):
    return str(L.eval_js(js))

def goto_home():
    L.run(["goto", "http://127.0.0.1:3002"]); time.sleep(4)
    L.eval_js("async ()=>{const e=document.querySelector('input[placeholder=输入用户名]'); if(e){e.value='admin'; e.dispatchEvent(new Event('input')); e.dispatchEvent(new Event('change')); return 'ok';} return 'no';}")
    time.sleep(2)
    # 切到模型训练与部署 tab
    s = L.snap()
    m = re.search(r'tab "模型训练与部署" \[ref=([^\]]+)\]', s)
    if m:
        L.click(m.group(1), 2)
    else:
        L.eval_js("async ()=>{const tabs=document.querySelectorAll('[role=tab]'); for(const t of tabs){ if(t.textContent.includes('模型训练与部署')){ t.click(); return 'ok'; } } return 'no';}")
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

def open_select_generic():
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

def pick_option(prefix):
    s = L.snap()
    opts = re.findall(r'option "([^"]*)" \[ref=([^\]]+)\]', s)
    for text, rid in opts:
        if text == prefix or text.startswith(prefix):
            L.click(rid, 1.5)
            return True
    return False

def click_folder_btn(name):
    s = L.snap()
    m = re.search(rf'button "📁 {re.escape(name)}" \[ref=([^\]]+)\]', s)
    if m:
        L.click(m.group(1), 1.5)
        return True
    return False

def select_train_data():
    """选择 admin/nifi-data → pima_diabetes → 勾选 pima_train.csv"""
    s = L.snap()
    if 'pima_train.csv' in s:
        rid = _checkbox_ref('pima_train.csv')
        if rid:
            L.click(rid, 0.8)
        return True
    if 'pima_diabetes' in s:
        if click_folder_btn('pima_diabetes'):
            time.sleep(1)
            rid = _checkbox_ref('pima_train.csv')
            if rid:
                L.click(rid, 0.8)
            return True
    if open_select_generic():
        if pick_option('admin/nifi-data'):
            time.sleep(1.5)
            if click_folder_btn('pima_diabetes'):
                time.sleep(1)
                rid = _checkbox_ref('pima_train.csv')
                if rid:
                    L.click(rid, 0.8)
                return True
    return False

def select_model_only(model):
    models = ["xgboost", "LightGBM", "CatBoost", "hgb (梯度提升树)",
              "TabNet (表格注意力)", "TabTransformer (表格架构)", "Deep-MLP (深度全连接)",
              "1D-CNN (卷积神经网络)", "Bi-LSTM (双向长短期记忆)", "Bi-GRU (双向门控循环)"]
    for m in models:
        rid = _checkbox_ref(m)
        if not rid:
            continue
        checked = eval_clean(f"async ()=>{{const labels=document.querySelectorAll('.el-checkbox'); for(const l of labels){{ if(l.textContent.trim()==='{m}'){{ return l.className.includes('is-checked')?'1':'0'; }} }} return '0';}}")
        is_checked = '"1"' in checked
        want = (m == model)
        if want and not is_checked:
            L.click(rid, 0.5)
        elif not want and is_checked:
            L.click(rid, 0.5)
    return True

def click_train():
    r = eval_clean("async ()=>{const btns=[...document.querySelectorAll('button')].filter(x=>x.textContent.includes('一键训练')); if(btns.length && !btns[0].disabled){btns[0].click(); return 'clicked';} return 'disabled';}")
    return '"clicked"' in r

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
            print(f"    [{time.strftime('%H:%M:%S')}] {st} {d.get('stage','')}", flush=True)
        except Exception:
            pass
        time.sleep(6)
    return None

def deploy_current():
    """点击 leaderboard 的一键部署至 IoT"""
    end = time.time() + 90
    while time.time() < end:
        r = eval_clean("async ()=>{const btns=[...document.querySelectorAll('button')].filter(x=>x.textContent.includes('一键部署至 IoT')); if(btns.length && !btns[0].disabled){btns[0].click(); return 'clicked';} return 'no';}")
        if '"clicked"' in r:
            time.sleep(2)
            return True
        time.sleep(3)
    return False

def goto_predict_tab():
    s = L.snap()
    m = re.search(r'tab "在线推理测试" \[ref=([^\]]+)\]', s)
    if m:
        L.click(m.group(1), 2)
    else:
        L.eval_js("async ()=>{const tabs=document.querySelectorAll('[role=tab]'); for(const t of tabs){ if(t.textContent.includes('在线推理测试')){ t.click(); return 'ok'; } } return 'no';}")
        time.sleep(2)

def _open_nth_generic(n):
    """点击第 n 个含 combobox 的 select 容器（1-based）"""
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
    if len(combos) >= n:
        L.click(combos[n - 1], 1.5)
        return True
    return False

def select_predict_folder_file():
    """选择预测文件夹+文件（预测 tab 内：文件夹下拉第 1 个，文件下拉选择文件夹后出现）"""
    # 预测文件夹下拉（第 1 个 select）
    if not _open_nth_generic(1):
        return False
    if not pick_option('admin/tagged_nifi_data'):
        return False
    time.sleep(1.5)
    if not click_folder_btn('pima_predict'):
        return False
    time.sleep(1.5)
    # 文件下拉（选择文件夹后出现的第 2 个 select）
    if not _open_nth_generic(2):
        return False
    s = L.snap()
    opts = re.findall(r'option "([^"]*)" \[ref=([^\]]+)\]', s)
    for text, rid in opts:
        if 'pima_test.csv' in text:
            L.click(rid, 1)
            return True
    return False

def submit_predict():
    r = eval_clean("async ()=>{const btns=[...document.querySelectorAll('button')].filter(x=>x.textContent.includes('提交至已部署')); if(btns.length && !btns[0].disabled){btns[0].click(); return 'clicked';} return 'disabled';}")
    return '"clicked"' in r

def get_predict_result(timeout=120):
    """等待预测结果，从 DOM 读取"""
    end = time.time() + timeout
    while time.time() < end:
        r = eval_clean("async ()=>{const els=document.querySelectorAll('.el-message-box'); for(const e of els){ const t=e.textContent; if(t.includes('故障') && t.includes('诊断')){ return 'BOX:'+t; } } return 'no';}")
        if 'BOX:' in r:
            m = re.search(r'发现 (\d+) 条故障', r)
            m2 = re.search(r'共诊断 (\d+) 条数据', r)
            fault = int(m.group(1)) if m else 0
            total = int(m2.group(1)) if m2 else 0
            # 关闭弹窗
            eval_clean("async ()=>{const btns=document.querySelectorAll('.el-message-box__btns button'); for(const b of btns){ if(b.textContent.includes('我知道了')){ b.click(); return 'ok'; } } return 'no';}")
            return fault, total
        time.sleep(3)
    return None, None

def run_model(model, idx):
    print(f"\n===== [{idx}/10] 模型: {model} =====", flush=True)
    goto_home()
    if not select_train_data():
        print("  FAIL: 选择数据失败", flush=True); return None
    select_model_only(model)
    time.sleep(0.5)
    if not click_train():
        print("  FAIL: 训练按钮不可用", flush=True); return None
    time.sleep(3)
    tid = latest_task()
    print(f"  task_id: {tid}", flush=True)
    data = wait_train(tid, 2400)
    if not data or data.get("status") != "completed":
        print("  FAIL: 训练未完成", flush=True)
        print("  " + json.dumps(data, ensure_ascii=False)[:400] if data else "  no data", flush=True)
        return None
    print("  训练完成:", flush=True)
    for m in data.get("models", []):
        print(f"    {m.get('model_name')}: f1={m.get('f1_score')} acc={m.get('accuracy')} status={m.get('status')}", flush=True)
    # 部署
    if not deploy_current():
        print("  WARN: 部署按钮未找到", flush=True)
    else:
        print("  部署成功", flush=True)
    # 预测
    goto_predict_tab()
    if not select_predict_folder_file():
        print("  FAIL: 预测数据选择失败", flush=True)
    else:
        if submit_predict():
            print("  预测已提交", flush=True)
            fault, total = get_predict_result(180)
            print(f"  预测结果: {fault}/{total} 条故障", flush=True)
        else:
            print("  FAIL: 预测提交失败", flush=True)
    return {"model": model, "train": data, "deployed": True, "predict_fault": fault if 'fault' in locals() else None}

if __name__ == "__main__":
    start = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    end = int(sys.argv[2]) if len(sys.argv) > 2 else len(MODELS)
    for i in range(start, end):
        res = run_model(MODELS[i], i + 1)
        if res:
            RESULTS.append(res)
            with open("/tmp/ai_all_results.json", "w") as f:
                json.dump(RESULTS, f, ensure_ascii=False, indent=2)