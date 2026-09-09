#!/usr/bin/env python3
"""通过前端训练单个模型并部署。用法: python3 _ai_train_deploy.py <model_name> [timeout]"""
import subprocess, time, os, re, sys, json, glob

sys.path.insert(0, "/home/yhz/iot")
import _ai_frontend_lib as L

PCLI = L.PCLI

def snap_text():
    s = L.snap()
    txt = re.sub(r'\[ref=[^\]]+\]', '', s)
    return txt

def open_folder_select():
    """打开训练数据集文件夹下拉（点击 generic 容器）"""
    s = L.snap()
    m = re.search(r'generic \[ref=([^\]]+)\] \[cursor=pointer\]:\n\s+- generic:\n\s+- combobox', s)
    if m:
        L.click(m.group(1), 1.5)
        return True
    # 备选：找含 combobox 的 generic
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
    """在展开的下拉中点击指定前缀的 option"""
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

def _checkbox_ref_by_text(text):
    """通过 snapshot 找 checkbox label 的 generic ref"""
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

def check_file(fname):
    """勾选训练文件 checkbox（playwright click 触发 Vue）"""
    rid = _checkbox_ref_by_text(fname)
    if rid:
        L.click(rid, 0.8)
        return True
    return False

def select_model_only(model):
    """清空其他模型勾选，只勾选指定模型"""
    models = ["xgboost", "LightGBM", "CatBoost", "hgb (梯度提升树)",
              "TabNet (表格注意力)", "TabTransformer (表格架构)", "Deep-MLP (深度全连接)",
              "1D-CNN (卷积神经网络)", "Bi-LSTM (双向长短期记忆)", "Bi-GRU (双向门控循环)"]
    for m in models:
        rid = _checkbox_ref_by_text(m)
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

def click_train_btn():
    return L.eval_js("async ()=>{const btns=[...document.querySelectorAll('button')].filter(x=>x.textContent.includes('一键训练')); if(btns.length && !btns[0].disabled){btns[0].click(); return 'clicked';} return 'disabled';}")

def wait_train_done(task_id, timeout=2400):
    import requests
    end = time.time() + timeout
    last = ""
    while time.time() < end:
        try:
            r = requests.get(f"http://127.0.0.1:8002/api/train/status/{task_id}?user_id=admin", timeout=10).json()
            d = r.get("data", {})
            last = json.dumps(d, ensure_ascii=False)
            st = d.get("status")
            if st in ("completed", "failed"):
                return d
            stage = d.get("stage", "")
            print(f"[{time.strftime('%H:%M:%S')}] {st} {stage}", flush=True)
        except Exception as e:
            print("poll err", e, flush=True)
        time.sleep(6)
    return None

def click_deploy():
    """等待 leaderboard 部署按钮可点击并点击"""
    end = time.time() + 120
    while time.time() < end:
        s = L.snap()
        for line in s.split("\n"):
            if "一键部署至 IoT" in line and "[ref=" in line and "disabled" not in line:
                m = re.search(r'\[ref=([^\]]+)\]', line)
                if m:
                    L.click(m.group(1), 2.5)
                    return True
        time.sleep(3)
    return False

def get_latest_task_id():
    fs = sorted(glob.glob("/home/yhz/solo_ai_iot/banckend/storage/tasks/admin/*.json"),
                key=lambda f: os.path.getmtime(f), reverse=True)
    return os.path.splitext(os.path.basename(fs[0]))[0] if fs else ""

def main():
    model = sys.argv[1]
    timeout = int(sys.argv[2]) if len(sys.argv) > 2 else 2400
    print(f"=== 训练模型: {model} (timeout={timeout}s) ===", flush=True)

    # 1. 确认用户名 admin
    L.eval_js("async ()=>{const e=document.querySelector('input[placeholder=输入用户名]'); if(e){e.value='admin'; e.dispatchEvent(new Event('input')); e.dispatchEvent(new Event('change')); return 'ok';} return 'no';}")
    time.sleep(2)

    # 2. 选择数据集 admin/nifi-data → pima_diabetes
    s = L.snap()
    if "pima_train" in s:
        pass  # 已在目标子目录
    elif "pima_diabetes" in s:
        # admin/nifi-data 已选中，直接进入子目录
        if not click_folder_btn("pima_diabetes"):
            print("FAIL: 无法进入 pima_diabetes", flush=True)
            sys.exit(1)
    else:
        open_folder_select()
        if not pick_option("admin/nifi-data"):
            print("FAIL: 无法选择 admin/nifi-data", flush=True)
            sys.exit(1)
        time.sleep(1.5)
        if "pima_train" not in L.snap():
            if not click_folder_btn("pima_diabetes"):
                print("FAIL: 无法进入 pima_diabetes", flush=True)
                sys.exit(1)
    # 3. 勾选文件
    r = check_file("pima_train.csv")
    print("check_file:", r, flush=True)
    time.sleep(0.8)

    # 4. 选择模型
    r = select_model_only(model)
    print("select_model:", r, flush=True)
    time.sleep(0.8)

    # 5. 点击训练
    r = click_train_btn()
    print("train btn:", str(r)[:80], flush=True)
    if '"clicked"' not in str(r):
        print("FAIL: 训练按钮不可用", flush=True)
        sys.exit(1)
    time.sleep(4)

    # 6. 获取 task_id 并等待
    tid = get_latest_task_id()
    print("task_id:", tid, flush=True)
    data = wait_train_done(tid, timeout)
    if data is None:
        print("FAIL: 训练超时", flush=True)
        sys.exit(2)
    status = data.get("status")
    models = data.get("models", [])
    print("=== 训练结果 ===", flush=True)
    print(json.dumps(data, ensure_ascii=False), flush=True)
    if status != "completed":
        print("FAIL: 训练失败", flush=True)
        sys.exit(1)

    # 7. 部署（页面不刷新，等待 leaderboard 更新）
    ok = click_deploy()
    if ok:
        print("=== 部署成功 ===", flush=True)
    else:
        print("WARN: 未找到可部署按钮", flush=True)

    # 输出 JSON 结果供外部解析
    res = {"model": model, "status": status, "models": models, "deployed": ok}
    with open(f"/tmp/ai_train_result_{model}.json", "w") as f:
        json.dump(res, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()