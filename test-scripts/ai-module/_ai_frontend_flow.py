#!/usr/bin/env python3
"""AI 训练网站前端全流程测试：训练单个模型 → 部署 → 预测"""
import subprocess, time, os, re, sys, json, glob

sys.path.insert(0, "/home/yhz/iot")
import _ai_frontend_lib as L

RESULTS = []

def rec(mod, name, passed, detail=""):
    RESULTS.append(dict(module=mod, name=name, passed=passed, detail=detail))
    print(f"  [{'PASS' if passed else 'FAIL'}] {mod}: {name}  {detail}", flush=True)

def login_admin():
    L.run(["goto", "http://127.0.0.1:3002"]); time.sleep(4)
    L.eval_js("async ()=>{const e=document.querySelector('input[placeholder=输入用户名]'); if(e){e.value='admin'; e.dispatchEvent(new Event('input')); e.dispatchEvent(new Event('change')); return 'ok';} return 'no';}")
    time.sleep(2)

def choose_train_data():
    """选择 admin/nifi-data → pima_diabetes → pima_train.csv"""
    # 打开文件夹下拉
    rid = L.ref("请选择一个数据集文件夹")
    if not rid:
        rid = L.ref("选择数据集文件夹")
    L.click(rid, 1.5)
    # 选 admin/nifi-data
    s = L.snap()
    m = re.search(r'option "admin/nifi-data" \[ref=([^\]]+)\]', s)
    if m:
        L.click(m.group(1), 1.5)
    else:
        return False
    # 点 pima_diabetes 子目录
    s = L.snap()
    m = re.search(r'button "📁 pima_diabetes" \[ref=([^\]]+)\]', s)
    if m:
        L.click(m.group(1), 1.5)
    else:
        return False
    # 勾选 pima_train.csv
    s = L.snap()
    m = re.search(r'checkbox "pima_train.csv"', s)
    if not m:
        return False
    # 找 checkbox 的 label 父级 ref
    s2 = L.snap()
    m2 = re.search(r'checkbox "pima_train.csv"\n([^\n]*\[ref=([^\]]+)\])', s2)
    # 用 eval 点击
    L.eval_js("async ()=>{const labels=document.querySelectorAll('.el-checkbox'); for(const l of labels){ if(l.textContent.includes('pima_train.csv') && !l.className.includes('is-checked')){ l.click(); break; } } return 'ok';}")
    time.sleep(1)
    return True, ""

def choose_model(model_name):
    """只勾选指定模型，清空其他"""
    L.eval_js("async ()=>{const labels=document.querySelectorAll('.el-checkbox'); for(const l of labels){ const t=l.textContent.trim(); if(t==='全选本文件夹') continue; const want=t.startsWith('"+model_name+"'); if(want && !l.className.includes('is-checked')){l.click();} else if(!want && l.className.includes('is-checked')){l.click();} } return 'ok';}")
    time.sleep(1)
    # 验证
    s = L.snap()
    return model_name in s

def start_train():
    """点击一键训练，返回 task_id"""
    L.eval_js("async ()=>{const btns=[...document.querySelectorAll('button')].filter(x=>x.textContent.includes('一键训练')); if(btns.length){btns[0].click(); return 'clicked';} return 'no';}")
    time.sleep(3)
    # 从最新 task 文件拿 task_id
    fs = sorted(glob.glob("/home/yhz/solo_ai_iot/banckend/storage/tasks/admin/*.json"),
                key=lambda f: os.path.getmtime(f), reverse=True)
    if not fs:
        return ""
    return os.path.splitext(os.path.basename(fs[0]))[0]

def wait_train(task_id, timeout=1800):
    """轮询训练状态直至完成"""
    import requests
    end = time.time() + timeout
    while time.time() < end:
        try:
            r = requests.get(f"http://127.0.0.1:8002/api/train/status/{task_id}?user_id=admin", timeout=10).json()
            data = r.get("data", {})
            status = data.get("status", "?")
            models = data.get("models", [])
            if status in ("completed", "failed"):
                return data
        except Exception:
            pass
        time.sleep(5)
    return None

def deploy_model():
    """点击 leaderboard 的一键部署至 IoT"""
    # 等待部署按钮可用
    end = time.time() + 60
    while time.time() < end:
        s = L.snap()
        m = re.search(r'button "一键部署至 IoT" \[ref=([^\]]+)\]', s)
        if m:
            # 检查是否 disabled
            line = [l for l in s.split("\n") if '一键部署至 IoT' in l and '[ref=' in l]
            if line and 'disabled' not in line[0]:
                L.click(m.group(1), 2)
                return True
        time.sleep(3)
    return False

def undeploy_all():
    """通过高级模型集市下线所有模型"""
    L.run(["goto", "http://127.0.0.1:3002"]); time.sleep(3)
    L.eval_js("async ()=>{const e=document.querySelector('input[placeholder=输入用户名]'); if(e){e.value='admin'; e.dispatchEvent(new Event('input')); e.dispatchEvent(new Event('change')); return 'ok';} return 'no';}")
    time.sleep(2)
    # 切到高级模型集市 tab
    s = L.snap()
    m = re.search(r'tab "高级模型集市" \[ref=([^\]]+)\]', s)
    if m:
        L.click(m.group(1), 2)
    # 下线所有模型
    for i in range(20):
        s = L.snap()
        m = re.search(r'button "下线此模型"\[ref=([^\]]+)\]', s)
        if not m:
            m = re.search(r'button "下线此模型" [ref=([^\]]+)\]', s)
        if not m:
            break
        L.click(m.group(1), 1.5)
        # 处理确认弹窗
        L.run(["dialog-accept", "确定下线"]); time.sleep(1.5)
    return True

def predict_file():
    """在线推理测试：选文件夹+文件，预测"""
    # 切到在线推理测试
    s = L.snap()
    m = re.search(r'tab "在线推理测试" \[ref=([^\]]+)\]', s)
    if m:
        L.click(m.group(1), 2)
    # 选预测文件夹
    s = L.snap()
    m = re.search(r'请选择" [ref=([^\]]+)\]', s)
    # 预测文件夹下拉
    L.eval_js("async ()=>{const selects=document.querySelectorAll('.el-select'); for(const s of selects){ const ph=s.querySelector('input'); if(ph && ph.placeholder==='请选择'){ s.click(); break; } } return 'ok';}")
    time.sleep(1.5)
    s = L.snap()
    m = re.search(r'option "admin/tagged_nifi_data" \[ref=([^\]]+)\]', s)
    if m:
        L.click(m.group(1), 1.5)
    else:
        return False, "无 admin/tagged_nifi_data 选项"
    # 进入 pima_predict 子目录
    s = L.snap()
    m = re.search(r'button "📁 pima_predict" \[ref=([^\]]+)\]', s)
    if m:
        L.click(m.group(1), 1.5)
    else:
        return False, "无 pima_predict 子目录"
    # 选择 pima_test.csv
    s = L.snap()
    m = re.search(r'pima_test\.csv', s)
    if not m:
        return False, "无 pima_test.csv"
    # 点击文件下拉
    L.eval_js("async ()=>{const selects=document.querySelectorAll('.el-select'); for(const sel of selects){ const txt=sel.textContent; if(txt.includes('请选择文件')){ sel.click(); return 'ok'; } } return 'no';}")
    time.sleep(1.5)
    s = L.snap()
    m = re.search(r'pima_test\.csv[^\n]*\[ref=([^\]]+)\]', s)
    if m:
        L.click(m.group(1), 1)
    else:
        return False, "文件下拉无 pima_test.csv"
    # 点击提交预测
    L.eval_js("async ()=>{const btns=[...document.querySelectorAll('button')].filter(x=>x.textContent.includes('提交至已部署')); if(btns.length){btns[0].click(); return 'clicked';} return 'no';}")
    time.sleep(5)
    return True, ""

def get_predict_result(timeout=120):
    """等待并抓取预测结果（故障数）"""
    import requests
    end = time.time() + timeout
    last = ""
    while time.time() < end:
        s = L.snap()
        # 诊断结果报告
        m = re.search(r'诊断结果报告[^\n]*', s)
        if m:
            last = m.group(0)
        # 故障数
        m2 = re.search(r'(\d+)\s*/\s*(\d+)\s*条故障', s)
        if m2:
            return int(m2.group(1)), int(m2.group(2)), s
        # 全部正常
        if "全部正常" in s:
            m3 = re.search(r'全部正常 \((\d+) 条\)', s)
            return 0, int(m3.group(1)) if m3 else 0, s
        time.sleep(3)
    return None, None, last

if __name__ == "__main__":
    # 测试单模型
    login_user()
    ok, err = choose_train_fold()
    print("choose_train_fold:", ok, err)
    ok2 = choose_model("xgboost")
    print("choose_model:", ok2)
    tid = start_training()
    print("task:", tid)
    data = wait_train(tid, 600)
    print("train result:", json.dumps(data, ensure_ascii=False)[:300])