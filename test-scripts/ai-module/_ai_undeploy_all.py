#!/usr/bin/env python3
"""通过前端高级模型集市下线所有已部署模型"""
import subprocess, time, os, re, sys, json

sys.path.insert(0, "/home/yhz/iot")
import _ai_frontend_lib as L

def goto_market():
    L.run(["goto", "http://127.0.0.1:3002"]); time.sleep(4)
    L.eval_js("async ()=>{const e=document.querySelector('input[placeholder=输入用户名]'); if(e){e.value='admin'; e.dispatchEvent(new Event('input')); e.dispatchEvent(new Event('change')); return 'ok';} return 'no';}")
    time.sleep(2)
    L.eval_js("async ()=>{const tabs=document.querySelectorAll('[role=tab]'); for(const t of tabs){ if(t.textContent.includes('高级模型集市')){ t.click(); return 'ok'; } } return 'no';}")
    time.sleep(2)

def undeploy_all():
    """点击所有下线按钮直到没有"""
    count = 0
    for i in range(50):
        r = L.eval_js("async ()=>{const btns=[...document.querySelectorAll('button')].filter(x=>x.textContent.includes('下线此模型')); if(btns.length){btns[0].click(); return 'clicked';} return 'none';}")
        if '"none"' in str(r):
            break
        time.sleep(1.5)
        # 处理确认弹窗
        r2 = L.eval_js("async ()=>{const btns=document.querySelectorAll('.el-message-box__btns button'); for(const b of btns){ if(b.textContent.includes('确定')||b.textContent.includes('下线')){ b.click(); return 'ok'; } } return 'no';}")
        time.sleep(1.5)
        count += 1
        print(f"  下线 {count} 个", flush=True)
    return count

if __name__ == "__main__":
    goto_market()
    n = undeploy_all()
    print(f"=== 下线完成: {n} 个模型 ===", flush=True)