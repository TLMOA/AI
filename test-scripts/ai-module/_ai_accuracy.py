#!/usr/bin/env python3
"""计算已部署模型对 pima_test.csv 的预测准确率（用原始 Outcome 作真实标签）"""
import requests, csv, json, sys

# 1. 读取真实标签（原始 pima-dataset.csv 的 Outcome 列）
real_labels = []
with open("/home/yhz/iot/pima-dataset.csv") as f:
    reader = csv.DictReader(f)
    for row in reader:
        real_labels.append(int(row["Outcome"].strip()))

# 2. 调用预测接口
r = requests.post("http://127.0.0.1:8002/api/predict/batch_file", data={
    "folder_name": "admin/tagged_nifi_data/pima_predict",
    "file_name": "pima_test.csv",
    "user_id": "admin"
}, timeout=300)
d = r.json()
if d.get("code") != 200:
    print("预测失败:", d)
    sys.exit(1)

print(f"msg: {d.get('msg')}")
print(f"共 {d.get('total_models')} 个模型参与, 总故障 {d.get('total_faults')} 条\n")

# 3. 对每个模型计算准确率
for res in d.get("results", []):
    preds = res.get("predictions", [])
    if not preds:
        print(f"{res['model_name']}: 无预测结果")
        continue
    # predictions 每行含 is_fault
    correct = 0
    total = len(preds)
    for i, p in enumerate(preds):
        if i >= len(real_labels):
            break
        pred_fault = p.get("is_fault", False)
        real_fault = (real_labels[i] == 1)
        if pred_fault == real_fault:
            correct += 1
    acc = correct / total * 100
    print(f"{res['model_name']}: 故障={res.get('fault_count')}/{res.get('total')} 准确率={acc:.2f}% ({correct}/{total})")