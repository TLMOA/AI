#!/usr/bin/env python3
"""对已部署模型用完整 768 行预测，与真实标签对比计算准确率/精确率/召回率/F1"""
import sys, os, csv, json
# pycaret 模型含相对路径依赖，需在 banckend 目录运行
os.chdir("/home/yhz/solo_ai_iot/banckend")
sys.path.insert(0, "/home/yhz/solo_ai_iot/banckend")
import pandas as pd
import numpy as np
from predict_service import _load_model_from_path, _predict_generic

# 1. 真实标签
real = []
with open("/home/yhz/iot/pima-dataset.csv") as f:
    for row in csv.DictReader(f):
        real.append(int(row["Outcome"].strip()))
real = np.array(real)

# 2. 预测数据（完整 768 行，无 Outcome）
df = pd.read_csv("/home/yhz/admin/tagged_nifi_data/pima_predict/pima_test.csv")

# 3. 遍历已部署模型
deployed_dir = "/home/yhz/solo_ai_iot/banckend/storage/deployed_models/admin"
results = []
for meta_file in sorted(os.listdir(deployed_dir)):
    if not meta_file.endswith(".meta.json"):
        continue
    with open(os.path.join(deployed_dir, meta_file)) as f:
        meta = json.load(f)
    model_path = meta.get("model_path", "")
    model_type = meta.get("model_type", "")
    if not os.path.exists(model_path):
        print(f"跳过(文件缺失): {meta.get('model_name')}")
        continue
    try:
        loaded, mtype_key = _load_model_from_path(model_path, model_type)
        preds = _predict_generic(loaded, mtype_key, df.copy())
        preds = pd.Series(preds).astype(str).str.strip()
        pred_binary = np.array([1 if p == "1" else 0 for p in preds])
        if len(pred_binary) != len(real):
            print(f"长度不一致: {meta.get('model_name')} pred={len(pred_binary)} real={len(real)}")
            continue
        tp = int(np.sum((pred_binary == 1) & (real == 1)))
        fp = int(np.sum((pred_binary == 1) & (real == 0)))
        tn = int(np.sum((pred_binary == 0) & (real == 0)))
        fn = int(np.sum((pred_binary == 0) & (real == 1)))
        acc = (tp + tn) / len(real)
        prec = tp / (tp + fp) if (tp + fp) else 0
        rec = tp / (tp + fn) if (tp + fn) else 0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0
        results.append({
            "model": meta.get("display_name", meta.get("model_name")),
            "name": meta.get("model_name"),
            "fault": int(pred_binary.sum()),
            "acc": round(acc, 4), "prec": round(prec, 4), "rec": round(rec, 4), "f1": round(f1, 4),
            "tp": tp, "fp": fp, "tn": tn, "fn": fn
        })
        print(f"{meta.get('display_name'):<20} 故障={pred_binary.sum():>4}/768  准确率={acc*100:5.2f}%  精确率={prec*100:5.2f}%  召回率={rec*100:5.2f}%  F1={f1:.4f}")
    except Exception as e:
        print(f"预测失败 {meta.get('model_name')}: {e}")

with open("/tmp/ai_accuracy_results.json", "w") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)
print(f"\n共评估 {len(results)} 个模型")