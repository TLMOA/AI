#!/usr/bin/env python3
"""对单个已部署模型用完整 768 行预测，与真实标签对比计算准确率。用法: python _ai_accuracy_one.py <model_meta_file>"""
import sys, os, csv, json
os.chdir("/home/yhz/solo_ai_iot/banckend")
sys.path.insert(0, "/home/yhz/solo_ai_iot/banckend")
import pandas as pd
import numpy as np
from predict_service import _load_model_from_path, _predict_generic

meta_file = sys.argv[1]
with open(meta_file) as f:
    meta = json.load(f)

# 真实标签
real = []
with open("/home/yhz/iot/pima-dataset.csv") as f:
    for row in csv.DictReader(f):
        real.append(int(row["Outcome"].strip()))
real = np.array(real)

df = pd.read_csv("/home/yhz/admin/tagged_nifi_data/pima_predict/pima_test.csv")

loaded, mtype_key = _load_model_from_path(meta["model_path"], meta.get("model_type", ""))
preds = _predict_generic(loaded, mtype_key, df.copy())
preds = pd.Series(preds).astype(str).str.strip()
pred_binary = np.array([1 if p == "1" else 0 for p in preds])

tp = int(np.sum((pred_binary == 1) & (real == 1)))
fp = int(np.sum((pred_binary == 1) & (real == 0)))
tn = int(np.sum((pred_binary == 0) & (real == 0)))
fn = int(np.sum((pred_binary == 0) & (real == 1)))
acc = (tp + tn) / len(real)
prec = tp / (tp + fp) if (tp + fp) else 0
rec = tp / (tp + fn) if (tp + fn) else 0
f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0

out = {
    "model": meta.get("display_name", meta.get("model_name")),
    "name": meta.get("model_name"),
    "fault": int(pred_binary.sum()),
    "acc": round(acc, 4), "prec": round(prec, 4), "rec": round(rec, 4), "f1": round(f1, 4),
    "tp": tp, "fp": fp, "tn": tn, "fn": fn
}
print(json.dumps(out, ensure_ascii=False))