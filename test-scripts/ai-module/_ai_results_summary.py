#!/usr/bin/env python3
"""汇总 AI 训练网站 10 个单模型测试结果"""
import json

# 前 5 个模型结果（从训练日志恢复）
early = [
    {"model": "xgboost", "f1": 0.7778, "acc": 0.7857, "pred_fault": 343, "pred_total": 768},
    {"model": "LightGBM", "f1": 0.6948, "acc": 0.7338, "pred_fault": 570, "pred_total": 768},
    {"model": "CatBoost", "f1": 0.6992, "acc": 0.7597, "pred_fault": 881, "pred_total": 768},
    {"model": "hgb (梯度提升树)", "f1": 0.6667, "acc": 0.7208, "pred_fault": 1179, "pred_total": 768},
    {"model": "TabNet (表格注意力)", "f1": 0.5917, "acc": 0.6003, "pred_fault": 1568, "pred_total": 768},
]

# 后 5 个模型结果（从 ai_all_results.json）
try:
    with open("/tmp/ai_all_results.json") as f:
        late = json.load(f)
    late_clean = []
    for r in late:
        m = r["train"]["models"][0]
        late_clean.append({
            "model": r["model"],
            "f1": m.get("f1_score"),
            "acc": m.get("accuracy"),
            "pred_fault": r.get("predict_fault"),
            "pred_total": 768,
        })
except Exception as e:
    print("读取失败:", e)
    late_clean = []

all_res = early + late_clean
with open("/home/yhz/iot/ai_single_model_results.json", "w") as f:
    json.dump(all_res, f, ensure_ascii=False, indent=2)

print("=== 10 个单模型训练+预测结果 ===")
print(f"{'模型':<25}{'F1':<10}{'准确率':<10}{'预测故障':<12}")
for r in all_res:
    print(f"{r['model']:<25}{r['f1']:<10}{r['acc']:<10}{r['pred_fault']}/{r['pred_total']}")