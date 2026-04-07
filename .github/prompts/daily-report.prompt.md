---
name: daily-report
description: "Generate a structured daily report for the AI module project with progress, blockers, next steps, and deliverables."
---

# Daily Report Prompt

请根据我提供的内容，生成一份今日日报。

## 输入

- 今日目标：{{goal}}
- 完成事项：{{done}}
- 进行中事项：{{in_progress}}
- 遇到问题：{{issues}}
- 明日计划：{{next_day}}
- 风险与协调项：{{risks}}
- 产出物清单：{{artifacts}}

## 输出要求

1. 使用中文。
2. 使用状态标记：✅ ⚠️ ⏳ 📌。
3. 结构固定为：
   - 今日目标
   - 完成事项
   - 进行中事项
   - 遇到问题
   - 明日计划
   - 风险与协调项
   - 产出物清单
4. 内容尽量简洁，可直接粘贴到日报文件。
