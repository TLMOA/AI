from pathlib import Path
import sys
import pandas as pd

src = Path('/home/yhz/iot/AI模块实施状态追踪表_简化.csv')
if not src.exists():
    print(f'CSV 文件未找到: {src}')
    sys.exit(2)

dst = src.with_suffix('.xlsx')

# 读取 CSV 并写入 XLSX
try:
    df = pd.read_csv(src, encoding='utf-8')
    df.to_excel(dst, index=False)
    print(f'已生成: {dst}')
except Exception as e:
    print('转换失败:', e)
    sys.exit(1)
