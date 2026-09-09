"""迁移旧 tagged_output 数据到独立顶层 tagged_nifi_data/tagged_real_nifi_data
处理子目录 + 平铺的 tag_*.csv 文件
"""
import os, shutil

BASE = '/home/yhz'
# 用户目录（排除全局 real_nifi_data、nifi-data、iot 等系统目录）
SKIP = {'real_nifi_data', 'nifi-data', 'iot', 'iot_clone', 'miniconda3', 'snap',
        'tmp', '.cache', '.config', 'in_data'}

def migrate(src_tagged, dst_root):
    if not os.path.isdir(src_tagged):
        return 0
    os.makedirs(dst_root, exist_ok=True)
    moved = 0
    for item in os.listdir(src_tagged):
        s = os.path.join(src_tagged, item)
        if os.path.isdir(s):
            # 子目录：inbox_csv/csv_to_json 等 → dst_root/<sub>/
            d = os.path.join(dst_root, item)
            os.makedirs(d, exist_ok=True)
            for f in os.listdir(s):
                sf = os.path.join(s, f)
                if os.path.isfile(sf):
                    shutil.move(sf, os.path.join(d, f))
                    moved += 1
        elif os.path.isfile(s):
            # 平铺文件：tag_*.csv → dst_root/ 根目录
            shutil.move(s, os.path.join(dst_root, item))
            moved += 1
    return moved

total = 0
for d in sorted(os.listdir(BASE)):
    up = os.path.join(BASE, d)
    if not os.path.isdir(up) or d in SKIP:
        continue
    # Local: nifi-data/tagged_output → tagged_nifi_data
    n = migrate(os.path.join(up, 'nifi-data', 'tagged_output'), os.path.join(up, 'tagged_nifi_data'))
    if n:
        print(f'{d}: nifi-data/tagged_output → tagged_nifi_data 迁移 {n} 文件')
        total += n
    # NiFi: real_nifi_data/tagged_output → tagged_real_nifi_data
    n2 = migrate(os.path.join(up, 'real_nifi_data', 'tagged_output'), os.path.join(up, 'tagged_real_nifi_data'))
    if n2:
        print(f'{d}: real_nifi_data/tagged_output → tagged_real_nifi_data 迁移 {n2} 文件')
        total += n2

# 全局 real_nifi_data/tagged_output → /home/yhz/tagged_real_nifi_data
g = migrate(os.path.join(BASE, 'real_nifi_data', 'tagged_output'), os.path.join(BASE, 'tagged_real_nifi_data'))
if g:
    print(f'全局: real_nifi_data/tagged_output → /home/yhz/tagged_real_nifi_data 迁移 {g} 文件')
    total += g

print(f'\n共迁移 {total} 个文件')