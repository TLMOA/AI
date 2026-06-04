#!/usr/bin/env python3
"""清理 nifi-data 目录中的冗余 .meta.json 文件。

删除旧格式的 meta 文件（缺少数据文件扩展名），保留正确命名的 meta。
运行方式: python3 /home/yhz/iot/V3执行清单/cleanup_meta.py
"""

import os

DATA_DIR = "/home/yhz/nifi-data"


def clean():
    data_set = set()
    meta_set = {}

    for root, dirs, files in os.walk(DATA_DIR):
        for f in files:
            fp = os.path.join(root, f)
            if "meta_backups" in fp:
                continue
            if f.endswith(".meta.json"):
                meta_set[fp] = f
            else:
                data_set.add(fp)

    wrong_to_delete = []
    correct_meta = []

    for mp in meta_set:
        data_path = mp.replace(".meta.json", "", 1)
        if data_path in data_set:
            correct_meta.append(mp)
        else:
            wrong_to_delete.append(mp)

    print(f"正确的 meta: {len(correct_meta)}")
    print(f"冗余的 meta: {len(wrong_to_delete)}")
    print()

    for i, wm in enumerate(wrong_to_delete):
        os.remove(wm)
        if (i + 1) % 50 == 0:
            print(f"  已删除 {i + 1} ...")

    print(f"\n完成: 删除了 {len(wrong_to_delete)} 个冗余 meta 文件")
    print(f"保留了 {len(correct_meta)} 个正确 meta 文件")


if __name__ == "__main__":
    clean()