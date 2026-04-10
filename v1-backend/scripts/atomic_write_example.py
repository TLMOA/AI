#!/usr/bin/env python3
"""原子写示例：写入临时文件 -> fsync -> 计算 sha256 -> 原子重命名

示例用法：
  python3 atomic_write_example.py /path/to/target/file.csv "content..."
"""
import os
import sys
import hashlib


def fsync_file(f):
    f.flush()
    os.fsync(f.fileno())


def atomic_write(path, data, tmp_suffix='.tmp'):
    dirpath = os.path.dirname(path)
    basename = os.path.basename(path)
    tmp_path = os.path.join(dirpath, basename + tmp_suffix)

    with open(tmp_path, 'wb') as f:
        if isinstance(data, str):
            data = data.encode('utf-8')
        f.write(data)
        fsync_file(f)

    # 计算 checksum
    h = hashlib.sha256()
    with open(tmp_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    checksum = h.hexdigest()

    # 原子重命名
    os.replace(tmp_path, path)

    return checksum


def main():
    if len(sys.argv) < 3:
        print('usage: atomic_write_example.py <target_path> <content>')
        sys.exit(2)
    target = sys.argv[1]
    content = sys.argv[2]
    os.makedirs(os.path.dirname(target), exist_ok=True)
    cs = atomic_write(target, content)
    print('wrote', target, 'sha256=', cs)


if __name__ == '__main__':
    main()
