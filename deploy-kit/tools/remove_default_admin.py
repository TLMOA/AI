#!/usr/bin/env python3
"""清理【旧版本部署遗留的默认 admin 账号】（弱口令管理员）。

为什么需要它：
  · 老版本后端启动时 init_db() 会自动建一个 admin 账号（密码取 IOT_ADMIN_PASSWORD，历史默认 123456）；
  · 新版本已用补丁（patch_no_default_admin.py）禁止它再被创建，
    但**已经建出来的那一条不会被自动删掉** —— 这是「从旧版本升级」才会有的缺口；
  · 结果是「一套实例 = 一个账号」被破坏，且公网机器上多了一个弱口令管理员（安全隐患）。
  · verify.sh / smoke_test.sh 会因此报「用户总数为 2（单账号模式应为 1）」。

⚠️ 两个库都要查、都要清（这点很关键，踩过坑）：
  · MySQL `nifi.iot_users`     —— 认证源（userctl.py list 查的是它）
  · SQLite `v1-backend/data/app.db` 的 `iot_users` —— **内部管理页列表读的是它**
  真实机器上残留可能只在其中一个库里（2026-09-16 目标机实测：MySQL 干净、SQLite 里多一条），
  只查一个库会漏掉。

安全约束（只删「能证明是默认账号」的那一条，绝不误删真实账号）：
  · 站点账号本身就叫 admin 时 → 【不动】，只提示人工确认；
  · 密码哈希必须匹配已知默认口令（123456 / admin / $IOT_ADMIN_PASSWORD）才删；
    匹配不上 → 只告警，交人工判断。

用法:
  python3 remove_default_admin.py [--site-user humi] [--dry-run]

退出码：0=已处理或无残留；2=发现 admin 但密码非默认（需人工）；1=出错
环境变量: CODE_DIR / DB_HOST / DB_PORT / DB_USER / DB_PASS / DB_NAME / IN_DATA_BASE_DIR
"""
import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bcrypt  # noqa: E402
from sqlalchemy import text  # noqa: E402
import userctl  # noqa: E402  —— 复用它已读好的 DB_* 环境变量与 engine()

TARGET = "admin"


def sqlite_session():
    """打开后端自己的 SQLite 会话（内部管理页读的就是它）。失败返回 (None, None)。"""
    try:
        from app.db_models import IotUser          # noqa
        from app.auth import SessionLocal          # noqa
        return SessionLocal(), IotUser
    except Exception as e:
        print(f"[warn] 打不开 SQLite（{e}）→ 只检查 MySQL")
        return None, None


def find_in_mysql():
    try:
        eng = userctl.engine()
        with eng.begin() as conn:
            for u, ph, adm in conn.execute(text(
                    "SELECT username, password_hash, is_admin FROM iot_users")):
                if u == TARGET:
                    return (ph, adm)
    except Exception as e:
        print(f"[warn] 查询 MySQL 失败（{e}）→ 跳过 MySQL")
    return None


def find_in_sqlite():
    sess, IotUser = sqlite_session()
    if sess is None:
        return None
    try:
        row = sess.query(IotUser).filter(IotUser.username == TARGET).first()
        return (row.password_hash, row.is_admin) if row else None
    except Exception as e:
        print(f"[warn] 查询 SQLite 失败（{e}）")
        return None
    finally:
        sess.close()


def _checkpw(plain: str, hashed) -> bool:
    try:
        if isinstance(hashed, str):
            hashed = hashed.encode()
        return bcrypt.checkpw(plain.encode(), hashed)
    except Exception:
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--site-user", default=os.environ.get("SITE_USER", ""),
                    help="本站账号名；若它就叫 admin 则不做任何删除")
    ap.add_argument("--dry-run", action="store_true", help="只报告不删除")
    a = ap.parse_args()

    # 保护条款①：站点账号本身就是 admin → 交人工判断
    if a.site_user and a.site_user == TARGET:
        print(f"[skip] 本站账号就是 {TARGET}，不自动删除（请人工确认它是不是当初那个默认账号）")
        return 0

    # 已知默认口令候选。为什么有 ADMIN_PASS / SITE_PASS：
    #   服务模板里有 `Environment=IOT_ADMIN_PASSWORD=__SITE_PASS__`（兜底，防止默认 admin 用弱口令）
    #   → 后端 init_db() 自建的 admin 密码 = **本站账号密码**（= .deploy-secrets 里的 ADMIN_PASS）。
    #   所以「密码 == 本站密码」的 admin 就是那条自动建的账号，不是谁的真实账号。
    defaults = [d for d in dict.fromkeys([
        "123456",                                   # 代码里的硬编码默认
        "admin",
        os.environ.get("IOT_ADMIN_PASSWORD", ""),    # 显式覆盖
        os.environ.get("SITE_PASS", ""),             # 本站账号密码
        os.environ.get("ADMIN_PASS", ""),            # 旧版部署生成的随机管理员口令
    ]) if d]

    in_mysql = find_in_mysql()
    in_sqlite = find_in_sqlite()
    where = [n for n, v in (("MySQL", in_mysql), ("SQLite", in_sqlite)) if v is not None]
    if not where:
        print(f"[ok] MySQL 与 SQLite 里都没有 {TARGET} 账号（单账号模式正常）")
        return 0

    print(f"[find] 发现 {TARGET} 账号，位置：{'、'.join(where)}")
    matched = ""
    for src in where:
        ph = (in_mysql or ("", 0))[0] if src == "MySQL" else (in_sqlite or ("", 0))[0]
        m = next((d for d in defaults if _checkpw(d, ph)), "")
        if m:
            matched = m
            break

    # 保护条款②：密码不是已知默认口令 → 可能是真实账号，不删
    if not matched:
        print(f"[warn] {TARGET} 的密码不是已知默认口令 → 不自动删除。")
        print("       请人工确认：是默认账号就删掉/改密码，是真实账号则保留")
        return 2

    # ⚠️ 千万不要把匹配到的口令打印出来！它往往就是**本站账号密码**
    #    （模板里 IOT_ADMIN_PASSWORD=__SITE_PASS__），打到终端/日志 = 把密码明文留在屏幕上。
    #    2026-09-16 真机踩过这个坑，只报「属于已知默认口令」即可。
    print("[ok] 其口令属于已知默认口令 → 认定为旧版遗留账号（口令本身不显示）")
    if a.dry_run:
        print("[dry-run] 只报告，不删除")
        return 0

    done = []
    if in_mysql is not None:
        try:
            with userctl.engine().begin() as conn:
                conn.execute(text("DELETE FROM iot_users WHERE username=:u"), {"u": TARGET})
            done.append("MySQL")
        except Exception as e:
            print(f"[err] 删除 MySQL 记录失败：{e}")
    if in_sqlite is not None:
        sess, IotUser = sqlite_session()
        if sess is not None:
            try:
                n = sess.query(IotUser).filter(IotUser.username == TARGET).delete()
                sess.commit()
                done.append(f"SQLite({n} 条)")
            except Exception as e:
                print(f"[err] 删除 SQLite 记录失败：{e}")
            finally:
                sess.close()

    if not done:
        print("[err] 两个库都没能删除成功")
        return 1
    print(f"[done] 已删除旧版遗留的默认 {TARGET} 账号：{' + '.join(done)}")
    data_dir = Path(os.environ.get("IN_DATA_BASE_DIR", "/home/yhz")) / TARGET
    if data_dir.exists():
        print(f"[note] 该账号的数据目录仍在：{data_dir}（不动数据；确认要清理请人工处理）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
