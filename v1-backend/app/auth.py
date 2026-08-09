from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
import os
import hashlib
import sqlalchemy
from .engine_factory import engine_from_config
from .db_models import IotUser, get_engine
from sqlalchemy.orm import sessionmaker
from pathlib import Path

# create a local SessionLocal for the application DB (same path as main)
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "app.db"
engine_local = get_engine(DB_PATH)
SessionLocal = sessionmaker(bind=engine_local)
import jwt
import time
from datetime import datetime, timedelta

# JWT settings
SECRET_KEY = os.getenv("IOT_SECRET_KEY", "dev-secret-change-me")
ACCESS_EXPIRE_SECONDS = int(os.getenv("IOT_ACCESS_EXPIRE_SECONDS", "900"))

router = APIRouter()
# which DB to prefer for auth: 'local' or 'nifi'
AUTH_DB = os.getenv("IOT_AUTH_DB", "local").lower()

# v4: 与 main.py 保持一致，公有化用户存放在 /home/yhz/{username}/ 下
IN_DATA_BASE_DIR = Path(os.getenv("IN_DATA_BASE_DIR", "/home/yhz"))


def _create_user_storage_dirs(username: str, deployment_mode: str, ceph_endpoint: str = "") -> None:
    """v4: 注册时自动创建用户完整的目录结构。"""
    if deployment_mode == "private" and ceph_endpoint:
        root = Path(ceph_endpoint)
    else:
        root = IN_DATA_BASE_DIR / username
    try:
        # 创建 nifi-data 及其子目录
        nifi_data = root / "nifi-data"
        nifi_data.mkdir(parents=True, exist_ok=True)
        
        # nifi-data 子目录
        (nifi_data / "export_jobs" / "inbox").mkdir(parents=True, exist_ok=True)
        (nifi_data / "export_jobs" / "done").mkdir(parents=True, exist_ok=True)
        (nifi_data / "export_jobs" / "error").mkdir(parents=True, exist_ok=True)
        (nifi_data / "output_csv").mkdir(parents=True, exist_ok=True)
        (nifi_data / "output_json").mkdir(parents=True, exist_ok=True)
        (nifi_data / "output_tsv").mkdir(parents=True, exist_ok=True)
        (nifi_data / "tagged_output").mkdir(parents=True, exist_ok=True)
        (nifi_data / "silent_exports").mkdir(parents=True, exist_ok=True)
        (nifi_data / "inbox_csv").mkdir(parents=True, exist_ok=True)
        (nifi_data / "inbox_json").mkdir(parents=True, exist_ok=True)
        (nifi_data / "inbox_tsv").mkdir(parents=True, exist_ok=True)
        (nifi_data / "csv_to_json").mkdir(parents=True, exist_ok=True)
        (nifi_data / "csv_to_tsv").mkdir(parents=True, exist_ok=True)
        (nifi_data / "json_to_csv").mkdir(parents=True, exist_ok=True)
        (nifi_data / "json_to_tsv").mkdir(parents=True, exist_ok=True)
        (nifi_data / "tsv_to_csv").mkdir(parents=True, exist_ok=True)
        (nifi_data / "tsv_to_json").mkdir(parents=True, exist_ok=True)
        (nifi_data / "meta_backups").mkdir(parents=True, exist_ok=True)
        (root / "meta_backups").mkdir(parents=True, exist_ok=True)  # v4 P2-5: 全局 meta 版本备份
        
        # 创建 real_nifi_data 及其子目录（与 nifi-data 保持业务目录一致）
        real_nifi_data = root / "real_nifi_data"
        real_nifi_data.mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "export_jobs" / "inbox").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "export_jobs" / "done").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "export_jobs" / "error").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "output_csv").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "output_json").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "output_tsv").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "tagged_output").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "silent_exports").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "inbox_csv").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "inbox_json").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "inbox_tsv").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "csv_to_json").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "csv_to_tsv").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "json_to_csv").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "json_to_tsv").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "tsv_to_csv").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "tsv_to_json").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "meta_backups").mkdir(parents=True, exist_ok=True)
        (real_nifi_data / "bin").mkdir(parents=True, exist_ok=True)

        # v4: 有标签文件独立顶层目录（不放在 nifi-data/real_nifi_data 内）
        # tagged_nifi_data — Local 模式有标签
        # tagged_real_nifi_data — NiFi 模式有标签
        for tagged_root_name in ("tagged_nifi_data", "tagged_real_nifi_data"):
            tagged_root = root / tagged_root_name
            tagged_root.mkdir(parents=True, exist_ok=True)
            for sub in ("inbox_csv", "inbox_json", "inbox_tsv",
                        "csv_to_json", "csv_to_tsv", "json_to_csv",
                        "json_to_tsv", "tsv_to_csv", "tsv_to_json",
                        "output_csv", "output_json", "output_tsv"):
                (tagged_root / sub).mkdir(parents=True, exist_ok=True)
    except (OSError, PermissionError):
        # 只读环境或权限不足时静默跳过，不阻塞注册
        pass


class DBConf(BaseModel):
    db_type: Optional[str] = Field(default=None)
    user: Optional[str] = Field(default=None)
    password: Optional[str] = Field(default=None)
    host: Optional[str] = Field(default=None)
    port: Optional[int] = Field(default=None)
    database: Optional[str] = Field(default=None)
    dsn: Optional[str] = Field(default=None)


class LoginReq(BaseModel):
    username: str = Field(...)
    password: str = Field(...)
    db: Optional[DBConf] = None
    deployment_mode: Optional[str] = Field(default=None)
    ceph_endpoint: Optional[str] = Field(default=None)


def _build_engine_from_env() -> any:
    db_conf = {
        "db_type": os.getenv("NIFI_DB_TYPE", "mysql"),
        "user": os.getenv("NIFI_DB_USER", "root"),
        "password": os.getenv("NIFI_DB_PASSWORD", "root"),
        "host": os.getenv("NIFI_DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("NIFI_DB_PORT", "3306")) if os.getenv("NIFI_DB_PORT") else None,
        "database": os.getenv("NIFI_DB_NAME", "nifi"),
    }
    return engine_from_config(db_conf)


def _build_engine_from_req(db: Optional[DBConf]) -> any:
    if not db:
        return _build_engine_from_env()
    conf = {}
    if db.dsn:
        conf["dsn"] = db.dsn
        return engine_from_config(conf)
    # map fields
    if db.db_type:
        conf["db_type"] = db.db_type
    if db.user:
        conf["user"] = db.user
    if db.password:
        conf["password"] = db.password
    if db.host:
        conf["host"] = db.host
    if db.port:
        conf["port"] = db.port
    if db.database:
        conf["database"] = db.database
    return engine_from_config(conf)


def _verify_password(stored: Optional[str], provided: str) -> bool:
    if stored is None:
        return False
    s = str(stored)
    # exact match (plaintext)
    if s == provided:
        return True
    # sha256 hex
    try:
        if len(s) == 64 and all(c in "0123456789abcdefABCDEF" for c in s):
            return hashlib.sha256(provided.encode("utf-8")).hexdigest() == s
    except Exception:
        pass
    # bcrypt: try when available
    if s.startswith("$2"):
        try:
            import bcrypt

            return bcrypt.checkpw(provided.encode("utf-8"), s.encode("utf-8"))
        except Exception:
            return False
    return False


def _build_user_info(row):
    user_info = {
        "username": row.username,
        "is_admin": bool(row.is_admin),
        "deployment_mode": (row.deployment_mode or "public"),
        "ceph_endpoint": (row.ceph_endpoint or ""),
    }
    return user_info


def _get_all_nifi_db_users() -> List[Dict[str, Any]]:
    """从 NiFi MySQL 数据库读取所有用户记录（供内部管理页合并展示）。"""
    try:
        engine = _build_engine_from_env()
    except Exception:
        return []
    candidate_tables = ["iot_users", "users", "user", "accounts", "admin_users", "nifi_users", "account"]
    user_cols = ["username", "user_name", "login", "account", "name"]
    admin_cols = ["is_admin", "isAdmin", "admin", "is_superuser"]
    mode_cols = ["deployment_mode", "deploymentMode"]
    ceph_cols = ["ceph_endpoint", "cephEndpoint"]
    created_cols = ["created_at", "createdAt"]
    seen = set()
    out: List[Dict[str, Any]] = []
    with engine.connect() as conn:
        for table in candidate_tables:
            try:
                rows = conn.execute(sqlalchemy.text(f"SELECT * FROM {table} LIMIT 10000")).fetchall()
            except Exception:
                continue
            for row in rows:
                mapping = row._mapping
                username = None
                for col in user_cols:
                    if col in mapping and mapping[col]:
                        username = str(mapping[col])
                        break
                if not username:
                    continue
                norm = username.lower()
                if norm in seen:
                    continue
                seen.add(norm)
                is_admin = False
                for col in admin_cols:
                    if col in mapping and mapping[col]:
                        try:
                            is_admin = bool(int(mapping[col]))
                        except Exception:
                            is_admin = bool(mapping[col])
                        break
                deployment_mode = "public"
                for col in mode_cols:
                    if col in mapping and mapping[col]:
                        deployment_mode = str(mapping[col]).lower()
                        break
                ceph_endpoint = ""
                for col in ceph_cols:
                    if col in mapping and mapping[col] is not None:
                        ceph_endpoint = str(mapping[col])
                        break
                created_at = ""
                for col in created_cols:
                    if col in mapping and mapping[col]:
                        created_at = mapping[col]
                        if hasattr(created_at, "isoformat"):
                            created_at = created_at.isoformat()
                        else:
                            created_at = str(created_at)
                        break
                out.append({
                    "username": username,
                    "is_admin": is_admin,
                    "deployment_mode": deployment_mode,
                    "ceph_endpoint": ceph_endpoint,
                    "created_at": created_at,
                })
    return out


def _sync_user_to_local(
    username: str,
    password_hash: str,
    is_admin: int = 0,
    deployment_mode: str = "public",
    ceph_endpoint: str = "",
) -> bool:
    """将用户同步到本地 SQLite，确保内部管理页能展示所有用户。"""
    try:
        sess = SessionLocal()
        try:
            exists = sess.query(IotUser).filter(IotUser.username == username).first()
            if exists:
                exists.password_hash = password_hash or exists.password_hash
                exists.is_admin = is_admin
                exists.deployment_mode = deployment_mode
                exists.ceph_endpoint = ceph_endpoint
            else:
                sess.add(IotUser(
                    username=username,
                    password_hash=password_hash,
                    is_admin=is_admin,
                    deployment_mode=deployment_mode,
                    ceph_endpoint=ceph_endpoint,
                ))
            sess.commit()
            return True
        finally:
            sess.close()
    except Exception:
        return False


def _delete_nifi_db_user(username: str) -> bool:
    """从 NiFi MySQL 的所有候选用户表中删除指定用户，防止删除 SQLite 后 NiFi DB 记录仍回流到用户列表。"""
    try:
        engine = _build_engine_from_env()
    except Exception:
        return False
    candidate_tables = ["iot_users", "users", "user", "accounts", "admin_users", "nifi_users", "account"]
    candidate_user_cols = ["username", "user_name", "login", "account", "name"]
    deleted = False
    with engine.begin() as conn:
        for table in candidate_tables:
            for user_col in candidate_user_cols:
                try:
                    conn.execute(sqlalchemy.text(f"DELETE FROM {table} WHERE {user_col} = :u LIMIT 1"), {"u": username})
                    deleted = True
                except sqlalchemy.exc.ProgrammingError:
                    continue
                except Exception:
                    continue
    return deleted


@router.post("/api/auth/login")
@router.post("/api/v1/auth/login")
def login(req: LoginReq) -> Dict[str, Any]:
    """Attempt to authenticate against a users table in the NiFi database.

    The implementation tries a few common table/column names to be resilient.
    Environment variables to configure DB connection:
      - NIFI_DB_HOST, NIFI_DB_PORT, NIFI_DB_USER, NIFI_DB_PASSWORD, NIFI_DB_NAME
    """
    # We'll attempt auth according to AUTH_DB preference.
    # If AUTH_DB == 'nifi', try NiFi DB first then local; otherwise local first then NiFi.
    MAX_FAILED_ATTEMPTS = 5
    LOCK_DURATION_MINUTES = 10

    def try_local_auth():
        try:
            sess = SessionLocal()
            row = sess.query(IotUser).filter(IotUser.username == req.username).first()
            if row:
                # v4: 检查账号是否被锁定
                if row.locked_until is not None:
                    now_utc = datetime.utcnow()
                    if row.locked_until > now_utc:
                        remaining = int((row.locked_until - now_utc).total_seconds())
                        raise HTTPException(status_code=423, detail=f"账号已锁定，请 {remaining // 60} 分 {remaining % 60} 秒后重试")
                    else:
                        # 锁定已过期，重置
                        row.locked_until = None
                        row.failed_attempts = 0
                        sess.commit()

                if _verify_password(row.password_hash, req.password):
                    # 登录成功，重置失败计数
                    row.failed_attempts = 0
                    row.locked_until = None
                    sess.commit()
                    user_info = _build_user_info(row)
                    payload = {"sub": row.username, "is_admin": bool(row.is_admin), "exp": int(time.time()) + ACCESS_EXPIRE_SECONDS}
                    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
                    resp = JSONResponse({"success": True, "user": user_info})
                    secure_flag = str(os.getenv("IOT_COOKIE_SECURE", "false")).lower() in ("1", "true", "yes")
                    resp.set_cookie("access_token", token, httponly=True, path="/", samesite="lax", secure=secure_flag, max_age=ACCESS_EXPIRE_SECONDS)
                    return resp
                else:
                    # v4: 密码错误，增加失败计数
                    row.failed_attempts = (row.failed_attempts or 0) + 1
                    if row.failed_attempts >= MAX_FAILED_ATTEMPTS:
                        row.locked_until = datetime.utcnow() + timedelta(minutes=LOCK_DURATION_MINUTES)
                        sess.commit()
                        raise HTTPException(status_code=423, detail=f"密码错误次数过多，账号已锁定 {LOCK_DURATION_MINUTES} 分钟")
                    sess.commit()
                    remaining = MAX_FAILED_ATTEMPTS - row.failed_attempts
                    raise HTTPException(status_code=401, detail=f"用户名或密码错误，还剩 {remaining} 次尝试机会")
            else:
                raise HTTPException(status_code=401, detail="用户名或密码错误")
        except HTTPException:
            raise
        except Exception:
            return None

    def try_nifi_auth(set_cookie=True):
        try:
            engine = _build_engine_from_req(req.db)
        except Exception as e:
            # bubble up engine init error only when this path is primary
            return None

        candidate_tables = ["iot_users", "users", "user", "accounts", "admin_users", "nifi_users", "account"]
        candidate_user_cols = ["username", "user_name", "login", "account", "name"]
        candidate_pass_cols = ["password", "passwd", "pwd", "pass_hash", "password_hash"]

        with engine.connect() as conn:
            for table in candidate_tables:
                for user_col in candidate_user_cols:
                    for pass_col in candidate_pass_cols:
                        try:
                            sql = sqlalchemy.text(f"SELECT * FROM {table} WHERE {user_col} = :u LIMIT 1")
                            row = conn.execute(sql, {"u": req.username}).fetchone()
                            if not row:
                                continue
                            stored = None
                            if pass_col in row._mapping:
                                stored = row._mapping.get(pass_col)
                            else:
                                for alt in ["password", "passwd", "pwd", "pass_hash", "password_hash"]:
                                    if alt in row._mapping:
                                        stored = row._mapping.get(alt)
                                        break
                            if _verify_password(stored, req.password):
                                # assemble user info and admin flag if present
                                is_admin = False
                                for admin_key in ("is_admin", "isAdmin", "admin", "is_superuser"):
                                    if admin_key in row._mapping and row._mapping.get(admin_key):
                                        try:
                                            is_admin = bool(int(row._mapping.get(admin_key)))
                                        except Exception:
                                            is_admin = bool(row._mapping.get(admin_key))
                                        break
                                deployment_mode = "public"
                                for mode_key in ("deployment_mode", "deploymentMode"):
                                    if mode_key in row._mapping and row._mapping.get(mode_key):
                                        deployment_mode = str(row._mapping.get(mode_key)).lower()
                                        break
                                ceph_endpoint = ""
                                for ceph_key in ("ceph_endpoint", "cephEndpoint"):
                                    if ceph_key in row._mapping and row._mapping.get(ceph_key) is not None:
                                        ceph_endpoint = str(row._mapping.get(ceph_key))
                                        break
                                user_info = {"username": req.username, "is_admin": is_admin, "deployment_mode": deployment_mode, "ceph_endpoint": ceph_endpoint}
                                # v4: 登录成功后把 NiFi DB 用户同步回 SQLite，确保内部管理页能展示
                                _sync_user_to_local(req.username, stored or "", int(is_admin), deployment_mode, ceph_endpoint)
                                if set_cookie:
                                    payload = {"sub": req.username, "is_admin": is_admin, "exp": int(time.time()) + ACCESS_EXPIRE_SECONDS}
                                    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
                                    resp = JSONResponse({"success": True, "user": user_info})
                                    secure_flag = str(os.getenv("IOT_COOKIE_SECURE", "false")).lower() in ("1", "true", "yes")
                                    resp.set_cookie("access_token", token, httponly=True, path="/", samesite="lax", secure=secure_flag, max_age=ACCESS_EXPIRE_SECONDS)
                                    return resp
                                else:
                                    return {"success": True, "user": user_info}
                            else:
                                raise HTTPException(status_code=401, detail="用户名或密码错误")
                        except sqlalchemy.exc.ProgrammingError:
                            continue
                        except HTTPException:
                            raise
                        except Exception:
                            continue
        return None

    # Branch according to preference
    if AUTH_DB == "nifi":
        # try NiFi first
        nifi_resp = try_nifi_auth(set_cookie=True)
        if nifi_resp:
            return nifi_resp
        # fallback to local
        local_resp = try_local_auth()
        if local_resp:
            return local_resp
    else:
        # default: local first, then NiFi
        local_resp = try_local_auth()
        if local_resp:
            return local_resp
        nifi_resp = try_nifi_auth(set_cookie=True)
        if nifi_resp:
            return nifi_resp

    try:
        engine = _build_engine_from_req(req.db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB engine init failed: {e}")

    candidate_tables = ["iot_users", "users", "user", "accounts", "admin_users", "nifi_users", "account"]
    candidate_user_cols = ["username", "user_name", "login", "account", "name"]
    candidate_pass_cols = ["password", "passwd", "pwd", "pass_hash", "password_hash"]

    with engine.connect() as conn:
        for table in candidate_tables:
            for user_col in candidate_user_cols:
                for pass_col in candidate_pass_cols:
                    try:
                        sql = sqlalchemy.text(f"SELECT * FROM {table} WHERE {user_col} = :u LIMIT 1")
                        row = conn.execute(sql, {"u": req.username}).fetchone()
                        if not row:
                            continue
                        stored = None
                        # try to extract password column if exists
                        if pass_col in row._mapping:
                            stored = row._mapping.get(pass_col)
                        else:
                            # fallback: try common names present in row
                            for alt in ["password", "passwd", "pwd", "pass_hash", "password_hash"]:
                                if alt in row._mapping:
                                    stored = row._mapping.get(alt)
                                    break
                        if _verify_password(stored, req.password):
                            # build simple user info and detect admin flag when present
                            is_admin = False
                            for admin_key in ("is_admin", "isAdmin", "admin", "is_superuser"):
                                if admin_key in row._mapping and row._mapping.get(admin_key):
                                    try:
                                        is_admin = bool(int(row._mapping.get(admin_key)))
                                    except Exception:
                                        is_admin = bool(row._mapping.get(admin_key))
                                    break
                            user_info = {"username": req.username, "is_admin": is_admin, "deployment_mode": "public", "ceph_endpoint": ""}
                            # include display name if available
                            for k in ("display_name", "displayName", "name", "full_name"):
                                if k in row._mapping and row._mapping.get(k):
                                    user_info["displayName"] = row._mapping.get(k)
                                    break
                            return {"success": True, "user": user_info}
                        else:
                            # found user but password mismatch -> auth fail
                            raise HTTPException(status_code=401, detail="用户名或密码错误")
                    except sqlalchemy.exc.ProgrammingError:
                        # table/column may not exist, skip
                        continue
                    except Exception:
                        # other DB error for this attempt, skip to next
                        continue

    # none matched
    raise HTTPException(status_code=401, detail="用户名或密码错误")


@router.post("/api/v1/auth/register")
def register(req: LoginReq):
    # register regular user
    if req.username.lower() == "admin":
        raise HTTPException(status_code=403, detail="管理员账号不可注册")

    try:
        import bcrypt as _bcrypt
        ph = _bcrypt.hashpw(req.password.encode('utf-8'), _bcrypt.gensalt()).decode('utf-8')
    except Exception:
        ph = req.password

    deployment_mode = (req.deployment_mode or "public").lower()
    if deployment_mode not in ("public", "private"):
        raise HTTPException(status_code=400, detail="deployment_mode must be public or private")
    ceph_endpoint = (req.ceph_endpoint or "").strip()
    if deployment_mode == "private" and not ceph_endpoint:
        raise HTTPException(status_code=400, detail="私有化部署必须填写 ceph_endpoint")

    # If configured to use NiFi DB for auth, attempt to write user into NiFi (MySQL)
    if AUTH_DB == "nifi":
        try:
            engine = _build_engine_from_req(req.db)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"无法初始化 NiFi DB 连接: {e}")

        candidate_tables = ["users", "iot_users", "accounts", "account"]
        with engine.begin() as conn:
            # check if user exists in any candidate table
            for table in candidate_tables:
                try:
                    row = conn.execute(sqlalchemy.text(f"SELECT 1 FROM {table} WHERE username = :u LIMIT 1"), {"u": req.username}).fetchone()
                    if row:
                        raise HTTPException(status_code=400, detail="用户名已存在")
                except sqlalchemy.exc.ProgrammingError:
                    # table might not exist, skip
                    continue
                except HTTPException:
                    raise
                except Exception:
                    # other DB error, skip this table
                    continue

            # try to insert into an existing candidate table or create a new one
            for table in candidate_tables:
                try:
                    # ensure table exists with expected columns (safe CREATE IF NOT EXISTS)
                    conn.execute(sqlalchemy.text(
                        f"CREATE TABLE IF NOT EXISTS {table} (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(128) UNIQUE NOT NULL, password_hash VARCHAR(256) NOT NULL, is_admin TINYINT DEFAULT 0, deployment_mode VARCHAR(32) DEFAULT 'public', ceph_endpoint VARCHAR(512) DEFAULT '', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
                    ))
                    # insert user
                    conn.execute(sqlalchemy.text(f"INSERT INTO {table} (username, password_hash, is_admin, deployment_mode, ceph_endpoint) VALUES (:u, :ph, 0, :dm, :ce)"), {"u": req.username, "ph": ph, "dm": deployment_mode, "ce": ceph_endpoint})
                    _create_user_storage_dirs(req.username, deployment_mode, ceph_endpoint)
                    # 同步写入 SQLite，确保文件扫描/权限过滤能识别该用户
                    _sync_user_to_local(req.username, ph, 0, deployment_mode, ceph_endpoint)
                    return {"success": True, "message": "注册成功", "table": table, "deployment_mode": deployment_mode, "ceph_endpoint": ceph_endpoint}
                except sqlalchemy.exc.IntegrityError:
                    raise HTTPException(status_code=400, detail="用户名已存在")
                except Exception:
                    # try next candidate
                    continue

        raise HTTPException(status_code=500, detail="在 NiFi DB 中写入用户失败")

    # Default: write to local application SQLite
    sess = SessionLocal()
    exists = sess.query(IotUser).filter(IotUser.username == req.username).first()
    if exists:
        raise HTTPException(status_code=400, detail="用户名已存在")
    user = IotUser(username=req.username, password_hash=ph, is_admin=0, deployment_mode=deployment_mode, ceph_endpoint=ceph_endpoint)
    sess.add(user)
    sess.commit()
    _create_user_storage_dirs(req.username, deployment_mode, ceph_endpoint)
    return {"success": True, "message": "注册成功", "deployment_mode": deployment_mode, "ceph_endpoint": ceph_endpoint}


def _get_current_user_from_token(token: Optional[str]):
    if not token:
        return None
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return {"username": payload.get("sub"), "is_admin": payload.get("is_admin", False)}
    except Exception:
        return None


@router.get("/api/v1/auth/me")
def me(request: Request):
    cookie = request.cookies.get("access_token")
    user = _get_current_user_from_token(cookie)
    if not user:
        raise HTTPException(status_code=401, detail="未登录")
    # v4: 返回 token 过期时间，前端用于会话过期提醒
    try:
        payload = jwt.decode(cookie, SECRET_KEY, algorithms=["HS256"])
        user["expires_at"] = payload.get("exp")
    except Exception:
        user["expires_at"] = None
    sess = SessionLocal()
    try:
        db_user = sess.query(IotUser).filter(IotUser.username == user["username"]).first()
        if db_user:
            user["deployment_mode"] = db_user.deployment_mode or "public"
            user["ceph_endpoint"] = db_user.ceph_endpoint or ""
        else:
            user["deployment_mode"] = "public"
            user["ceph_endpoint"] = ""
    finally:
        sess.close()
    return {"success": True, "user": user}


@router.post("/api/v1/auth/logout")
def logout():
    # clear cookie
    resp = JSONResponse({"success": True, "message": "已登出"})
    secure_flag = str(os.getenv("IOT_COOKIE_SECURE", "false")).lower() in ("1", "true", "yes")
    # delete_cookie will set Set-Cookie with expires in past
    resp.delete_cookie("access_token", path="/", samesite="lax")
    return resp


@router.post("/api/v1/auth/refresh")
def refresh(request: Request):
    """v4: 刷新会话，延长 token 有效期"""
    cookie = request.cookies.get("access_token")
    user = _get_current_user_from_token(cookie)
    if not user:
        raise HTTPException(status_code=401, detail="未登录")
    payload = {"sub": user["username"], "is_admin": user.get("is_admin", False), "exp": int(time.time()) + ACCESS_EXPIRE_SECONDS}
    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    resp = JSONResponse({"success": True, "message": "会话已延长", "expires_at": payload["exp"]})
    secure_flag = str(os.getenv("IOT_COOKIE_SECURE", "false")).lower() in ("1", "true", "yes")
    resp.set_cookie("access_token", token, httponly=True, path="/", samesite="lax", secure=secure_flag, max_age=ACCESS_EXPIRE_SECONDS)
    return resp
