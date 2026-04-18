from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
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

# JWT settings
SECRET_KEY = os.getenv("IOT_SECRET_KEY", "dev-secret-change-me")
ACCESS_EXPIRE_SECONDS = int(os.getenv("IOT_ACCESS_EXPIRE_SECONDS", "900"))

router = APIRouter()
# which DB to prefer for auth: 'local' or 'nifi'
AUTH_DB = os.getenv("IOT_AUTH_DB", "local").lower()


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
    def try_local_auth():
        try:
            sess = SessionLocal()
            row = sess.query(IotUser).filter(IotUser.username == req.username).first()
            if row:
                if _verify_password(row.password_hash, req.password):
                    user_info = {"username": row.username, "is_admin": bool(row.is_admin)}
                    payload = {"sub": row.username, "is_admin": bool(row.is_admin), "exp": int(time.time()) + ACCESS_EXPIRE_SECONDS}
                    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
                    resp = JSONResponse({"success": True, "user": user_info})
                    secure_flag = str(os.getenv("IOT_COOKIE_SECURE", "false")).lower() in ("1", "true", "yes")
                    resp.set_cookie("access_token", token, httponly=True, path="/", samesite="lax", secure=secure_flag, max_age=ACCESS_EXPIRE_SECONDS)
                    return resp
                else:
                    raise HTTPException(status_code=401, detail="用户名或密码错误")
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
                                user_info = {"username": req.username, "is_admin": is_admin}
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
                            user_info = {"username": req.username, "is_admin": is_admin}
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
                        f"CREATE TABLE IF NOT EXISTS {table} (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(128) UNIQUE NOT NULL, password_hash VARCHAR(256) NOT NULL, is_admin TINYINT DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
                    ))
                    # insert user
                    conn.execute(sqlalchemy.text(f"INSERT INTO {table} (username, password_hash, is_admin) VALUES (:u, :ph, 0)"), {"u": req.username, "ph": ph})
                    return {"success": True, "message": "注册成功", "table": table}
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
    user = IotUser(username=req.username, password_hash=ph, is_admin=0)
    sess.add(user)
    sess.commit()
    return {"success": True, "message": "注册成功"}


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
    return {"success": True, "user": user}


@router.post("/api/v1/auth/logout")
def logout():
    # clear cookie
    resp = JSONResponse({"success": True, "message": "已登出"})
    secure_flag = str(os.getenv("IOT_COOKIE_SECURE", "false")).lower() in ("1", "true", "yes")
    # delete_cookie will set Set-Cookie with expires in past
    resp.delete_cookie("access_token", path="/", samesite="lax")
    return resp
