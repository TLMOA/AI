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
    # First, try application-level users stored in iot_users (preferred)
    try:
        sess = SessionLocal()
        row = sess.query(IotUser).filter(IotUser.username == req.username).first()
        if row:
            if _verify_password(row.password_hash, req.password):
                user_info = {"username": row.username, "is_admin": bool(row.is_admin)}
                # issue JWT
                payload = {"sub": row.username, "is_admin": bool(row.is_admin), "exp": int(time.time()) + ACCESS_EXPIRE_SECONDS}
                token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
                # set cookie using Response API so we can control flags
                resp = JSONResponse({"success": True, "user": user_info})
                secure_flag = str(os.getenv("IOT_COOKIE_SECURE", "false")).lower() in ("1", "true", "yes")
                resp.set_cookie("access_token", token, httponly=True, path="/", samesite="lax", secure=secure_flag, max_age=ACCESS_EXPIRE_SECONDS)
                return resp
            else:
                raise HTTPException(status_code=401, detail="用户名或密码错误")
    except Exception:
        # fallthrough to try NiFi DB
        pass

    try:
        engine = _build_engine_from_req(req.db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB engine init failed: {e}")

    candidate_tables = ["users", "user", "accounts", "admin_users", "nifi_users", "account"]
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
                            # build simple user info
                            user_info = {"username": req.username}
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
    # register regular user into iot_users; disallow creating admin
    if req.username.lower() == "admin":
        raise HTTPException(status_code=403, detail="管理员账号不可注册")
    sess = SessionLocal()
    exists = sess.query(IotUser).filter(IotUser.username == req.username).first()
    if exists:
        raise HTTPException(status_code=400, detail="用户名已存在")
    try:
        import bcrypt as _bcrypt
        ph = _bcrypt.hashpw(req.password.encode('utf-8'), _bcrypt.gensalt()).decode('utf-8')
    except Exception:
        ph = req.password
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
