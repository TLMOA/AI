from fastapi import APIRouter, Body, Header
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
import sqlalchemy

router = APIRouter()

class DBConnectReq(BaseModel):
    db_type: str = Field(..., description="数据库类型，如mysql/postgres/sqlite")
    host: str = Field(..., description="主机名或IP")
    port: int = Field(..., description="端口号")
    username: str = Field(..., description="用户名")
    password: str = Field(..., description="密码")
    database: str = Field(..., description="数据库名")
    params: Optional[Dict[str, Any]] = Field(default_factory=dict, description="其他连接参数")

class DBConnectResp(BaseModel):
    code: int
    message: str
    detail: Optional[str] = None


class TableListResp(BaseModel):
    code: int
    message: str
    data: Optional[list] = None
    detail: Optional[str] = None

@router.post("/api/v1/db/test-connection", response_model=DBConnectResp)
def test_db_connection(req: DBConnectReq, x_trace_id: Optional[str] = Header(default=None)):
    """
    客服友好型数据库连接测试接口。
    1. 校验参数完整性
    2. 连接数据库，捕获常见错误，返回友好提示
    3. 失败时给出详细错误原因
    """
    # 参数校验
    if not req.host or not req.port or not req.username or not req.database:
        return DBConnectResp(code=1001, message="参数缺失，请检查主机、端口、用户名、数据库名是否填写完整")
    # 构建连接字符串
    try:
        if req.db_type == "mysql":
            url = f"mysql+pymysql://{req.username}:{req.password}@{req.host}:{req.port}/{req.database}"
        elif req.db_type == "postgres":
            url = f"postgresql://{req.username}:{req.password}@{req.host}:{req.port}/{req.database}"
        elif req.db_type == "sqlite":
            url = f"sqlite:///{req.database}"
        else:
            return DBConnectResp(code=1002, message=f"暂不支持的数据库类型: {req.db_type}")
        engine = sqlalchemy.create_engine(url, connect_args=req.params or {})
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("SELECT 1"))
        return DBConnectResp(code=0, message="连接成功")
    except sqlalchemy.exc.OperationalError as e:
        return DBConnectResp(code=2001, message="连接失败，网络或认证错误", detail=str(e))
    except sqlalchemy.exc.ProgrammingError as e:
        return DBConnectResp(code=2002, message="连接失败，数据库不存在或权限不足", detail=str(e))
    except Exception as e:
        return DBConnectResp(code=9999, message="连接失败，未知错误", detail=str(e))


@router.post("/api/v1/db/list-tables")
def list_tables(req: DBConnectReq, x_trace_id: Optional[str] = Header(default=None)):
    """返回指定数据库下的表名列表（简易实现，支持 mysql/postgres/sqlite）。"""
    try:
        if req.db_type == "mysql":
            url = f"mysql+pymysql://{req.username}:{req.password}@{req.host}:{req.port}/{req.database}"
        elif req.db_type == "postgres":
            url = f"postgresql://{req.username}:{req.password}@{req.host}:{req.port}/{req.database}"
        elif req.db_type == "sqlite":
            url = f"sqlite:///{req.database}"
        else:
            return TableListResp(code=1002, message=f"暂不支持的数据库类型: {req.db_type}")

        engine = sqlalchemy.create_engine(url, connect_args=req.params or {})
        tables = []
        with engine.connect() as conn:
            if req.db_type == "mysql":
                rows = conn.execute(sqlalchemy.text("SHOW TABLES")).fetchall()
                tables = [list(r)[0] for r in rows]
            elif req.db_type == "postgres":
                rows = conn.execute(sqlalchemy.text("SELECT tablename FROM pg_tables WHERE schemaname='public'"))
                tables = [r[0] for r in rows]
            elif req.db_type == "sqlite":
                rows = conn.execute(sqlalchemy.text("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"))
                tables = [r[0] for r in rows]

        return {"code": 0, "message": "OK", "data": tables}
    except Exception as e:
        return {"code": 9999, "message": "查询表失败", "detail": str(e)}
