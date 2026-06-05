from fastapi import APIRouter, Body, Header
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
import sqlalchemy
import concurrent.futures
import subprocess
import shutil
from .local_ip import _collect_local_ips, _resolve_local_host

# Import our Hadoop connection functions
from .engine_factory import engine_from_config

# Note: _collect_local_ips / _resolve_local_host moved to .local_ip
# to avoid circular import with engine_factory.

router = APIRouter()

class DBConnectReq(BaseModel):
    db_type: str = Field(..., description="数据库类型，如mysql/postgres/sqlite/hive/hbase/hdfs")
    host: str = Field(..., description="主机名或IP")
    port: int = Field(..., description="端口号")
    username: str = Field(..., description="用户名")
    password: str = Field(..., description="密码")
    database: Optional[str] = Field(default="", description="数据库名（HDFS/HBase 可留空）")
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
    dbt = (req.db_type or "").strip().lower()
    # 参数校验：HDFS/HBase/SQLite 不需要数据库名；SQLite 只需要路径。
    if dbt == "sqlite":
        if not req.database:
            return DBConnectResp(code=1001, message="参数缺失，请检查 SQLite 文件路径是否填写完整")
    elif not req.host or not req.port or not req.username or (dbt not in {"hdfs", "hbase"} and not req.database):
        return DBConnectResp(code=1001, message="参数缺失，请检查主机、端口、用户名、数据库名是否填写完整")

    # 如果请求的 host 是本机任意接口 IP，自动改用 127.0.0.1 直连，
    # 避免 MySQL 等服务对客户端 IP 进行反向 DNS 解析导致 10s 级别的卡顿。
    effective_host = _resolve_local_host(req.host)

    # 构建连接字符串
    try:
        # Handle Hadoop service types
        if dbt == "hive":
            # Prefer JDBC/Beeline validation because Hive 4.x + PyHive can be unstable.
            beeline = shutil.which("beeline")
            docker = shutil.which("docker")
            jdbc_url = f"jdbc:hive2://{effective_host}:{req.port}/{req.database or 'default'}"
            if beeline:
                cmd = [beeline, "-u", jdbc_url]
                if req.username:
                    cmd.extend(["-n", req.username])
                # In NOSASL / non-LDAP modes, do not pass password.
                if req.password and str(req.password).strip() and (req.params or {}).get("hive_auth", "").upper() in {"LDAP", "CUSTOM"}:
                    cmd.extend(["-p", req.password])
                try:
                    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=15)
                    output = (proc.stdout or "") + "\n" + (proc.stderr or "")
                    if proc.returncode == 0 and ("Connected to: Apache Hive" in output or "Connected to:" in output):
                        return DBConnectResp(code=0, message="连接成功")
                    return DBConnectResp(code=2001, message="Hive连接失败", detail=output.strip() or f"beeline exit code {proc.returncode}")
                except subprocess.TimeoutExpired:
                    return DBConnectResp(code=2003, message="Hive连接超时", detail="beeline 连接 HiveServer2 超时")
                except Exception as e:
                    return DBConnectResp(code=2001, message="Hive连接失败", detail=str(e))

            # If beeline is not installed locally, try the running Hive container.
            if docker:
                cmd = [docker, "exec", "hive-server2", "beeline", "-u", jdbc_url]
                if req.username:
                    cmd.extend(["-n", req.username])
                if req.password and str(req.password).strip() and (req.params or {}).get("hive_auth", "").upper() in {"LDAP", "CUSTOM"}:
                    cmd.extend(["-p", req.password])
                try:
                    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=20)
                    output = (proc.stdout or "") + "\n" + (proc.stderr or "")
                    if proc.returncode == 0 and ("Connected to: Apache Hive" in output or "Connected to:" in output):
                        return DBConnectResp(code=0, message="连接成功")
                    return DBConnectResp(code=2001, message="Hive连接失败", detail=output.strip() or f"docker exec beeline exit code {proc.returncode}")
                except subprocess.TimeoutExpired:
                    return DBConnectResp(code=2003, message="Hive连接超时", detail="docker exec beeline 连接 HiveServer2 超时")
                except Exception as e:
                    return DBConnectResp(code=2001, message="Hive连接失败", detail=str(e))

            # Fallback: PyHive connection check
            db_conf = {
                "db_type": "hive",
                "host": req.host,
                "port": req.port,
                "user": req.username,
                "password": req.password,
                "database": req.database
            }
            try:
                conn = engine_from_config(db_conf)
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                conn.close()
                return DBConnectResp(code=0, message="连接成功")
            except Exception as e:
                return DBConnectResp(code=2001, message="Hive连接失败", detail=str(e))
        elif dbt == "hbase":
            # Prefer native HBase shell in the running container; if that fails
            # (for example the container image lacks the `hbase` binary),
            # fall back to Thrift/happybase via engine_factory.
            docker = shutil.which("docker")
            docker_error_detail = None
            if docker:
                try:
                    proc = subprocess.run(
                        [docker, "exec", "hbase", "bash", "-lc", "printf 'list\\n' | hbase shell -n"],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        timeout=20,
                    )
                    output = (proc.stdout or "") + "\n" + (proc.stderr or "")
                    if proc.returncode == 0 and ("TABLE" in output or "row(s)" in output):
                        return DBConnectResp(code=0, message="连接成功")
                    # remember docker error but do not fail immediately — try Thrift fallback
                    docker_error_detail = output.strip() or f"hbase shell exit code {proc.returncode}"
                except subprocess.TimeoutExpired:
                    docker_error_detail = "docker exec hbase shell 超时"
                except Exception as e:
                    docker_error_detail = str(e)

            # Fallback to happybase/thrift (engine_factory) if container shell is not usable
            db_conf = {
                "db_type": "hbase",
                "host": req.host,
                "port": req.port,
                "user": req.username
            }
            try:
                conn = engine_from_config(db_conf)
                conn.tables()
                conn.close()
                return DBConnectResp(code=0, message="连接成功")
            except Exception as e:
                # If we had a docker error earlier, include it to help debugging.
                detail = str(e)
                if docker_error_detail:
                    detail = f"docker-hbase: {docker_error_detail}; thrift: {detail}"
                return DBConnectResp(code=2001, message="HBase连接失败", detail=detail)
        elif dbt == "hdfs":
            # Test HDFS connection using our engine factory
            db_conf = {
                "db_type": "hdfs",
                "host": req.host,
                "port": req.port,
                "user": req.username
            }
            try:
                client = engine_from_config(db_conf)
                # Try a simple operation to verify connection
                client.list('/')  # List root directory to verify connection
                return DBConnectResp(code=0, message="连接成功")
            except Exception as e:
                return DBConnectResp(code=2001, message="HDFS连接失败", detail=str(e))
        elif dbt in ("mysql", "mariadb"):
            url = f"mysql+pymysql://{req.username}:{req.password}@{effective_host}:{req.port}/{req.database}"
        elif dbt in ("postgres", "postgresql"):
            # prefer explicit psycopg2 driver
            url = f"postgresql+psycopg2://{req.username}:{req.password}@{effective_host}:{req.port}/{req.database}"
        elif dbt == "sqlite":
            url = f"sqlite:///{req.database}"
        elif dbt in ("mssql", "sqlserver"):
            # simple pyodbc template; requires system ODBC driver and pyodbc installed
            # user may need to URL-encode driver parameter in real deployments
            url = f"mssql+pyodbc://{req.username}:{req.password}@{effective_host}:{req.port}/{req.database}?driver=ODBC+Driver+17+for+SQL+Server"
        elif dbt == "oracle":
            # prefer oracledb Python package (thin mode possible)
            url = f"oracle+oracledb://{req.username}:{req.password}@{effective_host}:{req.port}/?service_name={req.database}"
        else:
            return DBConnectResp(code=1002, message=f"暂不支持的数据库类型: {req.db_type}")

        def _connect_and_ping(u, params):
            engine = sqlalchemy.create_engine(u, connect_args=params or {})
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text("SELECT 1"))

        # run the actual connect in a thread with timeout to avoid long blocking from drivers
        timeout_seconds = 8
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_connect_and_ping, url, req.params)
            try:
                future.result(timeout=timeout_seconds)
                return DBConnectResp(code=0, message="连接成功")
            except concurrent.futures.TimeoutError:
                return DBConnectResp(code=2003, message=f"连接超时（>{timeout_seconds}s）", detail="连接尝试超时，请检查网络或目标数据库是否可达")
            except Exception as e:
                # let outer exception handlers map the error
                raise
    except sqlalchemy.exc.OperationalError as e:
        return DBConnectResp(code=2001, message="连接失败，网络或认证错误", detail=str(e))
    except sqlalchemy.exc.ProgrammingError as e:
        return DBConnectResp(code=2002, message="连接失败，数据库不存在或权限不足", detail=str(e))
    except Exception as e:
        return DBConnectResp(code=9999, message="连接失败，未知错误", detail=str(e))


@router.post("/api/v1/db/list-tables")
def list_tables(req: DBConnectReq, x_trace_id: Optional[str] = Header(default=None)):
    """返回指定数据库下的表名列表（简易实现，支持 mysql/postgres/sqlite/hive/hbase）。"""
    try:
        dbt = (req.db_type or "").strip().lower()
        effective_host = _resolve_local_host(req.host)

        # Handle Hadoop service types for table listing
        if dbt == "hive":
            # Prefer Beeline/JDBC for Hive 4.x compatibility.
            beeline = shutil.which("beeline")
            docker = shutil.which("docker")
            jdbc_url = f"jdbc:hive2://{effective_host}:{req.port}/{req.database or 'default'}"
            cmd = None
            if beeline:
                cmd = [beeline, "-u", jdbc_url, "-e", "SHOW TABLES"]
                if req.username:
                    cmd.extend(["-n", req.username])
                if req.password and str(req.password).strip() and (req.params or {}).get("hive_auth", "").upper() in {"LDAP", "CUSTOM"}:
                    cmd.extend(["-p", req.password])
            elif docker:
                cmd = [docker, "exec", "hive-server2", "beeline", "-u", jdbc_url, "-e", "SHOW TABLES"]
                if req.username:
                    cmd.extend(["-n", req.username])
                if req.password and str(req.password).strip() and (req.params or {}).get("hive_auth", "").upper() in {"LDAP", "CUSTOM"}:
                    cmd.extend(["-p", req.password])
            if cmd:
                try:
                    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30)
                    output = (proc.stdout or "") + "\n" + (proc.stderr or "")
                    if proc.returncode != 0:
                        return TableListResp(code=9999, message="查询表失败", detail=output.strip() or f"hive beeline exit code {proc.returncode}")
                    tables = []
                    for line in output.splitlines():
                        s = line.strip()
                        if not s:
                            continue
                        if s.startswith("SLF4J:") or s.startswith("Connecting to:") or s.startswith("Connected to:"):
                            continue
                        if s.lower().startswith("show tables") or s.startswith("Beeline version") or s.startswith("Transaction isolation"):
                            continue
                        if s.lower() == "ok":
                            continue
                        # Beeline output often has table names as plain lines.
                        if "\t" in s:
                            s = s.split("\t")[-1].strip()
                        tables.append(s)
                    # de-dup while preserving order
                    seen = set()
                    tables = [t for t in tables if not (t in seen or seen.add(t))]
                    return TableListResp(code=0, message="OK", data=tables)
                except subprocess.TimeoutExpired:
                    return TableListResp(code=9999, message="查询表失败", detail="beeline 查询 Hive 表超时")
                except Exception as e:
                    return TableListResp(code=9999, message="查询表失败", detail=str(e))
            return TableListResp(code=9999, message="查询表失败", detail="找不到 beeline，且无法通过 docker exec 调用 hive-server2")
        elif dbt == "hbase":
            docker = shutil.which("docker")
            docker_error_detail = None
            if docker:
                try:
                    proc = subprocess.run(
                        [docker, "exec", "hbase", "bash", "-lc", "printf 'list\\n' | hbase shell -n"],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        timeout=20,
                    )
                    output = (proc.stdout or "") + "\n" + (proc.stderr or "")
                    if proc.returncode == 0:
                        tables = []
                        capture = False
                        for line in output.splitlines():
                            item = line.strip()
                            if item == "TABLE":
                                capture = True
                                continue
                            if not capture:
                                continue
                            if not item or item.startswith("SLF4J:") or "row(s)" in item or item.startswith("Took "):
                                continue
                            tables.append(item)
                        return TableListResp(code=0, message="OK", data=tables)
                    # remember docker error but continue to thrift fallback
                    docker_error_detail = output.strip() or f"hbase shell exit code {proc.returncode}"
                except subprocess.TimeoutExpired:
                    docker_error_detail = "hbase shell 查询 HBase 表超时"
                except Exception as e:
                    docker_error_detail = str(e)

            db_conf = {
                "db_type": "hbase",
                "host": effective_host,
                "port": req.port,
                "user": req.username
            }
            try:
                conn = engine_from_config(db_conf)
                tables = conn.tables()
                conn.close()
                return TableListResp(code=0, message="OK", data=[table.decode('utf-8') for table in tables])
            except Exception as e:
                detail = str(e)
                if docker_error_detail:
                    detail = f"docker-hbase: {docker_error_detail}; thrift: {detail}"
                return TableListResp(code=9999, message="查询表失败", detail=detail)
        elif dbt == "hdfs":
            db_conf = {
                "db_type": "hdfs",
                "host": effective_host,
                "port": req.port,
                "user": req.username
            }
            try:
                client = engine_from_config(db_conf)
                path = (req.database or "/").strip() or "/"
                # 兼容前端旧默认值：HDFS 默认应列根目录，而不是旧数据库名 /nifi。
                if path in ("nifi", "/nifi"):
                    path = "/"
                if not path.startswith("/"):
                    path = f"/{path}"
                entries = client.list(path, status=True)
                data = []
                for name, status in entries:
                    entry_type = status.get("type", "").lower()
                    data.append(f"{path.rstrip('/')}/{name}" if path != "/" else f"/{name}" if entry_type else name)
                return TableListResp(code=0, message="OK", data=data)
            except Exception as e:
                return TableListResp(code=9999, message="查询目录失败", detail=str(e))
        elif dbt in ("mysql", "mariadb"):
            url = f"mysql+pymysql://{req.username}:{req.password}@{effective_host}:{req.port}/{req.database}"
        elif dbt in ("postgres", "postgresql"):
            url = f"postgresql+psycopg2://{req.username}:{req.password}@{effective_host}:{req.port}/{req.database}"
        elif dbt == "sqlite":
            url = f"sqlite:///{req.database}"
        elif dbt in ("mssql", "sqlserver"):
            url = f"mssql+pyodbc://{req.username}:{req.password}@{effective_host}:{req.port}/{req.database}?driver=ODBC+Driver+17+for+SQL+Server"
        elif dbt == "oracle":
            url = f"oracle+oracledb://{req.username}:{req.password}@{effective_host}:{req.port}/?service_name={req.database}"
        else:
            return TableListResp(code=1002, message=f"暂不支持的数据库类型: {req.db_type}")

        engine = sqlalchemy.create_engine(url, connect_args=req.params or {})
        tables = []
        with engine.connect() as conn:
            if dbt in ("mysql", "mariadb"):
                rows = conn.execute(sqlalchemy.text("SHOW TABLES")).fetchall()
                tables = [list(r)[0] for r in rows]
            elif dbt in ("postgres", "postgresql"):
                rows = conn.execute(sqlalchemy.text("SELECT tablename FROM pg_tables WHERE schemaname='public'"))
                tables = [r[0] for r in rows]
            elif dbt == "sqlite":
                rows = conn.execute(sqlalchemy.text("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"))
                tables = [r[0] for r in rows]

        return TableListResp(code=0, message="OK", data=tables)
    except Exception as e:
        return TableListResp(code=9999, message="查询表失败", detail=str(e))