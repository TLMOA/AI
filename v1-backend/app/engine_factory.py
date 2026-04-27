from sqlalchemy import create_engine
from typing import Dict
try:
    import pyhive.hive
except ImportError:
    pyhive = None
try:
    import happybase
except ImportError:
    happybase = None
try:
    import hdfs
except ImportError:
    hdfs = None


def engine_from_config(db_conf: Dict) -> any:
    """Create SQLAlchemy engine from db_conf dict.
    db_conf example: {"db_type":"mysql","user":"root","password":"root","host":"127.0.0.1","port":3306,"database":"nifi"}
    Supports mysql, postgresql, hive, hdfs, and hbase for MVP.
    """
    dsn = db_conf.get("dsn")
    if dsn:
        return create_engine(dsn)

    db_type = db_conf.get("db_type", "mysql").lower()
    user = db_conf.get("user")
    password = db_conf.get("password")
    host = db_conf.get("host", "127.0.0.1")
    port = db_conf.get("port")
    database = db_conf.get("database")

    if db_type in ("mysql", "mariadb"):
        driver = db_conf.get("driver", "pymysql")
        port = port or 3306
        url = f"mysql+{driver}://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
    elif db_type in ("postgresql", "postgres"):
        driver = db_conf.get("driver", "psycopg2")
        port = port or 5432
        url = f"postgresql+{driver}://{user}:{password}@{host}:{port}/{database}"
    elif db_type == "sqlite":
        path = db_conf.get("path")
        if path:
            url = f"sqlite:///{path}"
        elif database:
            # Accept either filename or absolute path via database.
            if str(database).startswith("/"):
                url = f"sqlite:///{database}"
            else:
                url = f"sqlite:///{database}"
        else:
            raise ValueError("sqlite requires `path` or `database`")
    elif db_type in ("mssql", "sqlserver"):
        driver = db_conf.get("driver", "pyodbc")
        port = port or 1433
        url = f"mssql+{driver}://{user}:{password}@{host}:{port}/{database}"
    elif db_type == "oracle":
        driver = db_conf.get("driver", "oracledb")
        port = port or 1521
        url = f"oracle+{driver}://{user}:{password}@{host}:{port}/?service_name={database}"
    elif db_type == "hive":
        if pyhive is None:
            raise ImportError("pyhive is required for Hive connections. Please install it with: pip install pyhive")
        # Connect to Hive using PyHive
        port = port or 10000
        auth = db_conf.get("auth", "NOSASL")  # Default to no authentication
        database = database or "default"
        
        # Create connection using PyHive
        connection = pyhive.hive.Connection(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
            auth=auth
        )
        return connection
    elif db_type == "hbase":
        if happybase is None:
            raise ImportError("happybase is required for HBase connections. Please install it with: pip install happybase")
        # Connect to HBase using happybase
        port = port or 9090  # Default HBase Thrift port
        connection = happybase.Connection(host=host, port=port, timeout=5000)
        return connection
    elif db_type == "hdfs":
        if hdfs is None:
            raise ImportError("hdfs is required for HDFS connections. Please install it with: pip install hdfs")
        # Connect to HDFS using WebHDFS
        port = port or 9870  # Default WebHDFS port
        hdfs_url = f"http://{host}:{port}"
        client = hdfs.InsecureClient(hdfs_url, user=user)
        return client
    else:
        raise ValueError(f"Unsupported db_type: {db_type}")

    engine = create_engine(url)
    return engine