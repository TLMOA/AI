from sqlalchemy import create_engine
from typing import Dict


def engine_from_config(db_conf: Dict) -> any:
    """Create SQLAlchemy engine from db_conf dict.
    db_conf example: {"db_type":"mysql","user":"root","password":"root","host":"127.0.0.1","port":3306,"database":"nifi"}
    Supports mysql and postgresql for MVP.
    """
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
    else:
        raise ValueError(f"Unsupported db_type: {db_type}")

    engine = create_engine(url)
    return engine
