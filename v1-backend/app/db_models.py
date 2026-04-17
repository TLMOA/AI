from sqlalchemy import Column, Integer, String, DateTime, Text, JSON, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
from pathlib import Path

BASE = declarative_base()


class JobModel(BASE):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(128), unique=True, index=True, nullable=False)
    job_type = Column(String(64))
    status = Column(String(32))
    progress = Column(Integer)
    payload = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)


class FileModel(BASE):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True, autoincrement=True)
    file_id = Column(String(128), unique=True, index=True, nullable=False)
    file_name = Column(String(256))
    file_format = Column(String(32))
    file_size = Column(Integer)
    storage_type = Column(String(32))
    storage_path = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


class TagModel(BASE):
    __tablename__ = "tags"
    id = Column(Integer, primary_key=True, autoincrement=True)
    file_id = Column(String(128), index=True)
    row_id = Column(String(128))
    label = Column(String(128))
    operator = Column(String(64))
    created_at = Column(DateTime, default=datetime.utcnow)


class ExportJobModel(BASE):
    __tablename__ = "export_jobs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_name = Column(String(128), nullable=False)
    factory_id = Column(String(64), nullable=True)
    owner_id = Column(String(64), nullable=True)
    schedule = Column(String(64), nullable=True)
    file_format = Column(String(32), default="csv")
    destination = Column(JSON, default={})
    mode = Column(String(32), default="visible")
    enabled = Column(Integer, default=0)
    last_run = Column(DateTime, nullable=True)
    status = Column(String(64), nullable=True)
    db_config = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class IotUser(BASE):
    __tablename__ = "iot_users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(128), unique=True, index=True, nullable=False)
    password_hash = Column(String(256), nullable=False)
    is_admin = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)


def get_engine(db_path: Path):
    url = f"sqlite:///{db_path}"
    engine = create_engine(url, connect_args={"check_same_thread": False})
    return engine


def init_db(db_path: Path):
    engine = get_engine(db_path)
    BASE.metadata.create_all(engine)

    # Lightweight migration for existing SQLite DBs created before new columns were introduced.
    with engine.begin() as conn:
        cols = conn.execute(text("PRAGMA table_info(export_jobs)")).fetchall()
        col_names = {row[1] for row in cols}
        if "factory_id" not in col_names:
            conn.execute(text("ALTER TABLE export_jobs ADD COLUMN factory_id VARCHAR(64)"))

        # Ensure iot_users table exists and a default admin user is present
        try:
            users = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='iot_users'" )).fetchone()
            if not users:
                # create table via metadata (already called create_all), but ensure minimal init
                conn.execute(text("CREATE TABLE IF NOT EXISTS iot_users (id INTEGER PRIMARY KEY AUTOINCREMENT, username VARCHAR(128) UNIQUE NOT NULL, password_hash VARCHAR(256) NOT NULL, is_admin INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"))
        except Exception:
            pass

        # Insert default admin if not exists (password from env or '123456')
        try:
            row = conn.execute(text("SELECT id FROM iot_users WHERE username='admin' LIMIT 1")).fetchone()
            if not row:
                # compute bcrypt hash if bcrypt available
                import os as _os
                pwd = _os.getenv('IOT_ADMIN_PASSWORD', '123456')
                try:
                    import bcrypt as _bcrypt
                    ph = _bcrypt.hashpw(pwd.encode('utf-8'), _bcrypt.gensalt()).decode('utf-8')
                except Exception:
                    ph = pwd
                conn.execute(text("INSERT INTO iot_users (username, password_hash, is_admin) VALUES ('admin', :ph, 1)"), {"ph": ph})
        except Exception:
            pass

    return sessionmaker(bind=engine)
