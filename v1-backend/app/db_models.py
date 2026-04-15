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
    connection_profile_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class ConnectionProfileModel(BASE):
    __tablename__ = "connection_profiles"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), nullable=False, unique=True)
    db_type = Column(String(32), nullable=False, default="mysql")
    driver = Column(String(64), nullable=True)
    host = Column(String(255), nullable=True)
    port = Column(Integer, nullable=True)
    database = Column(String(255), nullable=True)
    username = Column(String(128), nullable=True)
    # For MVP, keep secret_ref as opaque text (vault ref or encrypted blob id).
    secret_ref = Column(Text, nullable=True)
    dsn = Column(Text, nullable=True)
    extra = Column(JSON, default={})
    owner_id = Column(String(64), nullable=True)
    enabled = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)


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
        if "connection_profile_id" not in col_names:
            conn.execute(text("ALTER TABLE export_jobs ADD COLUMN connection_profile_id INTEGER"))

    return sessionmaker(bind=engine)
