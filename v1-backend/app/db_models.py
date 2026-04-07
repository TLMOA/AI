from sqlalchemy import Column, Integer, String, DateTime, Text, JSON
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


def get_engine(db_path: Path):
    url = f"sqlite:///{db_path}"
    engine = create_engine(url, connect_args={"check_same_thread": False})
    return engine


def init_db(db_path: Path):
    engine = get_engine(db_path)
    BASE.metadata.create_all(engine)
    return sessionmaker(bind=engine)
