from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL

from config.settings import AppSettings


def build_db_url(settings: AppSettings) -> URL:
    return URL.create(
        drivername="postgresql+psycopg2",
        username=settings.DB_USER,
        password=settings.DB_PASSWORD,
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        database=settings.DB_NAME,
    )


def create_db_engine(settings: AppSettings) -> Engine:
    return create_engine(
        build_db_url(settings),
        pool_pre_ping=True,
        future=True,
    )