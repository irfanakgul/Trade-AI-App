from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()


class Database:
    def __init__(self):
        self._engine = None
        self._session_factory = None
        self._connected_mode = None

    def _get_connection_mode(self) -> str:
        return os.getenv("CONNECTION_WHERE", "cloud").strip().lower()

    def _build_url(self) -> URL:
        mode = self._get_connection_mode()

        if mode == "local":
            return URL.create(
                "postgresql+psycopg",
                username=os.getenv("LOCAL_DB_USER"),
                password=(os.getenv("LOCAL_DB_PASSWORD") or None),
                host=os.getenv("LOCAL_DB_HOST", "127.0.0.1"),
                port=int(os.getenv("LOCAL_DB_PORT", "5432")),
                database=os.getenv("LOCAL_DB_NAME"),
            )

        return URL.create(
            "postgresql+psycopg",
            username=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NAME"),
        )

    def connect(self):
        current_mode = self._get_connection_mode()

        if self._engine is not None and self._connected_mode == current_mode:
            return self._engine

        if self._engine is not None and self._connected_mode != current_mode:
            self.reset()

        url = self._build_url()

        self._engine = create_engine(
            url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            future=True,
        )

        self._session_factory = sessionmaker(
            bind=self._engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
        self._connected_mode = current_mode

        return self._engine

    def get_session(self):
        if self._session_factory is None:
            self.connect()
        return self._session_factory()

    def reset(self):
        if self._engine is not None:
            self._engine.dispose()

        self._engine = None
        self._session_factory = None
        self._connected_mode = None

    def get_connection_info(self) -> dict:
        mode = self._get_connection_mode()

        if mode == "local":
            return {
                "mode": "local",
                "host": os.getenv("LOCAL_DB_HOST", "127.0.0.1"),
                "port": int(os.getenv("LOCAL_DB_PORT", "5432")),
                "database": os.getenv("LOCAL_DB_NAME"),
                "user": os.getenv("LOCAL_DB_USER"),
            }

        return {
            "mode": "cloud",
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT", "5432")),
            "database": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
        }

    def print_connection_info(self) -> None:
        info = self.get_connection_info()
        print("=" * 60)
        print("🔗 DATABASE CONNECTION INFO 🔗")
        print(f"mode     : {info['mode']}")
        print(f"host     : {info['host']}")
        print(f"port     : {info['port']}")
        print(f"database : {info['database']}")
        print(f"user     : {info['user']}")
        print("=" * 60)