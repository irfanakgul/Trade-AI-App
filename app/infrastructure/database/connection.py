from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()  # .env dosyasını yükler


class Database:

    def __init__(self):
        self._engine = None
        self._session_factory = None

    def connect(self):
        if self._engine is None:
            url = URL.create(
                "postgresql+psycopg",
                username=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT")),
                database=os.getenv("DB_NAME"),
            )

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

        return self._engine

    def get_session(self):
        if self._session_factory is None:
            self.connect()
        return self._session_factory()