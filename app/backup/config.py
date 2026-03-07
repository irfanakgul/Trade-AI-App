from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


def _parse_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_int(value: Optional[str], default: int) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value


@dataclass
class BackupConfig:
    project_root: Path
    env_file: Path

    backup_source: str

    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    backup_dir: Path
    logs_dir: Path

    backup_prefix: str
    retention_days: int
    min_keep_count: int
    include_blobs: bool
    verbose: bool
    compress: bool

    pg_dump_path: str
    pg_restore_path: str
    createdb_path: str

    local_db_host: str
    local_db_port: int
    local_db_name: str
    local_db_user: str
    local_db_password: str
    local_db_for_using: str

    @classmethod
    def from_project_root(
        cls,
        project_root: str | Path,
        env_file: str | Path | None = None,
    ) -> "BackupConfig":
        root = Path(project_root).resolve()
        env_path = Path(env_file).resolve() if env_file else root / ".env"

        load_env_file(env_path)

        db_host = os.getenv("DB_HOST", "").strip()
        db_port = _parse_int(os.getenv("DB_PORT"), 5432)
        db_name = os.getenv("DB_NAME", "").strip()
        db_user = os.getenv("DB_USER", "").strip()
        db_password = os.getenv("DB_PASSWORD", "").strip()

        if not all([db_host, db_name, db_user, db_password]):
            missing = []
            if not db_host:
                missing.append("DB_HOST")
            if not db_name:
                missing.append("DB_NAME")
            if not db_user:
                missing.append("DB_USER")
            if not db_password:
                missing.append("DB_PASSWORD")
            raise ValueError(f"Eksik environment değişkenleri: {', '.join(missing)}")

        backup_dir = Path(os.getenv("BACKUP_DIR", str(root / "app" / "backup" / "data"))).resolve()
        logs_dir = Path(os.getenv("LOG_DIR", str(root / "logs"))).resolve()

        backup_dir.mkdir(parents=True, exist_ok=True)
        logs_dir.mkdir(parents=True, exist_ok=True)

        backup_source = os.getenv("BACKUP_SOURCE", "cloud").strip().lower()
        if backup_source not in {"cloud", "local"}:
            raise ValueError("BACKUP_SOURCE yalnızca 'cloud' veya 'local' olabilir")

        raw_prefix = os.getenv("BACKUP_PREFIX", "backup_tradeapp").strip()
        backup_prefix = f"{raw_prefix}_{backup_source}"

        return cls(
            project_root=root,
            env_file=env_path,
            backup_source=backup_source,
            db_host=db_host,
            db_port=db_port,
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
            backup_dir=backup_dir,
            logs_dir=logs_dir,
            backup_prefix=backup_prefix,
            retention_days=_parse_int(os.getenv("BACKUP_RETENTION_DAYS"), 7),
            min_keep_count=_parse_int(os.getenv("BACKUP_MIN_KEEP_COUNT"), 3),
            include_blobs=_parse_bool(os.getenv("BACKUP_INCLUDE_BLOBS"), True),
            verbose=_parse_bool(os.getenv("BACKUP_VERBOSE"), True),
            compress=_parse_bool(os.getenv("BACKUP_COMPRESS"), False),
            pg_dump_path=os.getenv("PG_DUMP_PATH", "pg_dump").strip(),
            pg_restore_path=os.getenv("PG_RESTORE_PATH", "pg_restore").strip(),
            createdb_path=os.getenv("CREATEDB_PATH", "createdb").strip(),
            local_db_host=os.getenv("LOCAL_DB_HOST", "127.0.0.1").strip(),
            local_db_port=_parse_int(os.getenv("LOCAL_DB_PORT"), 5432),
            local_db_name=os.getenv("LOCAL_DB_NAME", f"{db_name}_local").strip(),
            local_db_user=os.getenv("LOCAL_DB_USER", "postgres").strip(),
            local_db_password=os.getenv("LOCAL_DB_PASSWORD", "").strip(),
            local_db_for_using=os.getenv("LOCAL_DB_FOR_USING", "").strip(),
        )