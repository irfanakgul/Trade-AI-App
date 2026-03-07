from __future__ import annotations

import gzip
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from app.backup.config import BackupConfig
from app.backup.logger import get_logger


@dataclass
class BackupResult:
    success: bool
    backup_file: Path | None
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    message: str


class PostgresBackupService:
    def __init__(self, config: BackupConfig):
        self.config = config
        self.logger = get_logger(
            name="postgres_backup",
            log_file=self.config.logs_dir / "backup.out.log",
        )

    def _build_backup_filename(self) -> Path:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        ext = ".dump.gz" if self.config.compress else ".dump"
        return self.config.backup_dir / f"{self.config.backup_prefix}_{timestamp}{ext}"

    def _cleanup_old_backups(self) -> None:
        cutoff = datetime.now() - timedelta(days=self.config.retention_days)

        self.logger.info(
            "Retention cleanup başladı | retention_days=%s | min_keep_count=%s | backup_dir=%s",
            self.config.retention_days,
            self.config.min_keep_count,
            self.config.backup_dir,
        )

        backup_files = list(self.config.backup_dir.glob("*.dump")) + list(self.config.backup_dir.glob("*.dump.gz"))
        backup_files = sorted(backup_files, key=lambda p: p.stat().st_mtime, reverse=True)

        protected_files = set(backup_files[:self.config.min_keep_count])
        deleted_count = 0
        skipped_old_but_protected_count = 0

        for file_path in backup_files:
            modified_at = datetime.fromtimestamp(file_path.stat().st_mtime)
            is_older_than_retention = modified_at < cutoff
            is_protected = file_path in protected_files

            if is_older_than_retention and not is_protected:
                try:
                    file_path.unlink()
                    deleted_count += 1
                    self.logger.info("Eski backup silindi | file=%s", file_path)
                except Exception as exc:
                    self.logger.exception("Eski backup silinemedi | file=%s | error=%s", file_path, exc)
            elif is_older_than_retention and is_protected:
                skipped_old_but_protected_count += 1
                self.logger.info(
                    "Backup retention dışında ama korundu | file=%s | reason=min_keep_count",
                    file_path,
                )

        self.logger.info(
            "Retention cleanup bitti | total_files=%s | protected_files=%s | deleted_count=%s | old_but_protected_count=%s",
            len(backup_files),
            len(protected_files),
            deleted_count,
            skipped_old_but_protected_count,
        )

    def _gzip_file(self, source_file: Path) -> Path:
        gz_file = source_file.with_suffix(source_file.suffix + ".gz")

        with source_file.open("rb") as src, gzip.open(gz_file, "wb") as dst:
            shutil.copyfileobj(src, dst)

        source_file.unlink()
        return gz_file

    def create_backup(self) -> BackupResult:
        started_at = datetime.now()
        temp_dump_file = self.config.backup_dir / f"tmp_{started_at.strftime('%Y%m%d_%H%M%S')}.dump"
        final_target = self._build_backup_filename()

        source = self._get_backup_source_params()

        env = os.environ.copy()
        if source["password"]:
            env["PGPASSWORD"] = source["password"]
        else:
            env.pop("PGPASSWORD", None)

        cmd = [
            self.config.pg_dump_path,
            "-h", source["host"],
            "-p", str(source["port"]),
            "-U", source["user"],
            "-d", source["name"],
            "-F", "c",
            "-f", str(temp_dump_file),
        ]

        if self.config.include_blobs:
            cmd.append("-b")

        if self.config.verbose:
            cmd.append("-v")

        self.logger.info(
            "Backup başladı | source=%s | db=%s | host=%s | port=%s | target=%s",
            source["source"],
            source["name"],
            source["host"],
            source["port"],
            final_target,
        )

        try:
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                check=True,
            )

            if result.stdout.strip():
                self.logger.info("pg_dump stdout:\n%s", result.stdout.strip())

            if result.stderr.strip():
                self.logger.info("pg_dump stderr:\n%s", result.stderr.strip())

            if self.config.compress:
                gz_file = self._gzip_file(temp_dump_file)
                gz_file.rename(final_target)
            else:
                temp_dump_file.rename(final_target)

            self._cleanup_old_backups()

            finished_at = datetime.now()
            duration_seconds = (finished_at - started_at).total_seconds()

            self.logger.info(
                "Backup başarılı | source=%s | file=%s | duration_seconds=%.2f",
                source["source"],
                final_target,
                duration_seconds,
            )

            return BackupResult(
                success=True,
                backup_file=final_target,
                started_at=started_at,
                finished_at=finished_at,
                duration_seconds=duration_seconds,
                message=f"Backup başarılı ({source['source']})",
            )

        except subprocess.CalledProcessError as exc:
            finished_at = datetime.now()
            duration_seconds = (finished_at - started_at).total_seconds()

            self.logger.error(
                "Backup başarısız | source=%s | returncode=%s",
                source["source"],
                exc.returncode,
            )
            if exc.stdout:
                self.logger.error("pg_dump stdout:\n%s", exc.stdout.strip())
            if exc.stderr:
                self.logger.error("pg_dump stderr:\n%s", exc.stderr.strip())

            if temp_dump_file.exists():
                try:
                    temp_dump_file.unlink()
                except Exception:
                    self.logger.exception("Geçici dump dosyası silinemedi | file=%s", temp_dump_file)

            return BackupResult(
                success=False,
                backup_file=None,
                started_at=started_at,
                finished_at=finished_at,
                duration_seconds=duration_seconds,
                message=f"Backup başarısız ({source['source']}): {exc}",
            )

        except Exception as exc:
            finished_at = datetime.now()
            duration_seconds = (finished_at - started_at).total_seconds()

            self.logger.exception("Beklenmeyen backup hatası | source=%s | error=%s", source["source"], exc)

            if temp_dump_file.exists():
                try:
                    temp_dump_file.unlink()
                except Exception:
                    self.logger.exception("Geçici dump dosyası silinemedi | file=%s", temp_dump_file)

            return BackupResult(
                success=False,
                backup_file=None,
                started_at=started_at,
                finished_at=finished_at,
                duration_seconds=duration_seconds,
                message=f"Beklenmeyen hata ({source['source']}): {exc}",
            )
        
    def _get_backup_source_params(self) -> dict:
        if self.config.backup_source == "local":
            return {
                "source": "local",
                "host": self.config.local_db_host,
                "port": self.config.local_db_port,
                "name": self.config.local_db_name,
                "user": self.config.local_db_user,
                "password": self.config.local_db_password,
            }

        return {
            "source": "cloud",
            "host": self.config.db_host,
            "port": self.config.db_port,
            "name": self.config.db_name,
            "user": self.config.db_user,
            "password": self.config.db_password,
        }

    def print_backup_source_info(self) -> None:
        source = self._get_backup_source_params()
        self.logger.info(
            "Backup source bilgisi | source=%s | host=%s | port=%s | db=%s | user=%s",
            source["source"],
            source["host"],
            source["port"],
            source["name"],
            source["user"],
        )